#import "rcd.h"
#import "musl.h"
#import "acid.h"
#import "quark-internal.h"

#pragma librcd

#define QK_HEADER_MAGIC 0x6aef91b6b454b73f
#define QK_VERSION 1

/// The smallest extension possible to the acid memory range. 4MB.
#define QK_MIN_EXT_SIZE_2E 22

/// Smallest possible allocation size.
#define QK_PAGE_SIZE_2E 12

/// Terminology used in "The B-Skip-List: A Simpler Uniquely Represented
/// Alternative to B-Trees".

/// Skip-list tuning constants.
/// Level 1 will be the data layer.
#define QK_BSL_LEVELS 5
/// Partition size and height tuning parameter.
#define QK_BSL_GAMMA 4096


typedef struct qk_block_index {
    struct qk_block_index* next;
    uint8_t pages_2e;
} qk_block_index_t;

typedef struct qk_bsl_part {
    // Zero means this is the last partition on this level
    struct qk_bsl_part* next;
    // Zero means this is the first partition on this level
    struct qk_bsl_part* prev;
    // Size and space excluding header.
    size_t size;
    size_t free;
} qk_bsl_part_t;

typedef struct qk_hdr {
    /// Magic number. Used to verify if the db is initialized.
    uint64_t magic;
    /// Version of the db. Should be 1.
    uint64_t version;
    /// Session of the db. Initialized with 1 and incremented on every open.
    uint64_t session;
    /// Memory allocator free list. The smallest is 4k and gets
    /// twice as large for each size class.
    qk_block_index_t* free_list[32];
    /// Maximum free list size class. The largest size class that has ever
    /// been allocated. The free list vector is undefined above this point.
    uint64_t free_max_class;
    /// B-Skip-List fields.
    /// Entry pointers for skip-list levels.
    qk_bsl_part_t* skip_list[QK_BSL_LEVELS];
} qk_hdr_t;

CASSERT(sizeof(qk_hdr_t) <= PAGE_SIZE);

struct qk_ctx {
    acid_h* ah;
    qk_hdr_t* header;
};

/// Dynamic memory allocator related code, later to be pulled into librcd.

/// Returns log2(x) rounded down to the nearest integer.
static uint8_t qk_log2(uint64_t value) {
    // First round down to one less than a power of 2.
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    // Lookup in table.
    const uint8_t de_bruijn_table[64] = {
        63,  0, 58,  1, 59, 47, 53,  2,
        60, 39, 48, 27, 54, 33, 42,  3,
        61, 51, 37, 40, 49, 18, 28, 20,
        55, 30, 34, 11, 43, 14, 22,  4,
        62, 57, 46, 52, 38, 26, 32, 41,
        50, 36, 17, 19, 29, 10, 13, 21,
        56, 45, 25, 31, 35, 16,  9, 12,
        44, 24, 15,  8, 23,  7,  6,  5
    };
    return de_bruijn_table[((uint64_t)((value - (value >> 1)) * 0x07EDD5E59A4E28C2UL)) >> 58];
}

static uint8_t qk_value_to_2e(uint64_t value, bool round_up) {
    if ((value & ~1UL) == 0)
        return 0;
    return qk_log2(round_up? (value - 1) << 1: value);
}

static uint8_t qk_bytes_to_pages_2e(size_t bytes, bool round_up) {
    if (bytes <= (1UL << QK_PAGE_SIZE_2E))
        return 1;
    return qk_value_to_2e(bytes, round_up) + 1 - QK_PAGE_SIZE_2E;
}

static size_t qk_pages_2e_to_bytes(uint8_t pages_2e) {
    return (1UL << (pages_2e - 1 + QK_PAGE_SIZE_2E));
}

static void qk_dprint_free_list(qk_ctx_t* ctx) { sub_heap {
    DBG("Free lists contains:");
    for (size_t i = 0; i < 32; i++) {
        fstr_t line = concs("free_list[", i, "]: ");
        qk_block_index_t* next = ctx->header->free_list[i];
        while (next != 0){
            line = concs(line, "->[", next->pages_2e, "]");
            next = next->next;
        }
        DBG(line, "-> [\\0]");
    }
    acid_fsync(ctx->ah);
}}

/// Push a free block into the free list.
static void qk_free_list_push(qk_ctx_t* ctx, void* ptr, uint8_t pages_2e ) {
    // Create a new node for the free list.
    qk_block_index_t* blk_ix = ptr;
    // Point it to the currently next block of its size.
    // Index begins at 1.
    uint8_t fl_ix = pages_2e - 1;
    blk_ix->next = ctx->header->free_list[fl_ix];
    blk_ix->pages_2e = pages_2e;
    // Insert it into the free-list.
    ctx->header->free_list[fl_ix] = blk_ix;
    if (ctx->header->free_max_class < pages_2e)
        ctx->header->free_max_class = pages_2e;
}

/// Get a block that is at least bytes big
static void* qk_free_list_pop(qk_ctx_t* ctx, uint8_t pages_2e) {
    uint8_t fl_ix = pages_2e - 1;
    if (ctx->header->free_list[fl_ix] != 0) {
        // Requested size existed in free list.
        qk_block_index_t* block_idx = ctx->header->free_list[fl_ix];
        ctx->header->free_list[fl_ix] = block_idx->next;
        return (void*) block_idx;
    }
    if (pages_2e >= ctx->header->free_max_class) {
        uint8_t extension_2e = MAX(pages_2e, ctx->header->free_max_class);
        // There was no free memory big enough for the allocation.
        fstr_t current_memory  = acid_memory(ctx->ah);
        void* current_end = current_memory.str + current_memory.len;
        size_t new_size = current_memory.len + qk_pages_2e_to_bytes(extension_2e);
        acid_expand(ctx->ah, new_size);
        qk_free_list_push(ctx, current_end, extension_2e);
        return qk_free_list_pop(ctx, pages_2e);
    }
    // Get a bigger chunk and push half of it.
    void* chunk = qk_free_list_pop(ctx, pages_2e + 1);
    void* half_chunk = chunk + qk_pages_2e_to_bytes(pages_2e);
    qk_free_list_push(ctx, half_chunk, pages_2e);
    return chunk;
}

join_locked(void) qk_free(void* ptr, size_t bytes ,join_server_params, qk_ctx_t* ctx) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    memset(ptr, 0, qk_pages_2e_to_bytes(pages_2e));
    qk_free_list_push(ctx, ptr, pages_2e);
}

join_locked(void*) qk_alloc(size_t bytes ,join_server_params, qk_ctx_t* ctx) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    void* result = qk_free_list_pop(ctx, pages_2e);
    return result;
}

join_locked(void) qk_dprint_free_memory(join_server_params, qk_ctx_t* ctx) {
    qk_dprint_free_list(ctx);
}

/// B-skiplist implementation

inline static fstr_t qk_bsl_node_vec(qk_bsl_part_t* partition) {
    return (fstr_t) {.len = partition->size, .str = (void*) partition + sizeof(qk_bsl_part_t)};
}

inline static fstr_t qk_bsl_unpack_fstr(fstr_t* src) {
    fstr_t unpack = (fstr_t) {.len = *(uint64_t*) src->str,
                              .str = (uint8_t*) src->str + sizeof(uint64_t)};
    *src = fstr_slice(*src, unpack.len + sizeof(unpack.len), -1);
    return unpack;
}


inline static void* qk_bsl_unpack_ptr(fstr_t* src) {
    void* unpack = src->str;
    *src = fstr_slice(*src, sizeof(unpack), -1);
    return unpack;
}

/// Returns number of trials before a succesful outcome where one trial has
/// probability p of success.
static uint64_t qk_geornd(double p) {
    // Random nummer between 0 and 1.
    if (p == 1) {
        return 1;
    } else {
        double ent = (double) UINT64_MAX / lwt_rdrand64();
        return (uint64_t) (log(ent) / log((1 - p) + 1));
    }
}

static inline uint8_t qk_bsl_level() {
    // p = (1 - (1 / gamma))
    return qk_geornd((double) 1 - ((double) 1 / QK_BSL_GAMMA));
}

static inline uint64_t qk_bsl_part_size() {
    // p = (1 / gamma)
    return qk_geornd((double) 1 / QK_BSL_GAMMA);
}

static void qk_bsl_print_state(qk_ctx_t* ctx) {
    for (uint8_t i = QK_BSL_LEVELS; i > 0; i--) {
        qk_bsl_part_t* it = ctx->header->skip_list[i - 1];
        list(fstr_t)* addrlist = new_list(fstr_t);
        while (it != 0) {
            list_push_end(addrlist, fstr_t, concs(it));
            it = it->next;
        }
        DBG("level[", i, "]: [", fss(fstr_implode(addrlist, "] <--> [")), "]");
    }
}

static qk_bsl_part_t* qk_bsl_new_part(size_t min_size, qk_ctx_t* ctx) {
    size_t bytes = MAX(min_size, qk_bsl_part_size());
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    size_t actual_bytes = qk_pages_2e_to_bytes(pages_2e);
    qk_bsl_part_t* part = (qk_bsl_part_t*) qk_free_list_pop(ctx, pages_2e);
    memset(part, 0, qk_pages_2e_to_bytes(pages_2e));
    part->size = actual_bytes - sizeof(qk_bsl_part_t);
    part->free = part->size;
    return part;
}

/// Find the representative partition for key.
static qk_bsl_part_t* qk_bsl_search_fwd(fstr_t key, qk_bsl_part_t* start) {
    qk_bsl_part_t* it = start;
    while (it != 0) {
        // The first key will always be directly after the partition struct.
        fstr_t node_vec = qk_bsl_node_vec(it);
        fstr_t part_key = qk_bsl_unpack_fstr(&node_vec);
        qk_bsl_part_t* next = it->next;
        // The last node is automatically the representative if we get to it.
        if (next == 0) {
            return it;
        }
        if (fstr_cmp_lexical(key, part_key) < 0) {
            return it;
        }
        it = next;
    }
    return it;
}

static void qk_bsl_divide_part(qk_bsl_part_t* part, fstr_t key, bool data_level, fstr_t* o_before, fstr_t* o_rest) {
    fstr_t node_vec = qk_bsl_node_vec(part);
    fstr_t rest = node_vec;
    fstr_t node_vec_tail = node_vec;
    fstr_t n_key = qk_bsl_unpack_fstr(&node_vec_tail);
    while (n_key.len > 0) {
        // Consume the data between keys.
        if (data_level) {
            // On the data level the key is followed by a value.
            fstr_t value = qk_bsl_unpack_fstr(&node_vec_tail);
        } else {
            // On index levels the key is followed by a down pointer.
            qk_bsl_part_t* down = qk_bsl_unpack_ptr(&node_vec_tail);
        }
        // If current key is not too far update rest to tail.
        if (fstr_cmp_lexical(n_key, key) < 0) {
            rest = node_vec_tail;
        }
        n_key = qk_bsl_unpack_fstr(&node_vec_tail);
    }
    rest.len = node_vec_tail.str - rest.str - sizeof(uint64_t);
    *o_rest = rest;
    *o_before = (fstr_t){.str = node_vec.str, .len = rest.str - node_vec.str};
}

static qk_bsl_part_t* qk_insert(fstr_t key, fstr_t value, uint8_t level, qk_bsl_part_t* start, bool force_split, qk_ctx_t* ctx) {
    bool data_level = (level == 1);
    bool split = force_split;
    // Data-node format is [len_key][key][len_data][data]
    // Index-node format is [len_key][key][down_ptr]
    size_t new_size = data_level? sizeof(key.len) + key.len + sizeof(value.len) + value.len
                                 :sizeof(key.len) + key.len + sizeof(qk_bsl_part_t*);
    qk_bsl_part_t* rep_part = qk_bsl_search_fwd(key, start);
    // When a level is empty there wont be a representative.
    if (rep_part == 0) {
        DBGFN("representative was 0");
        // The representative node did not exist so it has to be made and insterted in ctx.
        rep_part = qk_bsl_new_part(new_size, ctx);
        rep_part->prev = 0;
        rep_part->next = ctx->header->skip_list[level - 1];
        ctx->header->skip_list[level - 1] = rep_part;
        // Split is never needed for the first node on a level.
        split = false;
    }
    DBGFN("rep part: ", rep_part);
    // Divide representative partition.
    fstr_t before, rest;
    qk_bsl_divide_part(rep_part, key, data_level, &before, &rest);
    // The partition that will end up with the new node.
    qk_bsl_part_t* dst_part;
    // Split the partition if it won't fit the data or forced by higher level.
    if (rep_part->free < new_size || split) {
        DBGFN("splitting partition");
        // We do not want to recursively split partitions so instead we make a big one.
        size_t min_part_size = new_size + rest.len;
        dst_part = qk_bsl_new_part(min_part_size , ctx);
        DBGFN("rep_part: ", rep_part);
        DBGFN("rep_part -> next: ", rep_part->next);
        DBGFN("rep_part -> prev: ", rep_part->prev);
        DBGFN("dst_part: ", dst_part);


        /// this is wrong.

        dst_part->prev = rep_part;
        dst_part->next = rep_part->next;

        if (dst_part->prev != 0) {
            dst_part->prev->next = dst_part;
        }

        DBGFN("dst_part -> next: ", dst_part->next);
        DBGFN("dst_part -> prev: ", dst_part->prev);
        // If there is nothing left of the partition in front it shall be removed.
        if (before.len == 0) {
            // If the old represantative partition was split and left without nodes
            // it shall be deleted.
            if (rep_part->prev == 0){
                DBG("HO HO");
                ctx->header->skip_list[level - 1] = dst_part;
                dst_part->prev = 0;
            } else {
                rep_part->prev->next = dst_part;
                dst_part->prev = rep_part->prev;
            }
            size_t p_bytes = rep_part->size + sizeof(qk_bsl_part_t);
            uint8_t p_pages_2e = qk_bytes_to_pages_2e(p_bytes, true);
            qk_free_list_push(ctx, rep_part, p_pages_2e);
        }
        before = "";
    } else{
        dst_part = rep_part;
    }
    DBGFN("before this node:[", fss(fstr_hexencode(before)), "]");
    DBGFN("after this node:[", fss(fstr_hexencode(rest)), "]");
    // Move the tail part into to dst segment before the new node.
    fstr_t dst_node_vec = qk_bsl_node_vec(dst_part);
    // DBGFN("newsize ", new_size);
    // fstr_t tail_node_segment = fstr_slice(dst_node_vec, new_size + before.len , -1);
    memmove(dst_node_vec.str + new_size, rest.str, rest.len);
    // Remove the tail from its old partition so it is not in two partitions.
    memset(rest.str, 0, MIN(new_size, rest.len));
    // Copy in the new node.
    fstr_t new_node_segment = fstr_slice(dst_node_vec, before.len, before.len + new_size);
    if (data_level) {
        // Insert the new k:v.
        *(uint64_t*) new_node_segment.str = key.len;
        new_node_segment.str += sizeof(key.len);
        memcpy(new_node_segment.str, key.str, key.len);
        new_node_segment.str += key.len;
        *(uint64_t*) new_node_segment.str = value.len;
        new_node_segment.str += sizeof(value.len);
        memcpy(new_node_segment.str, value.str, value.len);
    } else {
        // The start search node is the node below the last node before the new node.
        // If the new node is the new first node on this level use the entry node instead.
        qk_bsl_part_t* next_search_start;
        if (before.len > 0) {
            // It is safe to "go back".
            next_search_start = (void*) rest.str - sizeof(qk_bsl_part_t*);
        } else {
            // Levels start at 1.
            next_search_start = ctx->header->skip_list[level - 2];
        }
        // Paritions below insert level must be split.
        qk_bsl_part_t* part_below = qk_insert(key, value, level - 1, next_search_start, true, ctx);
         *(uint64_t*) new_node_segment.str = key.len;
        new_node_segment.str += sizeof(key.len);
        memcpy(new_node_segment.str, key.str, key.len);
        new_node_segment.str += key.len;
        *(qk_bsl_part_t**) new_node_segment.str = part_below;
    }
    dst_part->free -= new_size;
    return dst_part;
}

/// Find the representative partition for key.
static qk_bsl_part_t* qk_bsl_lookup(fstr_t key, qk_bsl_part_t* start) {
    return 0;
}

join_locked(void) qk_push_inner(fstr_t key, fstr_t value, join_server_params,  qk_ctx_t* ctx) {
    uint8_t insert_level = 1;
    DBGFN(key);
    qk_insert(key, value, insert_level, ctx->header->skip_list[0], true, ctx);
}

void qk_push(sf(quark)* sf, fstr_t key, fstr_t value) {
//    DBGFN("pushing key [", key, "] with value [", value, "]");
    qk_push_inner(key, value, quark_sf2id(sf).fid);
}

list(fstr_t)* qk_slice(sf(quark)* sf,fstr_t from_key, fstr_t to_key) {
    //FIXME:: implement
    return 0;
}

join_locked(void) qk_bsl_print_lists(join_server_params,  qk_ctx_t* ctx) {
    qk_bsl_print_state(ctx);
}

void qk_bls_print(sf(quark)* sf) {
    qk_bsl_print_lists(quark_sf2id(sf).fid);
}

fiber_main_t(quark) qk_fiber(fiber_main_attr, acid_h* ah) { try {
    // Make sure that there is enough space to initialize the required structures.
    size_t min_size = (1 << QK_MIN_EXT_SIZE_2E) + PAGE_SIZE;
    acid_expand(ah, min_size);
    // Put the state at the start of the memory range.
    fstr_t am = acid_memory(ah);
    qk_ctx_t ctx = {.header = (qk_hdr_t*) am.str, .ah = ah};
    if (ctx.header->magic == 0) {
        // This is a new database, it has to be initialized.
        ctx.header->magic = QK_HEADER_MAGIC;
        ctx.header->version = QK_VERSION;
        ctx.header->session = 1;
        // The if this really is a fresh acid memory chunk this is redundant.
        memset(ctx.header->free_list, 0, sizeof(ctx.header->free_list));
        ctx.header->free_max_class = 0;
        // First free block will start one page up in our acid memory.
        fstr_t free_block = fstr_slice(am, PAGE_SIZE, -1);
        qk_free_list_push(&ctx, free_block.str, qk_bytes_to_pages_2e(free_block.len, false));
        // Initialize the empty b-skip-list.
        memset(ctx.header->skip_list, 0, sizeof(ctx.header->skip_list));
    } else if (ctx.header->magic == QK_HEADER_MAGIC) {
        // We are opening an existing database;
        if (ctx.header->version != QK_VERSION) {
            throw_eio("bad database version", quark);
        }
        ctx.header->session += 1;
    } else {
        // This is not a valid database or empty acid memory range.
        throw_eio("corrupt or invalid database", quark);
    }
    acid_fsync(ah);
    // The allocation and free functions are mainly exposed for testing reasons.
    auto_accept_join(qk_free,
                     qk_alloc,
                     qk_dprint_free_memory,
                     qk_bsl_print_lists,
                     qk_push_inner,
                     qk_bsl_print_lists,
                     join_server_params, &ctx);
    } catch(exception_desync, e) {
        /// FIXME: might wanna do something here later.
    } finally {
        acid_close(ah);
    }
}

sf(quark)* qk_init(acid_h* ah) {
    fmitosis {
        // acid handles are not allocated in a heap, it is its own mmap.
        return spawn_fiber(qk_fiber("", ah));
    }
}
