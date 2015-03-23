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

typedef struct qk_block_index {
    struct qk_block_index* next;
    uint8_t pages_2e;
} qk_block_index_t;

// PACKED qk_hdr
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
} qk_hdr_t;

CASSERT(sizeof(qk_hdr_t) <= PAGE_SIZE);

struct qk_ctx {
    acid_h* ah;
    qk_hdr_t* header;
};
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
    } else if (ctx.header->magic == QK_HEADER_MAGIC) {
        // We are opening an existing database;
        if (ctx.header->version != QK_VERSION) {
            // Can't open any other version than current.
        }
        ctx.header->session += 1;
    } else {
        // This is not a valid database or empty acid memory range.
        /*throw_eio( */
        return;
    }
    acid_fsync(ah);
    auto_accept_join(qk_free,
                     qk_alloc,
                     qk_dprint_free_memory,
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
