#import "rcd.h"
#import "musl.h"
#import "acid.h"
#import "quark-internal.h"

#pragma librcd

#define QK_HEADER_MAGIC 0x6aef91b6b454b73f
#define QK_VERSION 2

/// Smallest possible allocation size: 12 2e = 2^12 = 4kib.
#define QK_PAGE_SIZE_2E 12

/// The default "untuned" target items per partition.
#define QK_DEFAULT_TARGET_IPP (20)

/// Sanity checks state.
#define QK_SANTIY_CHECK(x) qk_santiy_check((x), __FILE__, __LINE__)

typedef struct qk_idx {
    uint16_t keylen;
    uint8_t* keyptr;
} __attribute__((packed, aligned(1))) qk_idx_t;

/// B-Skip-List partition header.
/// [ header, sorted index space --> , gap, <-- data space ]
/// index structure: {
///     uint16_t keylen; (length of key)
///     uint8_t* keyptr; (offset to key from partition start)
/// }
/// level0 data structure: {
///     uint8_t key[]; (key, length found in index)
///     uint64_t valuelen;
///     uint8_t value[];
/// }
/// level1+ data structure: {
///     uint8_t key[]; (key, length found in index)
///     qk_part_t* down;
/// }
typedef struct qk_part {
    // Total size of the partition in bytes.
    uint64_t total_size;
    // Number of keys in partition.
    uint32_t n_keys;
    // Size of end value segments in bytes in partition.
    uint64_t data_size;
} __attribute__((packed, aligned(1))) qk_part_t;

/// Global quark header.
typedef struct qk_hdr {
    /// Magic number. Used to verify if the db is initialized.
    uint64_t magic;
    /// Version of the db. Should be 1.
    uint64_t version;
    /// Session of the db. Initialized with 1 and incremented on every open.
    uint64_t session;
    /// TODO: When this is non-zero, don't use variable key length. Should be 0 for now.
    uint64_t static_key_size;
    /// Total number of items in index.
    uint64_t total_count;
    /// Total size used by all items in index, including overhead in level 0.
    uint64_t total_level0_use;
    /// Tuning parameter: the target items per partition.
    /// Can be set freely on open if requested.
    uint16_t target_ipp;
    /// Memory allocator free list. The smallest is 4k and gets
    /// twice as large for each size class.
    void* free_list[32];
    /// End free list size class. The first free list class that is larger than what has
    /// ever been allocated. The free list vector is undefined at and above this point.
    uint8_t free_end_class;
    /// B-Skip-List root, an entry pointer for each level.
    qk_part_t* root[8];
} qk_hdr_t;

CASSERT(sizeof(qk_hdr_t) <= PAGE_SIZE);

struct qk_ctx {
    acid_h* ah;
    qk_hdr_t* hdr;
};

static noret void qk_throw_sanity_error(fstr_t file, int64_t line) {
    throw(concs("quark detected fatal memory corruption or algorithm error at ", file, ":", line), exception_fatal);
}

static inline void qk_santiy_check(bool check, fstr_t file, int64_t line) {
    if (!check) {
        qk_throw_sanity_error(file, line);
    }
}

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
        return 0;
    return qk_value_to_2e(bytes, round_up) - QK_PAGE_SIZE_2E;
}

static size_t qk_pages_2e_to_bytes(uint8_t pages_2e) {
    return (1UL << (pages_2e + QK_PAGE_SIZE_2E));
}

/*
static void qk_dprint_free_list(qk_ctx_t* ctx) { sub_heap {
    //DBG("Free lists contains:");
    for (size_t i = 0; i < 32; i++) {
        fstr_t line = concs("free_list[", i, "]: ");
        qk_block_index_t* next = ctx->hdr->free_list[i];
        while (next != 0){
            line = concs(line, "->[", next->pages_2e, "]");
            next = next->next;
        }
        //DBG(line, "-> [\\0]");
    }
    acid_fsync(ctx->ah);
}}*/

/// Free memory of the specified size class.
static void qk_vm_push(qk_ctx_t* ctx, void* block, uint8_t pages_2e) {
    qk_hdr_t* hdr = ctx->hdr;
    assert(pages_2e < hdr->free_end_class);
    *((void**) block) = hdr->free_list[pages_2e];
    hdr->free_list[pages_2e] = block;
}

/// Allocate memory of specified size class.
static void* qk_vm_pop(qk_ctx_t* ctx, uint8_t pages_2e) {
    qk_hdr_t* hdr = ctx->hdr;
    for (uint8_t i_2e = pages_2e;; i_2e++) {
        void* block;
        size_t block_len;
        if (i_2e >= hdr->free_end_class) {
            // Out of memory, need to reserve more.
            hdr->free_end_class = i_2e + 1;
            block_len = qk_pages_2e_to_bytes(i_2e);
            fstr_t amem = acid_memory(ctx->ah);
            acid_expand(ctx->ah, amem.len + block_len);
            block = amem.str + amem.len;
            goto has_rblock;
        }
        block = hdr->free_list[i_2e];
        if (block != 0) {
            // Pop block from free list.
            hdr->free_list[i_2e] = *((void**) block);
            // Split block and reinsert until we reach the required size.
            block_len = qk_pages_2e_to_bytes(i_2e);
            has_rblock:;
            for (; i_2e > pages_2e; i_2e--) {
                *((void**) block) = hdr->free_list[i_2e];
                hdr->free_list[i_2e] = block;
                block_len /= 2;
                block += block_len;
            }
            return block;
        }
    }
}

static void* qk_vm_alloc(qk_ctx_t* ctx, uint64_t bytes, uint64_t* out_bytes) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    if (out_bytes != 0)
        *out_bytes = qk_pages_2e_to_bytes(pages_2e);
    return qk_vm_pop(ctx, pages_2e);
}

static void qk_vm_free(qk_ctx_t* ctx, void* ptr, uint64_t bytes) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    qk_vm_push(ctx, ptr, pages_2e);
}

join_locked(void*) qk_alloc(uint64_t bytes, uint64_t* out_bytes, join_server_params, qk_ctx_t* ctx) {
    return qk_vm_alloc(ctx, bytes, out_bytes);
}

join_locked(void) qk_free(void* ptr, uint64_t bytes, join_server_params, qk_ctx_t* ctx) {
    qk_vm_free(ctx, ptr, bytes);
}

/// B-skiplist implementation

/// Takes an lvl0 index and resolves the value.
static inline fstr_t qk_idx0_get_value(qk_idx_t* idx) {
    uint64_t* valuelen = *((void**) (idx->keyptr + idx->keylen));
    uint8_t* valuestr = (void*) (valuelen + 1);
    fstr_t value = {
        .str = valuestr,
        .len = *valuelen,
    };
    return value;
}

/// Takes an lvl1+ index and resolves the down pointer reference.
static inline qk_part_t** qk_idx1_get_down_ptr(qk_idx_t* idx) {
    return (void*) (idx->keyptr + idx->keylen);
}

/// Allocates a new empty partition.
static inline qk_part_t* qk_part_alloc_new(qk_ctx_t* ctx, uint64_t req_space) {
    size_t part_size;
    uint64_t min_size = sizeof(qk_part_t) + req_space;
    qk_part_t* part = qk_vm_alloc(ctx, min_size, &part_size);
    *part = (qk_part_t) {
        .total_size = part_size
    };
    return part;
}

/// Frees a no longer used and no longer referenced partition.
static inline void qk_part_alloc_free(qk_ctx_t* ctx, qk_part_t* part) {
    qk_vm_free(ctx, part, part->total_size);
}

/// Returns the first index entity (at offset 0) in a partition.
static inline qk_idx_t* qk_part_get_idx0(qk_part_t* part) {
    return (void*) part + sizeof(*part);
}

/// Returns the start write pointer (at first allocated byte) in partition tail.
static inline qk_idx_t* qk_part_get_write0(qk_part_t* part) {
    return ((void*) part) + part->total_size - part->data_size;
}

/// Returns the number of bytes required to store a certain key/value pair at a certain level.
static inline uint64_t qk_space_kv_level(uint8_t level, fstr_t key, fstr_t value) {
    /// level0: [qk_idx_t] <--- free space ---> ["key"][qk_part_t*]
    /// level1+: [qk_idx_t] <--- free space ---> ["...key..."][uint64_t: valuelen]["...value..."]
    uint64_t size = sizeof(qk_idx_t) + key.len;
    if (level > 0) {
        size += sizeof(uint64_t) + value.len;
    } else {
        size += sizeof(qk_part_t*);
    }
    return size;
}

static inline uint64_t qk_space_idx_data_level(uint8_t level, qk_idx_t* idx) {
    uint64_t space = idx->keylen;
    if (level > 0) {
        space += sizeof(qk_part_t*);
    } else {
        space += sizeof(uint64_t);
        space += qk_idx0_get_value(idx).len;
    }
    return space;
}

static inline uint64_t qk_space_range_level(uint8_t level, qk_idx_t* idxS, qk_idx_t* idxE) {
    uint64_t space = 0;
    for (qk_idx_t* idxC = idxS; idxC < idxE; idxC++) {
        space += sizeof(qk_idx_t);
        space += qk_space_idx_data_level(level, idxC);
    }
    return space;
}

/// Cross partition copy of entries in a source range from a
/// source partition to destination partition on the same level.
/// The partition meta data is not updated to reflect the change.
static void qk_part_insert_entry_range(uint8_t level, qk_part_t* dst_part, qk_idx_t* idxS, qk_idx_t* idxSE) {
    qk_idx_t* idx0 = qk_part_get_idx0(dst_part);
    void* write0 = qk_part_get_write0(dst_part);
    void* writeD = write0;
    qk_idx_t* idxD = idx0 + dst_part->n_keys;
    for (; idxS < idxSE; idxS++, idxD++) {
        // Copy entry data.
        size_t dsize = qk_space_idx_data_level(level, idxS);
        writeD -= dsize;
        memcpy(writeD, idxS->keyptr, dsize);
        // Write index.
        idxD->keylen = idxS->keylen;
        idxD->keyptr = writeD;
        // Assert that we had space left, the caller is responsible for this.
        assert((void*) (idxD + 1) <= writeD);
    }
    dst_part->n_keys = (idxD - idx0);
    dst_part->data_size += (write0 - writeD);
}

/// Inserts an entry into a partition at a target index.
/// When idxT is null the function will add the entity to the end of the index.
/// References are returned to the "left" and "right" down pointers. The right
/// down pointer corresponds to the the inserted entry. The left down pointer
/// corresponds to the entry immediately lower than the inserted entry and can
/// be returned as null when targeting the leftmost index.
/// The caller is responsible for:
///     - Looking up the correct target index to insert into and passing it
///       into idxT. Inserting here should preserve the sorted property of
///       the index.
///     - Ensuring that the partition has room for the insert.
///     - Updating the returned right and left down pointer references
///       as required/applicable.
static void qk_part_insert_entry(
    uint8_t level, qk_part_t* dst_part, qk_idx_t* idxT,
    fstr_t key, fstr_t value,
    qk_part_t*** out_downL, qk_part_t*** out_downR
) {
    // Write entry data.
    void* write0 = qk_part_get_write0(dst_part);
    void* writeD = write0;
    if (level > 0) {
        // Allocate down pointer in data and return pointer to it so caller
        // can write it when the down partition has been resolved.
        writeD -= sizeof(qk_part_t*);
        // Writing right down pointer is up to caller.
        *out_downR = (qk_part_t**) writeD;
    } else {
        // Allocate value and write it.
        writeD -= value.len;
        memcpy(writeD, value.str, value.len);
        // Allocate valuelen and write it.
        writeD -= sizeof(uint64_t);
        *((uint64_t*) writeD) = value.len;
    }
    // Allocate key memory and write it.
    writeD -= key.len;
    memcpy(writeD, key.str, key.len);
    // Resolve destination index.
    qk_idx_t* idx0 = qk_part_get_idx0(dst_part);
    qk_idx_t* idxE = idx0 + dst_part->n_keys;
    if (idxT == 0) {
        // Append to index.
        idxT = idxE;
    } else if (idxT < idxE) {
        // Insertion sort insert:
        // Move all indexes forward to make room for insert into target index.
        memmove(idxT + 1, idxT, (idxE - idxT) * sizeof(*idxT));
    }
    assert(idxT >= idx0 && idxT <= idxE);
    // Write index.
    idxT->keylen = key.len;
    idxT->keyptr = writeD;
    // Update partition meta data.
    dst_part->n_keys++;
    dst_part->data_size += (write0 - writeD);
    // Resolve left down pointer.
    if (level > 0) {
        *out_downL = (idxT > idx0)? qk_idx1_get_down_ptr(idxT - 1): 0;
    }
}

/// Returns the number of bytes of free space in a partition.
static inline uint64_t qk_part_free_space(qk_part_t* part) {
    return part->total_size
        - sizeof(qk_part_t)
        - part->n_keys * sizeof(qk_idx_t)
        - part->data_size;
}

/// Reallocates a partition so it can support the requested amount of free space.
/// This deallocates the old partition and allocates a new partition that
/// replaces it with the necessary amount of free space.
/// The new partition has all internal pointers updated as required.
/// The function will however not update any external references, it leaves
/// that responsibility to the caller.
static qk_part_t* qk_part_realloc(qk_ctx_t* ctx, qk_part_t* part, uint64_t req_space) {
    // Expand partition capacity.
    uint64_t min_new_size = part->total_size + req_space;
    uint64_t new_size;
    qk_part_t* new_part = qk_vm_alloc(ctx, min_new_size, &new_size);
    // Copy data of entities. No internal translation is required.
    {
        void* data_dst = ((void*) new_part) + new_size - part->data_size;
        void* data_src = ((void*) part) + new_size - part->data_size;
        memcpy(data_dst, data_src, part->data_size);
    }
    // Copy keys of entities with translated key pointers.
    {
        int64_t part_offs =  (void*) new_part - (void*) part;
        int64_t part_data_offs = (int64_t) new_size - (int64_t) part->total_size + part_offs;
        qk_idx_t* idxC_old = qk_part_get_idx0(part);
        qk_idx_t* idxE_old = idxC_old + part->n_keys;
        qk_idx_t* idxC_new = qk_part_get_idx0(new_part);
        for (; idxC_old < idxE_old; idxC_old++, idxC_new++) {
            idxC_new->keylen = idxC_old->keylen;
            idxC_new->keyptr = idxC_old->keyptr + part_data_offs;
        }
    }
    // Initialize header.
    new_part->total_size = new_size;
    new_part->n_keys = part->n_keys;
    new_part->data_size = part->data_size;
    // Free old partition.
    qk_part_alloc_free(ctx, part);
    // Using new expanded partition now.
    assert(qk_part_free_space(new_part) >= req_space);
    return new_part;
}

/// After reallocating a partition it can be necessary to translate pointers into it.
/// This function takes a pointer and reallocation information and checks if translation
/// is required and returns the translated pointer.
static inline void* qk_part_realloc_translate(void* ptr, void* old_part, uint64_t old_size, qk_part_t* new_part) {
    if (ptr >= old_part && ptr < old_part + old_size) {
        int64_t part_offs = (void*) new_part - old_part;
        if (ptr < old_part + old_size - new_part->data_size) {
            // Pointer to index.
            ptr += part_offs;
        } else {
            // Pointer to data.
            int64_t part_data_offs = ((int64_t) new_part->total_size - (int64_t) old_size) + part_offs;
            ptr += part_data_offs;
        }
    }
    return ptr;
}

/// Binary search in a partition index for specified key and returns the index for it.
/// When the key is not found false is returned and a pointer to the index where the
/// key should be inserted.
static bool qk_idx_lookup(qk_idx_t* idx0, qk_idx_t* idxE, fstr_t keyT, qk_idx_t** out_idxT) {
    /// The range we are searching are from start (inclusive) to end (exclusive).
    qk_idx_t* idxS = idx0;
    qk_idx_t* idxC;
    int64_t cmp = 0;
    while (idxE > idxS) {
        idxC = idxS + ((idxE - idxS) / 2);
        fstr_t keyC = {.str = idxC->keyptr, .len = idxC->keylen};
        cmp = fstr_cmp_lexical(keyC, keyT);
        if (cmp < 0) {
            // keyC is too low. We look to the right after a higher keyC.
            idxS = (idxC + 1);
        } else if (cmp > 0) {
            // keyC is too high. We look to the left after a lower keyC.
            idxE = idxC;
        } else if (cmp == 0) {
            // Key found.
            *out_idxT = idxC;
            return true;
        }
    }
    // Based on last compare we can find the node than is less than the node we searched for.
    if (cmp < 0) {
        // Too low and last value. We must have looked at the largest value lower than keyT last.
        // This means that the target index is to the right.
        *out_idxT = idxC + 1;
    } else if (cmp > 0) {
        // Too high and last value. We must have looked at the smallest value higher than keyT last.
        // This means that this is our target index.
        *out_idxT = idxC;
    } else if (cmp == 0) {
        // No key looked at. Index is empty. The target index is the first.
        assert(idx0 == idxE);
        *out_idxT = idx0;
    }
    return false;
}

static bool qk_insert(qk_ctx_t* ctx, fstr_t key, fstr_t value) {
    qk_hdr_t* hdr = ctx->hdr;
    // Approximate the overhead of this entity.
    uint64_t est_level0_use = (key.len + value.len + 0x10);
    // Calculate average entity level0 size for self-tuning probability.
    uint64_t avg_ent_size = (est_level0_use + hdr->total_level0_use) / (hdr->total_count + 1);
    // Get target partition size.
    // Force it to be at least x2 the size of avg_ent_size for a worst case coin toss probability of 50/50 (binary skip list).
    uint64_t target_part_size = MAX(hdr->target_ipp * avg_ent_size, avg_ent_size * 2);
    // Mask the target partition size and round up as partitions must have a power of two size.
    // This also gets rid of the (negligible) non-biased "partial-bit" randomness problem.
    uint64_t target_ps_mask = (1ULL << qk_value_to_2e(target_part_size, true)) - 1;
    // Calculate the level to insert node at through a series of presumably heavily biased coin tosses.
    // Due to the heavy bias of the coin toss it should be faster to do this numerically than using software math.
    uint8_t insert_lvl = 0;
    do {
        uint64_t n = lwt_rdrand64() & target_ps_mask;
        if (n < avg_ent_size)
            break;
        insert_lvl++;
    } while (insert_lvl < LENGTHOF(hdr->root) - 1);
    // We have generated a fair insert level and is ready to begin insert.
    // Read phase: Search from top level to:
    //  1) Resolve target partitions to insert on.
    //  2) Resolve insert level target partition reference.
    //  3) Resolve all target lte indexes of all target partitions.
    //  4) See if key is already inserted.
    struct {
        /// Target partition to insert or split.
        qk_part_t* part;
        /// Target index slot to insert key.
        qk_idx_t* idxT;
    } target[LENGTHOF(hdr->root)];
    /// All partitions has exactly one incoming reference from root or from above.
    /// This is that reference for the insert level target partition.
    qk_part_t** insert_ref;
    // Start search.
    bool following_root = true;
    qk_part_t** next_ref;
    for (size_t i_lvl = LENGTHOF(hdr->root) - 1;; i_lvl--) {
        // Use level entry partition root reference when following root.
        if (following_root) {
            next_ref = &hdr->root[i_lvl];
        }
        // Resolve partition.
        qk_part_t* part = *next_ref;
        assert(part != 0);
        // Search partition index.
        qk_idx_t* idx0 = qk_part_get_idx0(part);
        qk_idx_t* idxE = idx0 + part->n_keys;
        qk_idx_t* idxT;
        if (qk_idx_lookup(idx0, idxE, key, &idxT)) {
            // Key already inserted!
            return false;
        }
        assert(idxT >= idx0 && idxT <= idxE);
        // Store resolved target if it's required later during write.
        if (i_lvl <= insert_lvl) {
            target[i_lvl].part = part;
            target[i_lvl].idxT = idxT;
        }
        if (i_lvl == insert_lvl) {
            insert_ref = next_ref;
        }
        // Travel further.
        if (i_lvl == 0) {
            // Node was not found.
            break;
        } else if (i_lvl > 0) {
            // Determine how to reference the next level.
            if (idxT == idx0) {
                // All keys are higher than the key we are inserting or there are no keys.
                // This is only possible if we made a wrong turn here from the skip list root because when we
                // follow a down pointer (from lower key) the sub partition contains the (lower) key we followed.
                // In this case we must continue to follow the skip list root down.
                QK_SANTIY_CHECK(following_root);
            } else {
                // We follow the key that is immediately lower than the key where
                // are inserting down to the next partition.
                next_ref = qk_idx1_get_down_ptr(idxT - 1);
                // Here we are no longer following the safety of the root but
                // a misty winding trail of non-cached memory.
                following_root = false;
            }
        }
    }
    // Write phase.
    // Calculate required insert space at entry level.
    uint64_t req_space = qk_space_kv_level(insert_lvl, key, value);
    // Start mutation.
    qk_part_t **downL, **downR;
    qk_part_t** prev_down_ptr = 0;
    for (size_t i_lvl = insert_lvl;;) {
        // Go to next resolved target partition
        qk_part_t* part = target[i_lvl].part;
        qk_idx_t* idxT = target[i_lvl].idxT;
        if (i_lvl == insert_lvl) {
            // At insert level we do a normal insert without any split.
            // Make sure the partition has enough space.
            uint64_t free_space = qk_part_free_space(part);
            if (free_space < req_space) {
                // We must reallocate the partition so we can have room for insert.
                void* old_part_ptr = part;
                uint64_t old_size = part->total_size;
                qk_part_t* new_part = qk_part_realloc(ctx, part, req_space);
                // Translate reference to target index. This faster than doing a new lookup.
                int64_t part_offs = (void*) new_part - (void*) part;
                idxT = ((void*) idxT) + part_offs;
                // Update old partition reference (root pointer or a down pointer) to point to new partition.
                *insert_ref = new_part;
                // We are now "on" the new partition.
                part = new_part;
            }
            // Insert the entity now at the resolved target index.
            // Also resolve initial left and right down reference for split phase.
            qk_part_insert_entry(i_lvl, part, idxT, key, value, &downL, &downR);
        } else {
            assert(i_lvl < insert_lvl);
            // Partition split mode.
            // We can only have come here from normal insert at level above
            // with a down pointer that is pending to be written.
            assert(downR != 0);
            // Right and left is empty (partition empty): just insert element, no split is equivalent to split.
            // Right is empty: allocate new right partition and insert on, no move required.
            // Left is empty: allocate new left partition and insert on, no move required.
            // Right and left both has elements: standard "hard" split.
            qk_idx_t* idx0 = qk_part_get_idx0(part);
            qk_idx_t* idxE = idx0 + part->n_keys;
            assert(idxT >= idx0 && idxT <= idxE);
            bool left_empty = (idxT == idx0);
            bool right_empty = (idxT == idxE);
            qk_part_t **next_downL, **next_downR;
            if (right_empty && left_empty) {
                // No split required, just insert.
                qk_part_insert_entry(i_lvl, part, 0, key, value, &next_downL, &next_downR);
            } else if (right_empty || left_empty) {
                // No move required. Allocate new partition to insert on.
                part = qk_part_alloc_new(ctx, req_space);
                qk_part_insert_entry(i_lvl, part, 0, key, value, &next_downL, &next_downR);
                // Left down pointer update not required. Partition is maintained in place.
            } else {
                // Standard "hard" split.
                // Calculate required space for new left and right partition.
                assert(idxT != 0);
                uint64_t spaceL = qk_space_range_level(i_lvl, idx0, idxT);
                uint64_t spaceR = req_space + qk_space_range_level(i_lvl, idxT, idxE);
                // Allocate new left + right partition.
                qk_part_t* partL = qk_part_alloc_new(ctx, spaceL);
                qk_part_t* partR = qk_part_alloc_new(ctx, spaceR);
                // Copy all entries to the left over to the left partition.
                qk_part_insert_entry_range(i_lvl, partL, idx0, idxT);
                // First element we insert in right partition is the new entity.
                qk_part_insert_entry(i_lvl, partR, 0, key, value, &next_downL, &next_downR);
                // Copy all entries to the right over to the right partition.
                qk_part_insert_entry_range(i_lvl, partR, idxT, idxE);
                // Update left down pointer to point to the new left partition.
                if (downL != 0) {
                    assert(*downL == part);
                    *downL = partL;
                } else {
                    // Root entry partitions are not referenced by any down pointer
                    // so in that and only that case we get a null downL.
                    assert(hdr->root[i_lvl] == part);
                    hdr->root[i_lvl] = partL;
                }
                // Deallocate the old partition.
                qk_part_alloc_free(ctx, part);
                // We are now "on" the right partition.
                part = partR;
            }
            // Split complete.
            // Write previous down pointer so it addresses the new partition.
            *downR = part;
            // Pass left/right down pointers to lower level insert.
            downR = next_downR;
            downL = next_downL;
        }
        // Go down one level.
        if (i_lvl == 0) {
            break;
        }
        i_lvl--;
        if (i_lvl == 0) {
            // Level zero has new space requirements.
            req_space = qk_space_kv_level(i_lvl, key, value);
        }
    }
    // Insert complete.
    return true;
}

fiber_main_t(quark) qk_fiber(fiber_main_attr, acid_h* ah) {
    try {
        // The allocation and free functions are mainly exposed for testing reasons.
        auto_accept_join(
            qk_free,
            qk_alloc,
            join_server_params,
            &ctx
        );
    } finally {
        acid_close(ah);
    }
}

sf(quark)* qk_init(acid_h* ah, qk_opt_t* opt) {
    fmitosis {
        // Read header.
        fstr_t am = acid_memory(ah);
        qk_ctx_t ctx = {
            .hdr = (void*) am.str,
            .ah = ah,
        };
        bool tune_target_ipp;
        if (ctx.hdr->magic == 0) {
            // This is a new database, initialize it.
            memset(ctx.hdr, 0, sizeof(*ctx.hdr));
            ctx.hdr->magic = QK_HEADER_MAGIC;
            ctx.hdr->version = QK_VERSION;
            // Allocate root entry partitions for all levels.
            for (uint8_t i_lvl = 0; i_lvl < LENGTHOF(ctx.hdr->root); i_lvl--) {
                ctx.hdr->root[i_lvl] = qk_part_alloc_new(&ctx, 0);
            }
            tune_target_ipp = true;
        } else if (ctx.hdr->magic == QK_HEADER_MAGIC) {
            // We are opening an existing database;
            if (ctx.hdr->version != QK_VERSION) {
                throw_eio("bad database version", quark);
            }
            tune_target_ipp = opt->overwrite_target_ipp;
        } else {
            // This is not a valid database.
            throw_eio("corrupt or invalid database", quark);
        }
        // Tune target partition size if requested.
        if (tune_target_ipp) {
            ctx.hdr->target_ipp = ((opt->target_ipp != 0)? opt->target_ipp: QK_DEFAULT_TARGET_IPP);
        }
        // Increment session and make first fsync to fail here if write does not work.
        ctx.hdr->session += 1;
        acid_fsync(ah);
        // Start handling database requests.
        return spawn_fiber(qk_fiber("", ah));
    }
}
