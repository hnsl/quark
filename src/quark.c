/* Copyright © 2014, Jumpstarter AB.
 * Quark b-skip list implementation. */

#include "quark-internal.h"
#include "hmap.h"

#pragma librcd

noret void qk_throw_sanity_error(fstr_t file, int64_t line) {
    throw(concs("quark detected fatal memory corruption or algorithm error at ", file, ":", line), exception_fatal);
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

/// Returns the number of bytes required to store a certain key/value pair at a certain level.
static inline uint64_t qk_space_kv_level(uint8_t level, fstr_t key, fstr_t value) {
    /// level0: [qk_idx_t] <--- free space ---> ["key"][qk_part_t*]
    /// level1+: [qk_idx_t] <--- free space ---> ["...key..."][uint64_t: valuelen]["...value..."]
    uint64_t size = sizeof(qk_idx_t) + key.len;
    if (level > 0) {
        size += sizeof(qk_part_t*);
    } else {
        size += sizeof(uint64_t) + value.len;
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
        // Writing right down pointer is required by the caller.
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
    assert((void*) (idxE + 1) <= writeD);
    // Write index.
    idxT->keylen = key.len;
    idxT->keyptr = writeD;
    // Update partition meta data.
    dst_part->n_keys++;
    dst_part->data_size += (write0 - writeD);
    // Resolve left down pointer if requested.
    if (level > 0 && out_downL != 0) {
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
        void* data_src = ((void*) part) + part->total_size - part->data_size;
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

bool qk_insert(qk_ctx_t* ctx, fstr_t key, fstr_t value) {
    qk_hdr_t* hdr = ctx->hdr;
    // Approximate the overhead of this entity.
    uint64_t est_level0_use = (key.len + value.len + 0x10);
    // Calculate average entity level0 size for self-tuning probability.
    uint64_t avg_ent_size = (est_level0_use + hdr->total_level0_use) / (hdr->total_count + 1);
    // Get target partition size.
    // Force it to be at least x2 the size of avg_ent_size for a worst case coin toss probability of 50/50 (binary skip list).
    // TODO: Target partition size cannot be smaller than minimum allocation size.
    // Minimum allocation size is likely to large as well... it's pointlessly large.
    uint64_t target_part_size = MAX(hdr->target_ipp * avg_ent_size, avg_ent_size * 2);
    // Mask the target partition size and round up as partitions must have a power of two size.
    // This also gets rid of the (negligible) non-biased "partial-bit" randomness problem.
    uint64_t target_ps_mask = (1ULL << qk_value_to_2e(target_part_size, true)) - 1;
    // Calculate the level to insert node at through a series of presumably heavily biased coin tosses.
    // Due to the heavy bias of the coin toss it should be faster to do this numerically than using software math.
    uint8_t insert_lvl = 0;
    do {
        uint64_t dice;
        if (hdr->dtrm_seed == 0) {
            dice = lwt_rdrand64();
        } else {
            dice = hmap_murmurhash_64a(key.str, key.len, hdr->dtrm_seed + insert_lvl);
        }
        dice = dice & target_ps_mask;
        // DBGFN("dice roll: ", avg_ent_size, "/", dice, "/", target_ps_mask);
        if (dice >= avg_ent_size)
            break;
        insert_lvl++;
    } while (insert_lvl < LENGTHOF(hdr->root) - 1);
    DBGFN("inserting [", key, "] => [", value, "] on level #", insert_lvl);
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
                DBGFN("reallocating partition ", part);
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
            // There are two main split scenarios:
            // * Right is empty and/or left is empty: no move is required. We can simply allocate new left
            //   or right "side" partition and insert on. The left side can only be empty when we made a
            //   leftmost insert on the insert level (followed from root). In this case if the right is
            //   *not* empty the current partition must be a root entry partition and the new partition
            //   is inserted on its left side, ripping the it from the level root.
            //   The new partition can however *not* be the new level entry partition since it's referenced
            //   from above (we are splitting) which means that the level root entry partition is empty
            //   (split resulted in zero elements) so we must allocate an empty partition and attach to it.
            // * Right and left both has elements: standard "hard" split.
            qk_idx_t* idx0 = qk_part_get_idx0(part);
            qk_idx_t* idxE = idx0 + part->n_keys;
            assert(idxT >= idx0 && idxT <= idxE);
            bool left_empty = (idxT == idx0);
            bool right_empty = (idxT == idxE);
            qk_part_t* partL;
            qk_part_t **next_downR;
            if (right_empty || left_empty) {
                // No move required. Allocate new partition to insert on.
                DBGFN("new splitting partition ", part, " on level #", i_lvl);
                qk_part_t* new_part = qk_part_alloc_new(ctx, req_space);
                qk_part_insert_entry(i_lvl, new_part, 0, key, value, 0, &next_downR);
                if (!left_empty) {
                    // The current partition is the left partition.
                    // We must update its down pointer if we are splitting below.
                    partL = part;
                } else {
                    // We don't have a left partition or will create an empty one.
                    partL = 0;
                    if (!right_empty) {
                        // The current partition is a level root entry partition since left is empty.
                        // However, since right isn't we must allocate a new empty partition and attach
                        // to the root. See split scenario abstract above for more information.
                        assert(hdr->root[i_lvl] == part);
                        hdr->root[i_lvl] = qk_part_alloc_new(ctx, 0);
                    }
                }
                // We are now "on" the new partition.
                part = new_part;
            } else {
                // Standard "hard" split.
                // Calculate required space for new left and right partition.
                DBGFN("hard splitting partition ", part, " on level #", i_lvl);
                assert(idxT != 0);
                uint64_t spaceL = qk_space_range_level(i_lvl, idx0, idxT);
                uint64_t spaceR = req_space + qk_space_range_level(i_lvl, idxT, idxE);
                // Allocate new left + right partition.
                partL = qk_part_alloc_new(ctx, spaceL);
                qk_part_t* partR = qk_part_alloc_new(ctx, spaceR);
                // Copy all entries to the left over to the left partition.
                qk_part_insert_entry_range(i_lvl, partL, idx0, idxT);
                // First element we insert in right partition is the new entity.
                qk_part_insert_entry(i_lvl, partR, 0, key, value, 0, &next_downR);
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
            if (i_lvl > 0) {
                // The right down pointer is not yet written and the reference we received from insert.
                downR = next_downR;
                // The left down pointer reference is the last entity in the left partition we split.
                // When we don't have anything to the left we don't have a left partition or an empty one
                // and will have a level root entry partition below. We use a null downL to signal this.
                downL = (partL != 0? qk_idx1_get_down_ptr(qk_part_get_idx0(partL) + partL->n_keys - 1): 0);
            }
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
    // Update tuning statistics.
    hdr->total_count++;
    hdr->total_level0_use += req_space;
    // Insert complete.
    return true;
}

qk_ctx_t* qk_open(acid_h* ah, qk_opt_t* opt) {
    // Create context.
    fstr_t am = acid_memory(ah);
    qk_ctx_t new_ctx = {
        .hdr = (void*) am.str,
        .ah = ah,
    };
    qk_ctx_t* ctx = cln(&new_ctx);
    // Read quark header.
    qk_hdr_t* hdr = ctx->hdr;
    bool tune_target_ipp;
    if (hdr->magic == 0) {
        // This is a new database, initialize it.
        memset(hdr, 0, sizeof(*hdr));
        hdr->magic = QK_HEADER_MAGIC;
        hdr->version = QK_VERSION;
        // Allocate root entry partitions for all levels.
        for (uint8_t i_lvl = 0; i_lvl < LENGTHOF(hdr->root); i_lvl++) {
            hdr->root[i_lvl] = qk_part_alloc_new(ctx, 0);
        }
        tune_target_ipp = true;
    } else if (hdr->magic == QK_HEADER_MAGIC) {
        // We are opening an existing database.
        if (hdr->version != QK_VERSION) {
            throw("bad database version", exception_io);
        }
        tune_target_ipp = opt->overwrite_target_ipp;
    } else {
        // This is not a valid database.
        throw("corrupt or invalid database", exception_io);
    }
    // Tune target partition size if requested.
    if (tune_target_ipp) {
        hdr->target_ipp = ((opt->target_ipp != 0)? opt->target_ipp: QK_DEFAULT_TARGET_IPP);
    }
    // Write deterministic seed setting.
    hdr->dtrm_seed = opt->dtrm_seed;
    // Increment session and complete first fsync to fail here if write does not work.
    hdr->session++;
    acid_fsync(ah);
    return ctx;
}
