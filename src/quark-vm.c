/* Copyright © 2014, Jumpstarter AB.
 * Quark memory allocator. */

#include "quark-internal.h"

#pragma librcd

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

uint8_t qk_value_to_2e(uint64_t value, bool round_up) {
    if ((value & ~1UL) == 0)
        return 0;
    return qk_log2(round_up? (value - 1) << 1: value);
}

static inline uint8_t qk_bytes_to_pages_2e(size_t bytes, bool round_up) {
    if (bytes <= (1UL << QK_VM_ATOM_2E))
        return 0;
    return qk_value_to_2e(bytes, round_up) - QK_VM_ATOM_2E;
}

static inline size_t qk_pages_2e_to_bytes(uint8_t pages_2e) {
    return (1UL << (pages_2e + QK_VM_ATOM_2E));
}

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
            while (i_2e > pages_2e) {
                i_2e--;
                *((void**) block) = hdr->free_list[i_2e];
                hdr->free_list[i_2e] = block;
                block_len /= 2;
                block += block_len;
            }
            return block;
        }
    }
}

void* qk_vm_alloc(qk_ctx_t* ctx, uint64_t bytes, uint64_t* out_bytes) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    if (out_bytes != 0)
        *out_bytes = qk_pages_2e_to_bytes(pages_2e);
    return qk_vm_pop(ctx, pages_2e);
}

void qk_vm_free(qk_ctx_t* ctx, void* ptr, uint64_t bytes) {
    uint8_t pages_2e = qk_bytes_to_pages_2e(bytes, true);
    qk_vm_push(ctx, ptr, pages_2e);
}
