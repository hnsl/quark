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
    const static uint8_t de_bruijn_table[64] = {
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

/// Free memory of the specified size class.
static void qk_vm_push(qk_ctx_t* ctx, void* block, uint8_t atom_2e) {
    qk_hdr_t* hdr = ctx->hdr;
    assert(atom_2e < hdr->free_end_class);
    *((void**) block) = hdr->free_list[atom_2e];
    hdr->free_list[atom_2e] = block;
}

/// Allocate memory of specified size class.
static void* qk_vm_pop(qk_ctx_t* ctx, uint8_t atom_2e) {
    qk_hdr_t* hdr = ctx->hdr;
    for (uint8_t i_2e = atom_2e;; i_2e++) {
        void* block;
        size_t block_len;
        if (i_2e >= hdr->free_end_class) {
            // Out of memory, need to reserve more.
            if (i_2e >= LENGTHOF(hdr->free_list))
                throw(concs("allocation unsupported, size too great [", i_2e, "]"), exception_fatal);
            // Cannot reserve less than one page of memory from acid.
            i_2e = MAX(i_2e, (QK_VM_PAGE_2E - QK_VM_ATOM_2E));
            // Allocate the block by expanding acid memory.
            hdr->free_end_class = i_2e + 1;
            block_len = qk_atoms_2e_to_bytes(i_2e);
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
            block_len = qk_atoms_2e_to_bytes(i_2e);
            has_rblock:;
            while (i_2e > atom_2e) {
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

void* qk_vm_alloc(qk_ctx_t* ctx, uint64_t bytes, uint64_t* out_bytes, uint8_t* out_atom_2e) {
    uint8_t atom_2e = qk_bytes_to_atoms_2e(bytes, true);
    if (out_bytes != 0)
        *out_bytes = qk_atoms_2e_to_bytes(atom_2e);
    if (out_atom_2e != 0)
        *out_atom_2e = atom_2e;
    return qk_vm_pop(ctx, atom_2e);
}

void qk_vm_free(qk_ctx_t* ctx, void* ptr, uint64_t bytes, uint8_t* out_atom_2e) {
    uint8_t atom_2e = qk_bytes_to_atoms_2e(bytes, true);
    if (out_atom_2e != 0)
        *out_atom_2e = atom_2e;
    qk_vm_push(ctx, ptr, atom_2e);
}
