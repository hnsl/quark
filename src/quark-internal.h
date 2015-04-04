#include "rcd.h"
#include "musl.h"
#include "acid.h"
#include "quark.h"

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
    /// Tuning statistics: Total number of items in index.
    uint64_t total_count;
    /// Tuning statistics: Total size used by all items in index, including overhead in level 0.
    uint64_t total_level0_use;
    /// Tuning parameter: the target items per partition.
    /// Can be set freely on open if requested.
    uint16_t target_ipp;
    /// Deterministic seed. When this parameter is non-zero the key is hashed with this
    /// seed to determine the entity height instead of using non-deterministic randomness.
    uint64_t dtrm_seed;
    /// Memory allocator free list. The smallest is 4k and gets
    /// twice as large for each size class.
    void* free_list[32];
    /// End free list size class. The first free list class that is larger than what has
    /// ever been allocated. The free list vector is zero at and above this point.
    uint8_t free_end_class;
    /// B-Skip-List root, an entry pointer for each level.
    qk_part_t* root[8];
} qk_hdr_t;

CASSERT(sizeof(qk_hdr_t) <= PAGE_SIZE);

struct qk_ctx {
    acid_h* ah;
    qk_hdr_t* hdr;
};

uint8_t qk_value_to_2e(uint64_t value, bool round_up);
void* qk_vm_alloc(qk_ctx_t* ctx, uint64_t bytes, uint64_t* out_bytes);
void qk_vm_free(qk_ctx_t* ctx, void* ptr, uint64_t bytes);

noret void qk_throw_sanity_error(fstr_t file, int64_t line);

static inline void qk_santiy_check(bool check, fstr_t file, int64_t line) {
    if (!check) {
        qk_throw_sanity_error(file, line);
    }
}

/// Takes an index and resolves the key.
static inline fstr_t qk_idx_get_key(qk_idx_t* idx) {
    fstr_t key = {
        .str = idx->keyptr,
        .len = idx->keylen,
    };
    return key;
}

/// Takes an lvl0 index and resolves the value.
static inline fstr_t qk_idx0_get_value(qk_idx_t* idx) {
    uint64_t* valuelen_ptr = (void*) (idx->keyptr + idx->keylen);
    uint8_t* valuestr = (void*) (valuelen_ptr + 1);
    fstr_t value = {
        .str = valuestr,
        .len = *valuelen_ptr,
    };
    return value;
}

/// Takes an lvl1+ index and resolves the down pointer reference.
static inline qk_part_t** qk_idx1_get_down_ptr(qk_idx_t* idx) {
    return (void*) (idx->keyptr + idx->keylen);
}

/// Returns the first index entity (at offset 0) in a partition.
static inline qk_idx_t* qk_part_get_idx0(qk_part_t* part) {
    return (void*) part + sizeof(*part);
}

/// Returns the start write pointer (at first allocated byte) in partition tail.
static inline qk_idx_t* qk_part_get_write0(qk_part_t* part) {
    return ((void*) part) + part->total_size - part->data_size;
}
