#include "rcd.h"
#include "musl.h"
#include "acid.h"
#include "quark.h"

#define QK_HEADER_MAGIC 0x6aef91b6b454b73f
#define QK_VERSION 4

/// Smallest possible logical allocation size: 8 2e = 2^8 = 256b.
#define QK_VM_ATOM_2E 8

/// Smallest possible physical allocation size: 12 2e = 2^12 = 4kb. (one page)
#define QK_VM_PAGE_2E 12

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

typedef struct qk_lvl_stats {
    /// Number of entries.
    uint64_t ent_count;
    /// Number of partitions.
    uint64_t part_count;
    /// Total bytes allocated for partitions.
    uint64_t total_alloc_b;
    /// Bytes allocated for data use (keys, values) but not indexes.
    uint64_t data_alloc_b;
    /// Bytes allocated for index use is sizeof(qk_idx_t) * ent_count.
} qk_lvl_stats_t;

typedef struct qk_stats {
    // Level statistics.
    qk_lvl_stats_t lvl[8];
    // Tracked global partition size class count.
    // Useful to understand disk cache efficiency and to tune ipp.
    uint64_t part_class_count[48];
} qk_stats_t;

/// Global quark header.
typedef struct qk_hdr {
    /// Magic number. Used to verify if the db is initialized.
    uint64_t magic;
    /// Version of the db. Should be QK_VERSION.
    uint64_t version;
    /// Session of the db. Initialized with 1 and incremented on every open.
    uint64_t session;
    /// AVL-tree of maps (qk_map_hdr_t) in the quark database.
    avltree_t maps;
} qk_hdr_t;

/// Map quark header.
typedef struct qk_map {
    /// Name of the map. String is allocated at the end of the header.
    fstr_t name;
    avltree_node_t node;
    /// Access session of the map. Set to global session every open.
    uint64_t asession;
    /// TODO: When this is non-zero, don't use variable key length. Should be 0 for now.
    uint64_t static_key_size;
    /// Deterministic seed. When this parameter is non-zero the key is hashed with this
    /// seed to determine the entity height instead of using non-deterministic randomness.
    uint64_t dtrm_seed;
    /// Tuning parameter: the target entries per partition.
    /// Controls level probability, partition size and total capacity.
    /// Can be set freely on open if requested.
    uint16_t target_ipp;
    /// B-Skip-List root, an entry pointer for each level.
    qk_part_t* root[8];
    /// End free list size class. The first free list class that is larger than what has
    /// ever been allocated. The free list vector is zero at and above this point.
    uint8_t free_end_class;
    /// Memory allocator free list. The smallest is 2^QK_VM_ATOM_2E bytes and gets
    /// twice as large for each size class.
    void* free_list[48];
    /// Statistics.
    qk_stats_t stats;
} qk_map_t;

CASSERT(sizeof(qk_hdr_t) <= PAGE_SIZE);

struct qk_ctx {
    acid_h* ah;
    qk_hdr_t* hdr;
};

struct qk_map_ctx {
    qk_ctx_t* ctx;
    qk_map_t* map;
    /// The expected entry capacity of the b-skip-list. Calculated from target_ipp.
    uint128_t entry_cap;
};

noret void qk_throw_sanity_error(fstr_t file, int64_t line);

static inline void qk_santiy_check(bool check, fstr_t file, int64_t line) {
    if (!check) {
        qk_throw_sanity_error(file, line);
    }
}

uint8_t qk_value_to_2e(uint64_t value, bool round_up);

static inline uint8_t qk_bytes_to_atoms_2e(size_t bytes, bool round_up) {
    if (bytes <= (1UL << QK_VM_ATOM_2E))
        return 0;
    return qk_value_to_2e(bytes, round_up) - QK_VM_ATOM_2E;
}

static inline size_t qk_atoms_2e_to_bytes(uint8_t atom_2e) {
    return (1UL << (atom_2e + QK_VM_ATOM_2E));
}

/// New raw non-freeable static allocation from expanding the acid memory.
void* qk_vm_mmap_raw(qk_ctx_t* ctx, size_t size);

void* qk_vm_alloc(qk_map_ctx_t* mctx, uint64_t bytes, uint64_t* out_bytes, uint8_t* out_atom_2e);
void qk_vm_free(qk_map_ctx_t* mctx, void* ptr, uint64_t bytes, uint8_t* out_atom_2e);

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
static inline void* qk_part_get_write0(qk_part_t* part) {
    return ((void*) part) + part->total_size - part->data_size;
}
