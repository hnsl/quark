#ifndef QUARK_H
#define QUARK_H

/// Maximum supported key length by quark.
/// Large keys is *not* recommended. Keys smaller than 64 bytes is recommended.
#define QUARK_MAX_KEY_LEN UINT16_MAX

/// Maximum supported value length by quark.
/// Large values is *not* recommended. Quark is designed for fast table scans
/// and large values will mess with the built-in tuning, significantly lowering
/// the performance.
#define QUARK_MAX_VALUE_LEN UINT16_MAX

#define QUARK_KEY_COMPILE(...) ({ \
    fstr_t keys[] = {__VA_ARGS__}; \
    qk_compile_key(LENGTHOF(keys), keys); \
})

#define QUARK_KEY_DECOMPILE(raw_key, ...) ({ \
    fstr_t* key_ptrs[] = {__VA_ARGS__}; \
    fstr_t keys[LENGTHOF(key_ptrs)]; \
    qk_decompile_key(raw_key, LENGTHOF(keys), keys); \
    for (size_t i = 0; i < LENGTHOF(keys); i++) \
        *key_ptrs[i] = keys[i]; \
})

#define _QUARK_SCAN(INIT_OP, KEY_NAME, VALUE_NAME, BAND_DECL_E, BAND_FREE_E, SCAN_E, BAND_REF_E) \
    LET(qk_scan_op_t _op = INIT_OP) \
    BAND_DECL_E \
    LET(fstr_mem_t* _cur_start_key = 0) \
    LET(uint64_t _count = 0, _total = 0, _limit = _op.limit) \
    LET(fstr_t _last_key = {0}) \
    LET(bool _eof = false) \
    while ( ({ \
        if (_last_key.str != 0) { \
            /* copy start key from band to scan state */ \
            lwt_alloc_free(_cur_start_key); \
            _cur_start_key = fstr_cpy(_last_key); \
            _last_key.str = 0; \
            /* update operation to use new start key */ \
            _op.key_start = fss(_cur_start_key); \
            _op.with_start = true; \
            _op.inc_start = false; \
        } \
        BAND_FREE_E; \
        /* adjust limit. */ \
        bool _count_end = false; \
        if (_limit > 0) { \
            if (_total >= _limit) { \
                _count_end = true; \
            } else { \
                _op.limit = _limit - _total; \
            } \
        } \
        if (_count_end || _eof) { \
            /* full scan complete. */ \
            lwt_alloc_free(_cur_start_key); \
            break; \
        } \
        SCAN_E; \
    }), true) \
    LET(fstr_t _band_io = BAND_REF_E, KEY_NAME, VALUE_NAME) \
    while ( ({ \
        if (!qk_band_read(&_band_io, &KEY_NAME, &VALUE_NAME)) { \
            /* segment scan complete. */ \
            break; \
        } \
        _total++; \
        _last_key = KEY_NAME; \
    }), true)

#define QUARK_SCAN(MCTX_EXPR, INIT_OP, KEY_NAME, VALUE_NAME, SCAN_BUF) \
    _QUARK_SCAN(INIT_OP, KEY_NAME, VALUE_NAME, \
        LET(fstr_t _band), \
        ((void) 0), \
        (_band = (SCAN_BUF), _count = qk_scan(MCTX_EXPR, _op, &_band, &_eof)), \
        _band \
    )

/// Options for quark.
typedef struct qk_opt {
    /// Tuning parameter: Target items per partition. Set to 0 to use the default
    /// or to use the existing value.
    /// The database will auto tune internal probabilities based on inserted data
    /// to attempt to approach this value.
    uint16_t target_ipp;
    /// Deterministic seed. When this parameter is non-zero the key is hashed with this
    /// seed to determine the entity height instead of using non-deterministic randomness.
    /// Useful when writing deterministic tests.
    uint64_t dtrm_seed;
} qk_opt_t;

/// Quark context.
typedef struct qk_ctx qk_ctx_t;

/// Quark map. Opened from a quark context.
typedef struct qk_map_ctx qk_map_ctx_t;

/// Fetches a single value from the quark database from the specified key.
/// Returns false if the key does not exist.
bool qk_get(qk_map_ctx_t* mctx, fstr_t key, fstr_t* out_value);

typedef struct qk_scan_op {
    /// Start scan operation at this key.
    fstr_t key_start;
    /// End scan operation at this key.
    fstr_t key_end;
    /// Default scan (0) has no limit.
    /// Set to non-zero to stop scan after this many entries read.
    size_t limit;
    /// Default scan is ascending. Set to true to scan in descending order.
    bool descending;
    /// Default scan ignores start key and starts on beginning of index
    /// if ascending or end if descending.
    /// Set to true to start scan at start key.
    bool with_start;
    /// Default scan ignores end key. Set to true to stop scan at end key.
    bool with_end;
    /// Default scan does not include entry matching start key.
    /// Set to true to include start key match.
    bool inc_start;
    /// Default scan does not include entry matching end key.
    /// Set to true to include end key match.
    bool inc_end;
    /// Default scan includes data.
    /// Set to true to only copy keys to band and return empty data values.
    bool ignore_data;
} qk_scan_op_t;

/// Reads out the next key/value pair from a band scanned by qk_scan() and
/// seeks to the next pair. Returns false when reached end of band.
bool qk_band_read(fstr_t* io_mem, fstr_t* out_key, fstr_t* out_value);

/// Scan values out of the quark database with maximal efficiency
/// (no random access), copying over key/values to a "band" with undefined
/// format. The band must be parsed with qk_band_next().
/// Memory to scan to should be passed via io_mem and will be cut off where
/// the scan terminates. When the memory runs out during scan the function
/// will return prematurely with a lower count than requested.
/// The "out_eof" parameter is set to false if the scan is terminated early
/// because the io_mem (band) runs out. In other cases the operation is
/// completed, the end is reached and eof is set.
/// The configuration for the scan is passed via "op".
/// The function returns the number of key/value pairs copied to the band.
uint64_t qk_scan(qk_map_ctx_t* mctx, qk_scan_op_t op, fstr_t* io_mem, bool* out_eof);

/// Updates a single value in the quark database with the specified key.
/// Returns false if the key does not exist.
/// This function is optimized to mutate values, not remove them or to save space
/// by shrinking them. Quark does currently not support removing key/value pairs
/// or freeing already reserved space.
bool qk_update(qk_map_ctx_t* mctx, fstr_t key, fstr_t new_value);

/// Inserts a key/value pair into quark database.
/// Will not attempt fsync or snapshot, caller is responsible for this.
/// This function must be synchronized. Attempting to sync the database before the
/// function is complete or calling insert in parallel will corrupt the database.
/// The function returns true if the key did not exist and was inserted, otherwise false.
bool qk_insert(qk_map_ctx_t* mctx, fstr_t key, fstr_t value);

/// Upserts a key/value pair into a quark database.
/// Exactly like qk_insert() but operation falls back into optimized update if the key is found.
/// The function returns true if the key did not exist and was inserted, otherwise false
/// if the key was found and updated.
bool qk_upsert(qk_map_ctx_t* mctx, fstr_t key, fstr_t value);

/// Deletes a key/value pair from the quark database.
/// The function returns true if the key existed and was removed, otherwise false.
bool qk_delete(qk_map_ctx_t* mctx, fstr_t key);

/// Returns statistics for the open database.
json_value_t qk_get_stats(qk_map_ctx_t* mctx);

/// Opens a quark database.
/// To close the quark database just fsync the acid handle as required and free the context.
qk_ctx_t* qk_open(acid_h* ah);

/// Opens a quark map.
qk_map_ctx_t* qk_open_map(qk_ctx_t* ctx, fstr_t name, qk_opt_t* opt);

/// Compiles a multi-dimensional quark key. The returned key has the property that it's a
/// single string but each individual part will be separated so each part is considered
/// first in sequence when keys are lexicographically compared.
fstr_mem_t* qk_compile_key(uint16_t n_parts, fstr_t* parts);

/// Decompiles a key in-place which was compiled with qk_compile_key()
/// and writes their respective parts to the given parts vector.
/// When key have more or less parts than n_parts it throws an io exception.
/// When key otherwise has invalid format the function throws an io exception as well.
/// If an io exception is thrown the raw key could have been modified and has undefined content.
fstr_mem_t* qk_decompile_key(fstr_t raw_key, size_t n_parts, fstr_t* out_parts);

/// Counts the number of parts in a raw key.
static inline size_t qk_key_count_parts(fstr_t raw_key) {
    bool in_null = false;
    size_t count = 1;
    for (size_t i = 0; i < raw_key.len; i++) {
        if (raw_key.str[i] == 0) {
            if (in_null) {
                count++;
            }
            in_null = !in_null;
        } else {
            in_null = false;
        }
    }
    return count;
}

#endif
