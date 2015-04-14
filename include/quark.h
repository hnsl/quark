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

/// Options for quark.
typedef struct qk_opt {
    /// Set to true to always overwrite database target ipp.
    /// Set to false to only set target ipp on db init.
    bool overwrite_target_ipp;
    /// Tuning parameter: Target items per partition. Set to 0 to use the default.
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

/// Updates a single value in the quark database with the specified key.
/// Returns false if the key does not exist.
/// This function is optimized to mutate values, not remove them or to save space
/// by shrinking them. Quark does currently not support removing key/value pairs
/// or freeing already reserved space.
bool qk_update(qk_ctx_t* ctx, fstr_t key, fstr_t new_value);

/// Fetches a single value from the quark database from the specified key.
/// Returns false if the key does not exist.
bool qk_get(qk_ctx_t* ctx, fstr_t key, fstr_t* out_value);

/// Inserts a key/value pair into quark database.
/// Will not attempt fsync or snapshot, caller is responsible for this.
/// This function must be synchronized. Attempting to sync the database before the
/// function is complete or calling insert in parallel will corrupt the database.
bool qk_insert(qk_ctx_t* ctx, fstr_t key, fstr_t value);

/// Returns statistics for the open database.
json_value_t qk_get_stats(qk_ctx_t* ctx);

/// Opens a quark database.
/// To close the quark database just fsync the acid handle as required and free the context.
qk_ctx_t* qk_open(acid_h* ah, qk_opt_t* opt);

/// Compiles a multi-dimensional quark key. The returned key has the property that it's a
/// single string but each individual part will be separated so each part is considered
/// first in sequence when keys are lexicographically compared.
fstr_mem_t* qk_compile_key(uint16_t n_parts, fstr_t* parts);

#endif
