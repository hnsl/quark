#ifndef QUARK_H
#define QUARK_H

#include "rcd.h"

/// Maximum supported key length by quark.
/// Large keys is *not* recommended. Keys smaller than 64 bytes is recommended.
#define QUARK_MAX_KEY_LEN UINT16_MAX

/// Maximum supported value length by quark.
/// Large values is *not* recommended. Quark is designed for fast table scans
/// and large values will mess with the built-in tuning, significantly lowering
/// the
#define QUARK_MAX_VALUE_LEN UINT16_MAX

typedef struct qk_opt {
    /// Set to true to always overwrite database target ipp.
    /// Set to false to only set target ipp on db init.
    bool overwrite_target_ipp;
    /// Tuning parameter: Target items per partition. Set to 0 to use the default.
    /// The database will auto tune internal probabilities based on inserted data
    /// to attempt to approach this value.
    uint16_t target_ipp;
} qk_opt_t;

decl_fid_t(quark);

define_eio(quark);

sf(quark)* qk_init(acid_h* ah, qk_opt_t* opt);

void qk_push(sf(quark)* sf, fstr_t key, fstr_t value);
list(fstr_t)* qk_slice(sf(quark)* sf,fstr_t from_key, fstr_t to_key);

#endif
