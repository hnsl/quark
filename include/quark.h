#ifndef QUARK_H
#define QUARK_H

#include "rcd.h"

decl_fid_t(quark);

define_eio(quark);

sf(quark)* qk_init(acid_h* ah);

void qk_push(sf(quark)* sf, fstr_t key, fstr_t value);
list(fstr_t)* qk_slice(sf(quark)* sf,fstr_t from_key, fstr_t to_key);

#endif
