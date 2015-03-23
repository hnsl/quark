#ifndef QUARK_H
#define QUARK_H

#include "rcd.h"

typedef struct qk_ctx qk_ctx_t;

decl_fid_t(quark);

define_eio(quark);

sf(quark)* qk_init(acid_h* ah);


#endif
