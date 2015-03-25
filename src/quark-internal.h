#ifndef QUARK_I_H
#define QUARK_I_H

#include "rcd.h"
#include "quark.h"

#pragma librcd

typedef struct qk_ctx qk_ctx_t;

join_locked_declare(void) qk_free(void* ptr, size_t bytes ,join_server_params, qk_ctx_t* ctx);
join_locked_declare(void*) qk_alloc(size_t bytes ,join_server_params, qk_ctx_t* ctx);
join_locked_declare(void) qk_dprint_free_memory(join_server_params, qk_ctx_t* ctx);
void qk_bls_print(sf(quark)* sf);



#endif
