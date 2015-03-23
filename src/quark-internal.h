#ifndef QUARK_I_H
#define QUARK_I_H

#include "rcd.h"
#include "quark.h"

#pragma librcd


join_locked_declare(void) qk_free(void* ptr, size_t bytes ,join_server_params, qk_ctx_t* ctx);
join_locked_declare(void*) qk_alloc(size_t bytes ,join_server_params, qk_ctx_t* ctx);
join_locked_declare(void) qk_dprint_free_memory(join_server_params, qk_ctx_t* ctx);




#endif
