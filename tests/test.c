#include "rcd.h"
#include "linux.h"
#include "acid.h"
#include "../src/quark-internal.h"
#include "ifc.h"

#pragma librcd

static void qk_vis_dump(qk_ctx_t* qk) { sub_heap {
    void qk_vis(qk_ctx_t* ctx, rio_t* out_h);
    rio_t* fh = rio_file_open("qk-vis-dump.html", false, true);
    rio_file_truncate(fh, 0);
    qk_vis(qk, fh);
}}

static void test0_alloc(fstr_t data_path, fstr_t journal_path) { sub_heap {
    acid_h* ah = acid_open(data_path, journal_path, ACID_ADDR_0, 0);
    qk_opt_t opt = {0};
    qk_ctx_t* qk = qk_open(ah, &opt);
    qk_vis_dump(qk);
}}

void rcd_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    fstr_t data_path = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".data");
    fstr_t journal_path = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".jrnl");
    test0_alloc(data_path, journal_path);
    exit(0);
}
