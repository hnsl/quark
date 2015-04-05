#include "rcd.h"
#include "linux.h"
#include "acid.h"
#include "../src/quark-internal.h"
#include "ifc.h"

#pragma librcd

list(fstr_t)* vis_snaps;
lwt_heap_t* vis_heap;

static void vis_init() {
    vis_heap = lwt_alloc_heap();
    switch_heap (vis_heap) {
        vis_snaps = new_list(fstr_t);
    }
}

static void vis_render(qk_ctx_t* qk) {
    switch_heap(vis_heap) {
        void qk_vis_render(qk_ctx_t* ctx, rio_t* out_h, list(fstr_t)* states);
        rio_t* fh = rio_file_open("qk-vis-dump.html", false, true);
        rio_file_truncate(fh, 0);
        qk_vis_render(qk, fh, vis_snaps);
    }
    lwt_alloc_free(vis_heap);
    vis_init();
    DBGFN("rendered snapshots");
}

static void vis_snapshot(qk_ctx_t* ctx) { switch_heap(vis_heap) {
    fstr_mem_t* qk_vis_dump_graph(qk_ctx_t* ctx);
    list_push_end(vis_snaps, fstr_t, fss(qk_vis_dump_graph(ctx)));
    DBGFN("taking snapshot #", list_count(vis_snaps, fstr_t));
}}

static void test_open_new_qk(qk_ctx_t** out_qk, acid_h** out_ah) {
    fstr_t data_path = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".data");
    fstr_t journal_path = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".jrnl");
    acid_h* ah = acid_open(data_path, journal_path, ACID_ADDR_0, 0);
    qk_opt_t opt = {
        // Allow test to be deterministic.
        .dtrm_seed = 1,
        // Very low target ipp for testing.
        .target_ipp = 4,
    };
    qk_ctx_t* qk = qk_open(ah, &opt);
    *out_qk = qk;
    *out_ah = ah;
}

static void test0() { sub_heap {
    qk_ctx_t* qk;
    acid_h* ah;
    test_open_new_qk(&qk, &ah);
    //vis_snapshot(qk);
    qk_insert(qk, "50", "fifty");
    qk_insert(qk, "25", "twentyfive");
    qk_insert(qk, "75", "seventyfive");
    qk_insert(qk, "30", "thirty");
    qk_insert(qk, "60", "sixty");
    qk_insert(qk, "90", "ninety");
    qk_insert(qk, "70", "seventy");
    qk_insert(qk, "80", "eighty");
    qk_insert(qk, "10", "ten");
    qk_insert(qk, "20", "twenty");
    //vis_snapshot(qk);
    qk_insert(qk, "51", "fiftyone");
    qk_insert(qk, "26", "twentysix");
    qk_insert(qk, "76", "seventysix");
    qk_insert(qk, "31", "thirtyone");
    qk_insert(qk, "61", "sixtyone");
    qk_insert(qk, "91", "ninetyone");
    qk_insert(qk, "71", "seventyone");
    qk_insert(qk, "81", "eightyone");
    qk_insert(qk, "11", "eleven");
    qk_insert(qk, "21", "twentyone");
    //vis_snapshot(qk);
    //vis_render(qk);
    acid_fsync(ah);
    acid_close(ah);
}}


static void test1() { sub_heap {
    qk_ctx_t* qk;
    acid_h* ah;
    test_open_new_qk(&qk, &ah);
    vis_snapshot(qk);
    size_t n = 0;
    extern fstr_t capitals;
    fstr_t tail = capitals;
    for (fstr_t row; fstr_iterate_trim(&tail, "\n", &row);) {
        fstr_t country, capital;
        if (!fstr_divide(row, ",", &country, &capital))
            continue;
        qk_insert(qk, capital, country);
        n++;
        if ((n % 40) == 0) {
            vis_snapshot(qk);
        }
    }
    vis_snapshot(qk);
    vis_render(qk);
    acid_fsync(ah);
    acid_close(ah);

}}

void rcd_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    vis_init();
    test0();
    test1();
    rio_debug("tests done");
    exit(0);
}
