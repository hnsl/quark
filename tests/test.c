#include "rcd.h"
#include "linux.h"
#include "hmap.h"
#include "acid.h"
#include "../src/quark-internal.h"
#include "ifc.h"

#pragma librcd

list(fstr_t)* vis_snaps;
lwt_heap_t* vis_heap;

static uint64_t test_hash64(void* key, uint64_t len, uint64_t seed) {
    return hmap_murmurhash_64a(key, len, seed);
}


static uint64_t test_hash64_2n(uint64_t x, uint64_t y) {
    return test_hash64(&x, sizeof(x), y);
}

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

static void print_stats(qk_ctx_t* qk) { sub_heap {
    DBGFN(fss(json_stringify_pretty(qk_get_stats(qk))));
}}

static fstr_t test_get_db_path() {
    return concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64());
}

static void test_open_new_qk(fstr_t db_path, qk_ctx_t** out_qk, acid_h** out_ah, qk_opt_t* set_opt) {
    fstr_t data_path = concs(db_path, ".data");
    fstr_t journal_path = concs(db_path, ".jrnl");
    acid_h* ah = acid_open(data_path, journal_path, ACID_ADDR_0, 0);
    qk_opt_t opt = {
        // Allow test to be deterministic.
        .dtrm_seed = 1,
        // Very low target ipp for testing.
        .target_ipp = 4,
    };
    if (set_opt != 0)
        opt = *set_opt;
    qk_ctx_t* qk = qk_open(ah, &opt);
    *out_qk = qk;
    *out_ah = ah;
}

static void test_rm_db(fstr_t db_path) { sub_heap {
    fstr_t data_path = concs(db_path, ".data");
    fstr_t journal_path = concs(db_path, ".jrnl");
    rio_file_unlink(data_path);
    rio_file_unlink(journal_path);
}}

static void test0() { sub_heap {
    rio_debug("running test0\n");
    qk_ctx_t* qk;
    acid_h* ah;
    fstr_t db_path = test_get_db_path();
    test_open_new_qk(db_path, &qk, &ah, 0);
    //x-dbg/ vis_snapshot(qk);
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
    //x-dbg/ vis_snapshot(qk);
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
    //x-dbg/ vis_snapshot(qk);
    //x-dbg/ vis_render(qk);
    acid_fsync(ah);
    acid_close(ah);
    test_rm_db(db_path);
}}

static void test01() { sub_heap {
    rio_debug("running test01\n");
    sub_heap {
        fstr_t key = fss(QUARK_KEY_COMPILE("a\x00\x01"));
        fstr_t p0, p1;
        QUARK_KEY_DECOMPILE(key, &p0);
        atest(fstr_equal(p0, "a\x00\x01"));
        try {
            QUARK_KEY_DECOMPILE(fsc(key), &p0, &p1);
            atest(false);
        } catch (exception_io, e);
    }
    sub_heap {
        fstr_t key = fss(QUARK_KEY_COMPILE("a\x00\x01", "b\x00\x01"));
        DBGFN(fss(fstr_ace_encode(key)));
        fstr_t p0, p1, p2;
        try {
            QUARK_KEY_DECOMPILE(fsc(key), &p0);
            atest(false);
        } catch (exception_io, e);
        QUARK_KEY_DECOMPILE(key, &p0, &p1);
        atest(fstr_equal(p0, "a\x00\x01"));
        atest(fstr_equal(p1, "b\x00\x01"));
        try {
            QUARK_KEY_DECOMPILE(fsc(key), &p0, &p1, &p2);
            atest(false);
        } catch (exception_io, e);
    }
    sub_heap {
        fstr_t key = fss(QUARK_KEY_COMPILE("a\x00\x01", "", "b\x00\x01"));
        fstr_t p0, p1, p2, p3;
        try {
            QUARK_KEY_DECOMPILE(fsc(key), &p0, &p1);
            atest(false);
        } catch (exception_io, e);
        QUARK_KEY_DECOMPILE(key, &p0, &p1, &p2);
        atest(fstr_equal(p0, "a\x00\x01"));
        atest(fstr_equal(p1, ""));
        atest(fstr_equal(p2, "b\x00\x01"));
        try {
            QUARK_KEY_DECOMPILE(fsc(key), &p0, &p1, &p2, &p3);
            atest(false);
        } catch (exception_io, e);
    }
}}

static void test1() { sub_heap {
    rio_debug("running test1\n");
    qk_ctx_t* qk;
    acid_h* ah;
    fstr_t db_path = test_get_db_path();
    test_open_new_qk(db_path, &qk, &ah, 0);
    //x-dbg/ vis_snapshot(qk);
    //x-dbg/ print_stats(qk);
    size_t total = 0;
    extern fstr_t capitals;
    bool found_andorra = false;
    rio_debug("test1: standard insert\n");
    for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) {
        fstr_t country, capital;
        if (!fstr_divide(row, ",", &country, &capital))
            continue;

        fstr_t value;
        atest(!qk_get(qk, capital, &value));
        atest(qk_insert(qk, capital, country));
        total++;
        atest(qk_get(qk, capital, &value));

        atest(fstr_equal(value, country));
        if (fstr_equal(capital, "Andorra la Vella")) {
            found_andorra = true;
        }
        if (found_andorra) {
            atest(qk_get(qk, "Andorra la Vella", &value));
            atest(fstr_equal(value, "Andorra"));
        } else {
            atest(!qk_get(qk, "Andorra la Vella", &value));
        }
    }
    //x-dbg/ vis_snapshot(qk);
    //x-dbg/ vis_render(qk);
    //x-dbg/ print_stats(qk);
    rio_debug("test1: standard read\n");
    for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) { sub_heap {
        fstr_t country, capital;
        if (!fstr_divide(row, ",", &country, &capital))
            continue;
        fstr_t value;
        //x-dbg/ rio_debug(concs("looking up [", capital, "]"));
        atest(qk_get(qk, capital, &value));
        atest(fstr_equal(value, country));
        atest(!qk_get(qk, concs(capital, "\x00"), &value));
    }}
    // Test scan.
    rio_debug("test1: scan default\n");
    fstr_t keys[total];
    fstr_t values[total];
    {
        // A default scan should read out all entries in ascending order.
        qk_scan_op_t op = {0};
        bool eof = false;
        fstr_t g_scan_mem = fss(fstr_alloc(100 * PAGE_SIZE));
        fstr_t scan_mem = g_scan_mem;
        size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
        atest(scan_n == total);
        atest(eof);
        fstr_t key, prev_key, value;
        for (size_t i = 0; i < total; i++) {
            atest(qk_band_read(&scan_mem, &key, &value));
            //x-dbg/ DBGFN("scan [", i, "] [", key, "] => [", value, "]");
            keys[i] = key;
            values[i] = value;
            if (i > 0) {
                atest(fstr_cmp_lexical(key, prev_key) > 0);
            }
            prev_key = key;
        }
        atest(!qk_band_read(&scan_mem, &key, &value));
    }
    fstr_t g_scan_mem2 = fss(fstr_alloc(100 * PAGE_SIZE));
    rio_debug("test1: scan reverse\n");
    {
        // Reverse/descending scan.
        qk_scan_op_t op = {
            .descending = true
        };
        bool eof = false;
        fstr_t scan_mem = g_scan_mem2;
        size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
        atest(scan_n == total);
        atest(eof);
        fstr_t key, value;
        for (size_t i = 0; i < total; i++) {
            atest(qk_band_read(&scan_mem, &key, &value));
            size_t j = total - i - 1;
            //x-dbg/ DBGFN("rscan [", j, "] [", key, "] => [", value, "]");
            atest(fstr_equal(key, keys[j]));
            atest(fstr_equal(value, values[j]));
        }
        atest(!qk_band_read(&scan_mem, &key, &value));
    }
    rio_debug("test1: scan limited\n");
    {
        // Reverse/descending limited scan.
        for (size_t limit = 1; limit < 4; limit++) sub_heap {
            // First test limit with number.
            if (limit == 4)
                limit = total - 1;
            qk_scan_op_t op = {
                .descending = true,
                .limit = limit
            };
            fstr_t post_scan_mem;
            {
                bool eof = true;
                fstr_t scan_mem = g_scan_mem2;
                size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
                post_scan_mem = scan_mem;
                atest(scan_n == limit);
                atest(eof);
                fstr_t key, value;
                for (size_t i = 0; i < limit; i++) {
                    atest(qk_band_read(&scan_mem, &key, &value));
                    size_t j = total - i - 1;
                    //x-dbg/ DBGFN("rscan [", j, "] [", key, "] => [", value, "]");
                    atest(fstr_equal(key, keys[j]));
                    atest(fstr_equal(value, values[j]));
                }
                atest(!qk_band_read(&scan_mem, &key, &value));
            }
            // Second test limit with short band.
            {
                bool eof = true;
                op.limit = 0;
                fstr_t scan_mem = fss(fstr_alloc(post_scan_mem.len));
                size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
                atest(scan_n == limit);
                atest(!eof);
                fstr_t key, value;
                for (size_t i = 0; i < limit; i++) {
                    atest(qk_band_read(&scan_mem, &key, &value));
                    size_t j = total - i - 1;
                    //x-dbg/ DBGFN("rscan [", j, "] [", key, "] => [", value, "]");
                    atest(fstr_equal(key, keys[j]));
                    atest(fstr_equal(value, values[j]));
                }
                atest(!qk_band_read(&scan_mem, &key, &value));
            }
        }
    }
    rio_debug("test1: scan partials\n");
    {
        // Partial scans.
        for (size_t start = 0; start < total + 1; start++) {
            for (size_t end = start; end < total + 1; end++) {
                for (size_t dir = 0; dir < 2; dir++) {
                    for (size_t inc = 0; inc < 4; inc++) {
                        for (size_t part = 0; part < 4; part++) sub_heap {
                            //x-dbg/ DBGFN("partial scan operation start:[", start, "] end:[", end, "] dir:[", dir, "] inc:[", inc, "] part:[", part, "]");
                            qk_scan_op_t op = {
                                .key_start = start > 0? keys[start - 1]: "",
                                .key_end = end > 0? keys[end - 1]: "",
                                .with_start = (start != 0),
                                .with_end = (end != 0),
                                .inc_start = (inc == 1 || inc == 3),
                                .inc_end = (inc == 2 || inc == 3),
                            };
                            bool pm_start = false, pm_end = false;
                            if (part == 1 || part == 3) {
                                // Partial matching start key.
                                if (start == 0)
                                    continue;
                                op.key_start = concs(op.key_start, "!");
                                pm_start = true;
                            }
                            if (part == 2 || part == 3) {
                                // Partial end key.
                                if (end == 0)
                                    continue;
                                op.key_end = concs(op.key_end, "!");
                                pm_end = true;
                            }
                            if (dir == 1) {
                                op.descending = true;
                                FLIP(op.key_start, op.key_end);
                                FLIP(op.with_start, op.with_end);
                                FLIP(op.inc_start, op.inc_end);
                                FLIP(pm_start, pm_end);
                            }
                            bool eof = ((start % 2) == 0);
                            fstr_t scan_mem = g_scan_mem2;

                            /*
                            DBGFN("partial scan operation"
                                " key_start:[", (op.with_start? op.key_start: "n/a"), "]"
                                " key_end:[", (op.with_end? op.key_end: "n/a"), "]"
                                " inc_start:[", (op.inc_start? "yes": "no"), "]"
                                " inc_end:[", (op.inc_end? "yes": "no"), "]"
                                " order:[", (op.descending? "desc": "asc"), "]"
                                " limit:[", (op.limit > 0? STR(op.limit): "∞"), "]"
                            );
                            */
                            size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
                            atest(eof);

                            ssize_t start_i = start > 0? start - 1: 0;
                            ssize_t end_i = end > 0? end - 1: total - 1;
                            size_t read_n = 0;

                            fstr_t key, value;
                            if (op.descending) {
                                FLIP(start_i, end_i);
                                for (ssize_t i = start_i; i >= end_i; i--) {
                                    if (i == start_i) {
                                        if (op.with_start && (!op.inc_start && !pm_start))
                                            continue;
                                    }
                                    if (i == end_i) {
                                        if (op.with_end && (!op.inc_end || pm_end))
                                            continue;
                                    }
                                    // DBGFN("reading band, key #", i);
                                    if (!qk_band_read(&scan_mem, &key, &value)) {
                                        rio_debug(concs("unexpected end of scan band on key [", key, "] (#", i, ")\n"));
                                        atest(false);
                                    }
                                    read_n++;
                                    atest(fstr_equal(key, keys[i]));
                                    atest(fstr_equal(value, values[i]));
                                }
                                if (qk_band_read(&scan_mem, &key, &value)) {
                                    rio_debug(concs("unexpected continuation of scan band, got key [", key, "] (#", (end_i + 1), ")\n"));
                                    atest(false);
                                }
                            } else {
                                for (ssize_t i = start_i; i <= end_i; i++) {
                                    if (i == start_i) {
                                        if (op.with_start && (!op.inc_start || pm_start))
                                            continue;
                                    }
                                    if (i == end_i) {
                                        if (op.with_end && (!op.inc_end && !pm_end))
                                            continue;
                                    }
                                    // DBGFN("reading band, key #", i);
                                    if (!qk_band_read(&scan_mem, &key, &value)) {
                                        rio_debug(concs("unexpected end of scan band on key [", key, "] (#", i, ")\n"));
                                        atest(false);
                                    }
                                    read_n++;
                                    atest(fstr_equal(key, keys[i]));
                                    atest(fstr_equal(value, values[i]));
                                }
                                if (qk_band_read(&scan_mem, &key, &value)) {
                                    rio_debug(concs("unexpected continuation of scan band, got key [", key, "] (#", (end_i + 1), ")\n"));
                                    atest(false);
                                }
                            }
                            atest(scan_n == read_n);
                        }
                    }
                }
            }
        }
        qk_scan_op_t op = {
            .descending = true
        };
        bool eof = false;
        fstr_t g_scan_mem = fss(fstr_alloc(100 * PAGE_SIZE));
        fstr_t scan_mem = g_scan_mem;
        size_t scan_n = qk_scan(qk, op, &scan_mem, &eof);
        atest(scan_n == total);
        atest(eof);
        fstr_t key, value;
        for (size_t i = 0; i < total; i++) {
            atest(qk_band_read(&scan_mem, &key, &value));
            size_t j = total - i - 1;
            //x-dbg/ DBGFN("rscan [", j, "] [", key, "] => [", value, "]");
            atest(fstr_equal(keys[j], key));
            atest(fstr_equal(values[j], value));
        }
        atest(!qk_band_read(&scan_mem, &key, &value));
    }
    // Test update.
    rio_debug("test1: update\n");
    {
        size_t i = 0;
        for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) { sub_heap {
            fstr_t country, capital;
            if (!fstr_divide(row, ",", &country, &capital))
                continue;
            // Test update non-existing key.
            atest(!qk_update(qk, concs(capital, "\x00"), capital));
            if ((i % 3) == 0) {
                // Test value expand.
                fstr_t new_value = concs(capital, " of ", country);
                //x-dbg/ DBGFN("update [", capital, "] => [", new_value, "]");
                atest(qk_update(qk, capital, new_value));
                fstr_t value;
                atest(qk_get(qk, capital, &value));
                atest(fstr_equal(value, new_value));
            } else if ((i % 3) == 1) {
                // Test value shrink.
                fstr_t new_value = fstr_slice(capital, 0, 3);
                //x-dbg/ DBGFN("update [", capital, "] => [", new_value, "]");
                atest(qk_update(qk, capital, new_value));
                fstr_t value;
                atest(qk_get(qk, capital, &value));
                atest(fstr_equal(value, new_value));
            } else {
                // Test value replace.
                fstr_t new_value = fss(fstr_reverse(country));
                //x-dbg/ DBGFN("update [", capital, "] => [", new_value, "]");
                atest(qk_update(qk, capital, new_value));
                fstr_t value;
                atest(qk_get(qk, capital, &value));
                atest(fstr_equal(value, new_value));
            }
            // For each iteration, verify all key/value mappings.
            {
                size_t j = 0;
                for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) { sub_heap {
                    fstr_t country, capital;
                    if (!fstr_divide(row, ",", &country, &capital))
                        continue;
                    if (j <= i) {
                        fstr_t value;
                        //x-dbg/ rio_debug(concs("looking up [", capital, "]"));
                        atest(qk_get(qk, capital, &value));
                        fstr_t expect_value;
                        if ((j % 3) == 0) {
                            expect_value = concs(capital, " of ", country);
                        } else if ((j % 3) == 1) {
                            expect_value = fstr_slice(capital, 0, 3);
                        } else {
                            expect_value = fss(fstr_reverse(country));
                        }
                        atest(fstr_equal(value, expect_value));
                        j++;
                    }
                }}
            }{
                size_t j = 0;
                for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) { sub_heap {
                    fstr_t country, capital;
                    if (!fstr_divide(row, ",", &country, &capital))
                        continue;
                    if (j > i) {
                        fstr_t value;
                        //x-dbg/ rio_debug(concs("looking up [", capital, "]"));
                        atest(qk_get(qk, capital, &value));
                        atest(fstr_equal(value, country));
                    }
                    j++;
                }}
            }
            i++;
        }}
    }
    acid_fsync(ah);
    acid_close(ah);
    test_rm_db(db_path);
}}

typedef struct cc_pair {
    fstr_t capital;
    fstr_t country;
} cc_pair_t;

static void cc_pair_vec_init(cc_pair_t* cc_vec, dict(fstr_t)* countries) {
    size_t i = 0;
    dict_foreach(countries, fstr_t, capital, country) {
        cc_vec[i].capital = capital;
        cc_vec[i].country = country;
        i++;
    }
}

static void test2() { sub_heap {
    rio_debug("running test2\n");

    // Index capitals mapped to countries.
    dict(fstr_t)* countries = new_dict(fstr_t);
    extern fstr_t capitals;
    for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) {
        fstr_t country, capital;
        if (!fstr_divide(row, ",", &country, &capital))
            continue;
        dict_inserta(countries, fstr_t, capital, country);
    }

    // Allocate vector of capital/country pairs.
    size_t n_pairs = dict_count(countries, fstr_t);
    cc_pair_t cc_vec[n_pairs];

    qk_ctx_t* qk;
    acid_h* ah;
    fstr_t db_path = test_get_db_path();

    // First, test 1000 different orders of inserts.
    cc_pair_vec_init(cc_vec, countries);
    for (size_t i = 0; i < 1000; i++) sub_heap {
        if ((i % 100) == 0) {
            rio_debug(concs("insert order #", i, "/1000\n"));
        }
        // Open database and begin insert.
        test_open_new_qk(db_path, &qk, &ah, 0);
        for (size_t src = 0; src < n_pairs; src++) {
            fstr_t capital = cc_vec[src].capital, country = cc_vec[src].country, r_country;
            //x-dbg/ rio_debug(concs("inserting #", i, " [", capital, "] [", country, "]\n"));
            atest(!qk_get(qk, capital, &r_country));
            atest(qk_insert(qk, capital, country));
            atest(qk_get(qk, capital, &r_country));
            atest(fstr_equal(r_country, country));
        }
        // Shuffle the cc_vec.
        for (size_t src = 0; src < n_pairs; src++) {
            size_t dst = test_hash64_2n(i, src) % n_pairs;
            FLIP(cc_vec[dst], cc_vec[src]);
        }
        // Verify that all key pairs exists.
        for (size_t src = 0; src < n_pairs; src++) { sub_heap {
            fstr_t capital = cc_vec[src].capital, country = cc_vec[src].country, r_country;
            atest(qk_get(qk, capital, &r_country));
            atest(fstr_equal(r_country, country));
            fstr_t corrupt_capital = concs(capital, "\xfe");
            corrupt_capital.str[test_hash64_2n(i, src) % corrupt_capital.len]++;
            atest(!qk_get(qk, corrupt_capital, &r_country));
        }}
        // Close database.
        acid_close(ah);
        // Remove database.
        test_rm_db(db_path);
    }

    // Second, test 1000 different probability seeds.
    cc_pair_vec_init(cc_vec, countries);
    for (size_t i = 0; i < 1000; i++) sub_heap {
        if ((i % 100) == 0) {
            rio_debug(concs("height seed #", i, "/1000\n"));
        }
        // Open database and begin insert.
        qk_opt_t opt = {
            .dtrm_seed = 100 + i,
        };
        test_open_new_qk(db_path, &qk, &ah, &opt);
        for (size_t src = 0; src < n_pairs; src++) {
            fstr_t capital = cc_vec[src].capital, country = cc_vec[src].country, r_country;
            //x-dbg/ rio_debug(concs("inserting #", i, " [", capital, "] [", country, "]\n"));
            atest(!qk_get(qk, capital, &r_country));
            atest(qk_insert(qk, capital, country));
            atest(qk_get(qk, capital, &r_country));
            atest(fstr_equal(r_country, country));
        }
        // Verify that all key pairs exists.
        for (size_t src = 0; src < n_pairs; src++) { sub_heap {
            fstr_t capital = cc_vec[src].capital, country = cc_vec[src].country, r_country;
            atest(qk_get(qk, capital, &r_country));
            atest(fstr_equal(r_country, country));
            fstr_t corrupt_capital = concs(capital, "\xfe");
            corrupt_capital.str[test_hash64_2n(i, src) % corrupt_capital.len]++;
            atest(!qk_get(qk, corrupt_capital, &r_country));
        }}
        // Close database.
        acid_close(ah);
        // Remove database.
        test_rm_db(db_path);
    }
}}

/// Returns deterministic Gaussian noise.
/// The return value is in units of standard deviation in the range (-INF, +INF).
static double dtr_gnoise(uint64_t r, double mu, double sigma) {
    uint64_t r1 = test_hash64_2n(r, 0x8937588a2e5becb2);
    uint64_t r2 = test_hash64_2n(r, 0xf69dbe7ae1412feb);
    double u1;
    do {
        u1 = ((double) r1) * (1.0 / (double) UINT64_MAX);
    } while (u1 <= (DBL_EPSILON));
    double u2 = ((double) r2) * (1.0 / (double) UINT64_MAX);
    double z0 = sqrt(-2.0 * log(u1)) * cos((2.0 *  M_PI) * u2);
    return z0 * sigma + mu;
}

/// Fills string with deterministic noise.
static void dtr_str_fill(uint64_t r, fstr_t str) {
    uint64_t state = r;
    for (size_t i = 0; i < str.len;) {
        state = test_hash64_2n(state, 0xbc05101bce6231ae);
        for (size_t j = 0; i < str.len && j < 8; j++) {
            uint8_t chr = (state >> (j * 8));
            str.str[i++] = chr;
        }
    }
}

static fstr_t get_ent_key(uint64_t ent_id) {
    double noise = dtr_gnoise(ent_id, 0, 1);
    double avg_len = 50;
    double sdv_len = (noise < 0)? 20: 80;
    size_t key_len = MIN(MAX(avg_len + sdv_len * noise, 8.0), 0xffff.0p0);
    fstr_t key = fss(fstr_alloc(key_len));
    dtr_str_fill(ent_id, key);
    return key;

    /*
    uint128_t epoch_ns = 1428583206ULL * RIO_NS_SEC + RIO_NS_MS * ent_id;
    return fss(rio_clock_to_rfc3339(rio_epoch_to_clock_time(epoch_ns), 4));*/
}

static fstr_t get_ent_value(uint64_t ent_id) {
    double noise = dtr_gnoise(ent_id, 0, 1);
    double avg_len = 400;
    double sdv_len = (noise < 0)? 200: 300;
    size_t value_len = MAX(avg_len + sdv_len * noise, 0.0);
    fstr_t value = fss(fstr_alloc(value_len));
    dtr_str_fill(ent_id, value);
    return value;
}

static void test_128g(fstr_t db_path, bool scan) { sub_heap {
    // In this test we attempt to store 128 gibibytes of data.
    rio_debug("running test3 - the 128 GiB test\n");
    const uint64_t target_bytes = 128ULL * 1024 * 1024 * 1024;
    if (db_path.len == 0)
        db_path = test_get_db_path();
    rio_debug(concs("data is stored at [", db_path, "]\n"));
    // Open database.
    qk_ctx_t* qk;
    acid_h* ah;
    qk_opt_t opt = {
        .dtrm_seed = 0x5aaad5b38fd30f38,
        .target_ipp = 40,
    };
    test_open_new_qk(db_path, &qk, &ah, &opt);
    uint64_t ent_seq = 0;
    uint64_t total_snaps = 0;
    //x-dbg/ fstr_t maps_buffer = fss(fstr_alloc_buffer(0x10000000));

    for (;;) sub_heap {

        json_value_t stats = qk_get_stats(qk);
        uint64_t total_alloc = 0;
        uint64_t i_lvl = 0;
        JSON_ARR_FOREACH(JSON_REF(stats, "levels"), level) {
            if (i_lvl == 0) {
                uint64_t count = jnumv(JSON_REF(level, "ent_count"));
                if (ent_seq != count) {
                    rio_debug(concs("invalid ent_seq, calibrating: [", ent_seq, "] [", count, "]\n"));
                    if (ent_seq != 0)
                        lwt_exit(1);
                    ent_seq = count;
                }
            }
            total_alloc += jnumv(JSON_REF(level, "total_alloc_b"));
            i_lvl++;
        }
        rio_debug(concs("allocated [", (total_alloc / 1024 / 1024), "/",
            (target_bytes / 1024 / 1024), "] MiB, total snapshots: [", total_snaps, "]\n"));

        if (scan) {
            bool first = true;
            fstr_t band_mem = fss(fstr_alloc_buffer(10000 * PAGE_SIZE));
            rio_debug(concs("scanning with [", (band_mem.len), "] byte band\n"));
            fstr_t prev_key = "";
            size_t i = 0;
            uint128_t d0 = rio_get_time_timer();
            for (;;) {
                fstr_t band = band_mem;
                qk_scan_op_t op = {
                    .with_start = !first,
                    .key_start = prev_key,
                    .inc_start = false
                };
                bool eof;
                qk_scan(qk, op, &band, &eof);
                for (;;) {
                    fstr_t key, value;
                    if (!qk_band_read(&band, &key, &value)) {
                        rio_debug(concs("end of band, read [", i, "/", ent_seq, "] entries\n"));
                        if (eof) {
                            atest(i == ent_seq);
                            uint128_t d1 = rio_get_time_timer();
                            uint128_t dt = (d1 - d0);
                            double dts = (dt / RIO_NS_MS) / 1000.0;
                            rio_debug(concs("scan complete, took [", dts, "] sec\n"));
                            rio_debug(concs("scan speed [", ((total_alloc / 1024. / 1024.) / dts), "] MiB/s, [", (ent_seq / dts), "] ents/s\n"));
                            lwt_exit(0);
                        }
                        prev_key = fsc(key);
                        break;
                    }
                    atest(fstr_cmp_lexical(key, prev_key) > 0);
                    prev_key = key;
                    i++;
                }
                first = false;
            }
        }

        if (total_alloc >= target_bytes)
            goto done;
        size_t n_inserts = 20000;

        /*rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_a", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));*/

        rio_debug(concs("testing [", n_inserts, "] * 2 keys\n"));

        for (size_t i = 0; i < n_inserts; i++) sub_heap {
            // Read n_inserts * 2 random log points to confirm integrity.
            if (ent_seq > 0) {
                uint64_t ent_id = test_hash64_2n(i, ent_seq) % ent_seq;
                fstr_t key = get_ent_key(ent_id);
                fstr_t value = get_ent_value(ent_id), r_value;
                if (!qk_get(qk, key, &r_value)) {
                    rio_debug(concs("could not find inserted key [", fss(fstr_hexencode(key)), "] (test #", i, ") (entry #", ent_id, ")!\n"));
                    lwt_exit(1);
                }
                if (!fstr_equal(value, r_value)) {
                    rio_debug(concs("invalid value [", fss(fstr_hexencode(value)), "] != [", fss(fstr_hexencode(r_value)), "] (test #", i, ") (entry #", ent_id, ")!\n"));
                    lwt_exit(1);
                }
            }
            {
                uint64_t ent_id = ent_seq + (test_hash64_2n(i, ent_seq) % n_inserts);
                fstr_t key = get_ent_key(ent_id), r_value;
                atest(!qk_get(qk, key, &r_value));
            }
        }

        rio_debug(concs("inserting [", n_inserts, "] keys\n"));

        uint128_t t0 = rio_epoch_ns_now();

        bool snap_ok = false;
        for (size_t i = 0; i < n_inserts; i++) sub_heap {
            uint64_t ent_id = (ent_seq++);
            fstr_t key = get_ent_key(ent_id);

            fstr_t value = get_ent_value(ent_id), r_value;

            if ((i % 40) != 0) {
                //x-dbg/ rio_debug(concs("inserting [", fss(fstr_hexencode(fstr_slice(key, 0, 8))), "] => [", fss(fstr_hexencode(fstr_slice(value, 0, 8))), "] (entry #", ent_id, ")!\n"));
                if (!qk_insert(qk, key, value)) {
                    rio_debug(concs("key conflict for [", fss(fstr_hexencode(key)), "] (entry #", ent_id, ")!\n"));
                    lwt_exit(1);
                }
            } else {
                // Every 40th insert we first insert the wrong value and then update to the right value to stress test update.
                fstr_t wrong_value = get_ent_value(ent_id + 1);
                atest(qk_insert(qk, key, wrong_value));
                // Since we don't sync in between the wrong value should never be visible when reopening the database.
                atest(qk_update(qk, key, value));
            }
            atest(qk_get(qk, key, &r_value));
            atest(fstr_equal(value, r_value));

            if (acid_snapshot(ah)) {
                total_snaps++;
                snap_ok = true;
            }
        }

        /*
        rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_b", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));
        rio_debug(concs("fsyncing!\n"));
        acid_fsync(ah);
        rio_debug(concs("fsync complete!\n"));
        rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_c", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));

        rio_wait(RIO_NS_SEC * 4);


        rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_c2", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));


        acid_fsync(ah);

        rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_c3", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));

        rio_debug(concs("closing!\n"));
        acid_close(ah);
        rio_debug(concs("writing maps\n"));
        rio_write_file_contents("/tmp/maps_d", rio_read_virtual_file_contents("/proc/self/maps", maps_buffer));
        lwt_exit(0);*/

        if (!snap_ok) {
            rio_debug(concs("all snapshots failed! throttling insert until fsync completes..."));
            acid_fsync(ah);
            rio_debug(concs("fsync complete!\n"));
        }

        /*
        rio_debug(concs("fsyncing... "));
        acid_fsync(ah);

        uint128_t t1 = rio_epoch_ns_now();

        rio_debug(concs("fsync complete! [", ((t1 - t0) / RIO_NS_SEC), "] sec\n"));

        rio_wait(RIO_NS_SEC * 10000);
        */
    }
    done:;
    rio_debug(concs("done, data is stored at [", db_path, "]\n"));
    lwt_exit(0);
}}

void rcd_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    /*for (size_t i = 0; i < 8; i++) {
        DBGFN("[", i, "]: ", fss(fstr_hexencode(get_ent_value(i))));
    }
    lwt_exit(0);*/
    vis_init();
    fstr_t arg0;
    if (list_unpack(main_args, fstr_t, &arg0)) {
        (void) list_pop_start(main_args, fstr_t);
        if (fstr_equal(arg0, "128g")) {
            fstr_t db_path, op;
            bool scan = false;
            if (list_unpack(main_args, fstr_t, &db_path, &op)) {
                scan = fstr_equal_case(op, "scan");
            } else {
                db_path = "";
            }
            test_128g(db_path, scan);
        } else {
            throw(concs("unknown test [", arg0, "]"), exception_io);
        }
    } else {
        // Standard test.
        rio_debug("standard test\n");
        test0();
        test01();
        test1();
        test2();
        rio_debug("tests done\n");
    }
    lwt_exit(0);
}
