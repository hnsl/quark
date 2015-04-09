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

static void test1() { sub_heap {
    rio_debug("running test1\n");
    qk_ctx_t* qk;
    acid_h* ah;
    fstr_t db_path = test_get_db_path();
    test_open_new_qk(db_path, &qk, &ah, 0);
    //x-dbg/ vis_snapshot(qk);
    //x-dbg/ print_stats(qk);
    size_t n = 0;
    extern fstr_t capitals;
    bool found_andorra = false;
    for (fstr_t row, tail = capitals; fstr_iterate_trim(&tail, "\n", &row);) {
        fstr_t country, capital;
        if (!fstr_divide(row, ",", &country, &capital))
            continue;

        fstr_t value;
        atest(!qk_get(qk, capital, &value));
        atest(qk_insert(qk, capital, country));
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

static void test_128g(fstr_t db_path) { sub_heap {
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
            (target_bytes / 1024 / 1024), "] mb, total snapshots: [", total_snaps, "]\n"));
        if (total_alloc >= target_bytes)
            goto done;
        size_t n_inserts = 200000;

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
            //x-dbg/ rio_debug(concs("inserting [", fss(fstr_hexencode(fstr_slice(key, 0, 8))), "] => [", fss(fstr_hexencode(fstr_slice(value, 0, 8))), "] (entry #", ent_id, ")!\n"));
            if (!qk_insert(qk, key, value)) {
                rio_debug(concs("key conflict for [", fss(fstr_hexencode(key)), "] (entry #", ent_id, ")!\n"));
                lwt_exit(1);
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
            fstr_t db_path;
            if (!list_unpack(main_args, fstr_t, &db_path))
                db_path = "";
            test_128g(db_path);
        } else {
            throw(concs("unknown test [", arg0, "]"), exception_io);
        }
    } else {
        // Standard test.
        rio_debug("standard test\n");
        test0();
        test1();
        test2();
        rio_debug("tests done\n");
    }
    lwt_exit(0);
}
