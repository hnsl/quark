#include "rcd.h"
#include "linux.h"
#include "acid.h"
#include "../src/quark-internal.h"
#include "ifc.h"

#pragma librcd

static void test0(fstr_t data_file, fstr_t journal_file) {
    sub_heap {
        acid_h* ah = acid_open(data_file, journal_file, ACID_ADDR_0, 0);
        DBG("Testing basic acid-allocation");
        sf(quark)* sf = qk_init(ah);
        uint128_t qfid = quark_sf2id(sf).fid;
        void* ptr1 = qk_alloc(PAGE_SIZE, qfid);
        uint8_t* ptr2 = qk_alloc(PAGE_SIZE * 63, qfid);
        memset((void*)ptr2, 0xff, PAGE_SIZE * 63);
        for (size_t i = 0; i < PAGE_SIZE * 63; i++) {
            atest(ptr2[i] == 0xff);
        }
        qk_free(ptr1, PAGE_SIZE, qfid);
        qk_free(ptr2, PAGE_SIZE * 63, qfid);
    }
}

static void test1(fstr_t data_file, fstr_t journal_file) {
    DBG("Testing durability");
    uint8_t* ptr;
    sub_heap {
        acid_h* ah = acid_open(data_file, journal_file, ACID_ADDR_0, 0);
        sf(quark)* sf = qk_init(ah);
        uint128_t qfid = quark_sf2id(sf).fid;
        ptr = qk_alloc(PAGE_SIZE, qfid);
        memset((void*)ptr, 0x7f, PAGE_SIZE);
    }
    sub_heap {
        acid_h* ah = acid_open(data_file, journal_file, ACID_ADDR_0, 0);
        sf(quark)* sf = qk_init(ah);
        uint128_t qfid = quark_sf2id(sf).fid;
        for (size_t i = 0; i < PAGE_SIZE; i++) {
            atest(ptr[i] == 0x7f);
        }
        qk_free(ptr, PAGE_SIZE, qfid);
    }
}

void rcd_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    fstr_t data_file = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".data");
    fstr_t journal_file = concs("/var/tmp/.librcd-acid-test.", lwt_rdrand64(), ".jrnl");
    test0(data_file, journal_file);
    test1(data_file, journal_file);
    exit(0);
}
