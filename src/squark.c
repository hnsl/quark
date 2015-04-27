/*
 * Sub-quark process
 */
#include "rcd.h"
#include "squark.h"
#include "musl.h"

#pragma librcd

typedef struct {
    lwt_heap_t* heap;
    list(uint128_t)* sync_ids;
} sync_queue_t;

typedef struct {
    rcd_fid_t main_fid;
    acid_h* ah;
    qk_ctx_t* qk;
    rio_t* in_h;
    rio_t* out_h;
    sync_queue_t sq_cur;
    sync_queue_t sq_pnd;
    bool is_dirty;
} sq_state_t;

typedef enum {
    // Barrier: will start sync and notify when any previous operations are synced.
    SQUARK_CMD_BARRIER = 0,
    // Request to scan data from key.
    SQUARK_CMD_SCAN = 100,
    // Immutable store: inserts key/value.
    // When key already exists the insert is ignored.
    SQUARK_CMD_INSERT_IMM = 200,
    /*
    // Mutable store: inserts key/value.
    // When key already exists the existing value is returned.
    SQUARK_CMD_STORE_MUT = 201,*/
    // Updates existing key. When key does not exists an insert is made instead.
    SQUARK_CMD_UPSERT = 202,
} squark_cmd_t;

typedef enum {
    // Sync completed.
    SQUARK_RES_SYNC = 0,
    // Scan response.
    SQUARK_RES_SCAN = 100,
    /*
    // Response from SQUARK_CMD_STORE_MUT.
    // Key value pair already exists. The existing value is returned.
    SQUARK_RES_BOUNCE_MUT = 200*/
} squark_res_t;

sync_queue_t sq_new() {
    sync_queue_t ret = {
        .heap = lwt_alloc_heap(),
        .sync_ids = new_list(uint128_t),
    };
    return ret;
}

static void sq_add(sync_queue_t sq, uint128_t sync_id) {
    switch_heap (sq.heap) {
        list_push_end(sq.sync_ids, uint128_t, sync_id);
    }
}

static void squark_start_sync(sq_state_t* state);

join_locked(void) squark_sync_done(join_server_params, sq_state_t* state) { server_heap_flip {
    // Notify all given barriers that sync was complete.
    assert(state->sq_cur.sync_ids != 0);
    list_foreach(state->sq_cur.sync_ids, uint128_t, sync_id) {
        rio_write_u16(state->out_h, SQUARK_RES_SYNC, true);
        rio_write_u128(state->out_h, sync_id, false);
    }
    lwt_alloc_free(state->sq_cur.heap);
    // Determine next sync.
    if (state->sq_pnd.sync_ids == 0) {
        // No more sync barriers.
        state->sq_cur.sync_ids = 0;
    } else {
        // Start sync for pending barriers.
        state->sq_cur = state->sq_pnd;
        state->sq_pnd.sync_ids = 0;
        squark_start_sync(state);
    }
}}

fiber_main squark_sync_wait(fiber_main_attr, rcd_fid_t main_fid, rcd_fid_t sync_fid) { try {
    //x-dbg/ DBGFN("waiting for sync [", sync_fid, "]");
    ifc_wait(sync_fid);
    //x-dbg/ DBGFN("wait for sync [", sync_fid, "] complete");
    squark_sync_done(main_fid);
} catch (exception_desync, e); }

static void squark_start_sync(sq_state_t* state) {
    //x-dbg/ DBGFN("starting squark sync");
    fmitosis {
        rcd_fid_t sync_fid = acid_fsync_async(state->ah);
        assert(sync_fid != 0);
        state->is_dirty = false;
        spawn_static_fiber(squark_sync_wait("", state->main_fid, sync_fid));
    }
}

join_locked(void) squark_read(join_server_params, sq_state_t* state) {
    for (;;) { sub_heap {
        squark_cmd_t cmd = rio_read_u16(state->in_h);
        switch (cmd) {{
        } case SQUARK_CMD_BARRIER: {
            // Create a sync barrier with a specified id.
            uint128_t sync_id = rio_read_u128(state->in_h);
            server_heap_flip {
                if (state->sq_cur.sync_ids == 0) {
                    // No existing barriers, start sync now.
                    squark_start_sync(state);
                    state->sq_cur = sq_new();
                    sq_add(state->sq_cur, sync_id);
                } else {
                    // Add to pending barriers.
                    if (state->sq_pnd.sync_ids == 0)
                        state->sq_pnd = sq_new();
                    sq_add(state->sq_pnd, sync_id);
                }
            }
            break;
        } case SQUARK_CMD_SCAN: {
            // Request to scan data with a specific id.
            //x-dbg/ DBGFN("got scan op, reading op");
            uint128_t request_id = rio_read_u128(state->in_h);
            qk_scan_op_t op;
            rio_read_fill(state->in_h, FSTR_PACK(op));
            if (op.with_start)
                op.key_start = fss(rio_read_fstr(state->in_h));
            if (op.with_end)
                op.key_end = fss(rio_read_fstr(state->in_h));
            //x-dbg/ DBGFN("scan op read, executing");
            // Execute scan.
            bool eof;
            fstr_t band_mem = fss(fstr_alloc_buffer(1000 * PAGE_SIZE));
            uint64_t count = qk_scan(state->qk, op, &band_mem, &eof);
            // Write result back.
            //x-dbg/ DBGFN("writing result back (", count, ")");
            rio_write_u16(state->out_h, SQUARK_RES_SCAN, true);
            rio_write_u128(state->out_h, request_id, true);
            rio_write_u64(state->out_h, count, true);
            rio_write_bool(state->out_h, eof, true);
            rio_write_fstr(state->out_h, band_mem);
            break;
        } case SQUARK_CMD_UPSERT: {
        } case SQUARK_CMD_INSERT_IMM: {
            // Store/update an entry.
            fstr_t key = fss(rio_read_fstr(state->in_h));
            fstr_t value = fss(rio_read_fstr(state->in_h));
            if (cmd == SQUARK_CMD_UPSERT) {
                // Attempt to update.
                //x-dbg/ DBGFN("update: [", key, "] => [", value, "]");
                if (qk_update(state->qk, key, value)) {
                    state->is_dirty = true;
                    break;
                }
            }
            //x-dbg/ DBGFN("insert: [", key, "] => [", value, "]");
            bool insert_ok = qk_insert(state->qk, key, value);
            if (!insert_ok) {
                if (cmd == SQUARK_CMD_UPSERT)
                    throw("insert conflict after update failure", exception_fatal);
            } else {
                state->is_dirty = true;
            }

            /*
            if (!insert_ok) {
                if (SQUARK_RES_BOUNCE_MUT) {
                    // Respond with mutable store bounce.
                    fstr_t cur_value;
                    if (!qk_get(state->qk, key, &cur_value))
                        throw("get failed after insert conflict", exception_fatal);
                    rio_write_u16(state->out_h, SQUARK_RES_BOUNCE_MUT, true);
                    rio_write_fstr(state->out_h, key);
                    rio_write_fstr(state->out_h, cur_value);
                } else {
                    // Ignore non-existence.
                }
            }*/
            break;
        } default: {
            throw(concs("unknown command! [", cmd, "]"), exception_fatal);
            break;
        }}
        if (!rio_poll(state->in_h, true, false))
            return;
    }}
}

fiber_main squark_stdin_reader(fiber_main_attr, rcd_fid_t main_fid, rio_epoll_t* stdin_epoll) { try {
    try {
        for (;;) {
            rio_epoll_poll(stdin_epoll, true);
            squark_read(main_fid);
        }
    } catch_eio (rio_eos, e) {
        // End of pipe stream means that parent closed and we will be terminated.
        // We use the exit code 8 to signal that we where not shut down in the
        // manner we would have actually preferred (SIGTERM/SIGKILL).
        lwt_exit(8);
    }
} catch (exception_desync, e); }

void squark_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    fstr_t arg0, db_path, target_ipp_arg;
    if (!list_unpack(main_args, fstr_t, &arg0, &db_path, &target_ipp_arg))
        return;
    if (!fstr_equal(arg0, "squark"))
        return;
    try {
        // Open acid handle.
        fstr_t data_path = concs(db_path, ".data");
        fstr_t journal_path = concs(db_path, ".journal");
        acid_h* ah = acid_open(data_path, journal_path, ACID_ADDR_0, 0);
        // Configure squark and open it.
        uint16_t target_ipp = fs2ui(target_ipp_arg);
        qk_opt_t opt = {
            .overwrite_target_ipp = true,
            .target_ipp = target_ipp,
        };
        qk_ctx_t* qk = qk_open(ah, &opt);
        // Initialize state.
        sq_state_t* state = new(sq_state_t);
        state->main_fid = rcd_self;
        state->ah = ah;
        state->qk = qk;
        // Open in_h/out_h.
        // TODO: Perhaps use ib pipe for fast buffered out_h.
        state->in_h = rio_stdin();
        state->out_h = rio_stdout();
        // Spawn fiber that waits on in_h and reads.
        fmitosis {
            rio_epoll_t* stdin_epoll = rio_epoll_create(state->in_h, rio_epoll_event_inlvl);
            spawn_static_fiber(squark_stdin_reader("", rcd_self, stdin_epoll));
        }
        for (;;) {
            // Accept asynchronous events (from file system or from stdin).
            accept_join(squark_read, squark_sync_done, join_server_params, state);
            // Attempt to clean state now by synchronizing if needed and possible.
            // This ensures that we flush to disk asynchronously at the maximum possible rate.
            if (state->is_dirty && state->sq_cur.sync_ids == 0) {
                squark_start_sync(state);
                state->sq_cur = sq_new();
                assert(!state->is_dirty);
            }
        }
    } catch (exception_any, e) {
        // Add context to exception and rethrow.
        throw_fwd_same(concs("unhandled squark exception [", db_path, "]"), e);
    }
}

join_locked(void) has_scan_band_res(fstr_mem_t* scan_band, uint64_t count, bool eof, join_server_params, fstr_mem_t** out_scan_band, uint64_t* out_count, bool* out_eof) { server_heap_flip {
    *out_scan_band = import(scan_band);
    *out_count = count;
    *out_eof = eof;
}}

fiber_main squark_reader(fiber_main_attr, rio_t* in_h) { try {
    for (;;) sub_heap {
        squark_res_t rsp = rio_read_u16(in_h);
        switch (rsp) {{
        } case SQUARK_RES_SYNC: {
            // Sync complete, kill barrier.
            uint128_t barrier_fid = rio_read_u128(in_h);
            lwt_cancel_fiber_id(barrier_fid);
            break;
        } case SQUARK_RES_SCAN: {
            uint128_t scan_res_fid = rio_read_u128(in_h);
            uint64_t count = rio_read_u64(in_h);
            bool eof = rio_read_bool(in_h);
            fstr_mem_t* scan_band = rio_read_fstr(in_h);
            try {
                // Send result back to waiting fiber.
                has_scan_band_res(scan_band, count, eof, scan_res_fid);
            } catch (exception_inner_join_fail, e) {
                // No longer interested in result.
            }
            break;
        } default: {
            throw(concs("unknown response! [", rsp, "]"), exception_fatal);
            break;
        }}
    }
} catch(exception_desync, e); }

/// Watches a squark.
fiber_main squark_watcher(fiber_main_attr, rio_proc_t* proc) { try {
    int32_t rcode = rio_proc_wait(proc);
    throw(concs("squark crashed with [", rcode, "]"), exception_fatal);
} catch (exception_desync, e); }

squark_t* squark_spawn(fstr_t db_dir, fstr_t index_id, uint16_t target_ipp, list(fstr_t)* unix_env) { sub_heap {
    fstr_t db_path = concs(db_dir, "/", index_id);
    //x-dbg/ DBGFN("starting squark [", db_path, "]");
    // Execute squark subprocess.
    rio_t *stdin_pipe_r, *stdin_pipe_w;
    rio_realloc_split(rio_open_pipe(), &stdin_pipe_r, &stdin_pipe_w);
    rio_t *stdout_pipe_r, *stdout_pipe_w;
    rio_realloc_split(rio_open_pipe(), &stdout_pipe_r, &stdout_pipe_w);
    rio_sub_exec_t se = { .exec = {
        .path = rio_self_path,
        .args = new_list(fstr_t, "squark", db_path, ui2fs(target_ipp)),
        .env = unix_env,
        .io_in = stdin_pipe_r,
        .io_out = stdout_pipe_w,
    }};
    rio_proc_t* proc_h = escape(rio_proc_execute(se));
    // Return handle to squark.
    lwt_heap_t* heap = lwt_alloc_heap();
    squark_t* sq;
    switch_heap (heap) {
        sq = new(squark_t);
        sq->is_dirty = false;
        sq->heap = heap;
        sq->proc = import(proc_h);
        sq->out_h = import(stdin_pipe_w);
        fmitosis {
            sq->reader = spawn_fiber(squark_reader("", import(stdout_pipe_r)));
        }
        fmitosis {
            sq->watcher = spawn_fiber(squark_watcher("", sq->proc));
        }
        // Import stdin/stdout pipe. We are not interesting in detecting exit via pipe i/o error.
        // Exit detect is done via watcher fiber instead.
        import_list(stdin_pipe_r, stdout_pipe_w);
    }
    return escape_complex(sq);
}}

void squark_kill(squark_t* sq) {
    // Clean up fibers first to avoid io exception.
    lwt_alloc_free(sq->watcher);
    lwt_alloc_free(sq->reader);
    // Now kill process.
    rio_proc_signal(sq->proc, SIGKILL);
    rio_proc_wait(sq->proc);
    lwt_alloc_free(sq->heap);
}

fiber_main squark_barrier_fiber(fiber_main_attr, rcd_fid_t watcher_fid) { try {
    // Keep barrier fiber alive as long as squark is alive.
    ifc_wait(watcher_fid);
} catch (exception_desync, e); }

rcd_fid_t squark_op_barrier(squark_t* sq) {
    uninterruptible {
        rcd_fid_t barrier_fid;
        fmitosis {
            rcd_fid_t watcher_fid = sfid(sq->watcher);
            barrier_fid = spawn_static_fiber(squark_barrier_fiber("", watcher_fid));
        }
        rio_write_u16(sq->out_h, SQUARK_CMD_BARRIER, true);
        rio_write_u128(sq->out_h, barrier_fid, false);
        return barrier_fid;
    }
}

void squark_op_insert(squark_t* sq, fstr_t key, fstr_t value) {
    uninterruptible {
        rio_write_u16(sq->out_h, SQUARK_CMD_INSERT_IMM, true);
        rio_write_fstr(sq->out_h, key);
        rio_write_fstr(sq->out_h, value);
    }
}

void squark_op_upsert(squark_t* sq, fstr_t key, fstr_t value) {
    uninterruptible {
        rio_write_u16(sq->out_h, SQUARK_CMD_UPSERT, true);
        rio_write_fstr(sq->out_h, key);
        rio_write_fstr(sq->out_h, value);
    }
}

join_locked(fstr_mem_t*) get_scan_band_res(uint64_t* out_count, bool* out_eof, join_server_params, fstr_mem_t* scan_band, uint64_t count, bool eof) {
    *out_count = count;
    *out_eof = eof;
    return import(scan_band);
}

fiber_main scan_op_fiber(fiber_main_attr) { try {
    fstr_mem_t* scan_band;
    uint64_t count;
    bool eof;
    accept_join(has_scan_band_res, join_server_params, &scan_band, &count, &eof);
    accept_join(get_scan_band_res, join_server_params, scan_band, count, eof);
} catch (exception_desync, e); }

rcd_sub_fiber_t* squark_op_scan(squark_t* sq, qk_scan_op_t op) {
    uninterruptible fmitosis {
        // We could design this so the scan op fiber does the write asynchronously instead
        // but it's not necessary because deadlock is impossible anyway as the reader is never
        // really blocking on anything. Scan results are already passed asynchronously back.
        rio_write_u16(sq->out_h, SQUARK_CMD_SCAN, true);
        rio_write_u128(sq->out_h, new_fid, true);
        rio_write(sq->out_h, FSTR_PACK(op));
        if (op.with_start)
            rio_write_fstr(sq->out_h, op.key_start);
        if (op.with_end)
            rio_write_fstr(sq->out_h, op.key_end);
        //x-dbg/ DBGFN("written scan op");
        return spawn_fiber(scan_op_fiber(""));
    }
}

fstr_mem_t* squark_get_scan_res(rcd_fid_t scan_fid, uint64_t* out_count, bool* out_eof) { sub_heap {
    try {
        return escape(get_scan_band_res(out_count, out_eof, scan_fid));
    } catch (exception_inner_join_fail, e) {
        // Expected when squark is deleted.
        *out_count = 0;
        *out_eof = true;
        return escape(fstr_cpy(""));
    }
}}

void squark_rm_index(fstr_t db_dir, fstr_t index_id) { sub_heap {
    fstr_t db_path = concs(db_dir, "/", index_id);
    fstr_t data_path = concs(db_path, ".data");
    fstr_t journal_path = concs(db_path, ".journal");
    rio_file_unlink(journal_path);
    rio_file_unlink(data_path);
}}

list(fstr_t)* squark_get_indexes(fstr_t db_dir) {
    list(fstr_t)* indexes = new_list(fstr_t);
    list_foreach(rio_file_list(db_dir), fstr_mem_t*, name_mem) {
        fstr_t name = fss(name_mem);
        fstr_t index_id;
        #pragma re2c(name): ^ ([^\.]+){index_id} \. data {@match}
        continue;
        match: {
            list_push_end(indexes, fstr_t, index_id);
        }
    }
    return indexes;
}
