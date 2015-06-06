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
    rio_t* out_h;
    dict(qk_map_ctx_t*)* maps;
    void* cb_ctx_ptr;
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
    // Performs an abstract operation on the squark map without returning a result.
    SQUARK_CMD_PERFORM = 203,
    // Request to provide status.
    SQUARK_CMD_STATUS = 300,
} squark_cmd_t;

typedef enum {
    // Sync completed.
    SQUARK_RES_SYNC = 0,
    // Scan response.
    SQUARK_RES_SCAN = 100,
    // Status response.
    SQUARK_RES_STATUS = 300,
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

static qk_map_ctx_t* resolve_map_ctx(sq_state_t* state, fstr_t map_id) {
    qk_map_ctx_t** map_ptr = dict_read(state->maps, qk_map_ctx_t*, map_id);
    if (map_ptr == 0)
        throw(concs("no such map [", map_id, "]"), exception_fatal);
    return *map_ptr;
}

__attribute__((weak))
void* squark_cb_init_ctx(acid_h* ah, qk_ctx_t* qk, dict(qk_map_ctx_t*)* maps) {
    return 0;
}

__attribute__((weak))
void squark_cb_perform(void* ctx_ptr, fstr_t op_arg) {
    throw("squark perform callback not implemented", exception_fatal);
}

join_locked(void) squark_read(rio_t* in_h, join_server_params, sq_state_t* state) {
    for (;;) { sub_heap {
        squark_cmd_t cmd = rio_read_u16(in_h);
        switch (cmd) {{
        } case SQUARK_CMD_BARRIER: {
            // Create a sync barrier with a specified id.
            uint128_t sync_id = rio_read_u128(in_h);
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
            fstr_t map_id = fss(rio_read_fstr(in_h));
            uint128_t request_id = rio_read_u128(in_h);
            qk_scan_op_t op;
            rio_read_fill(in_h, FSTR_PACK(op));
            if (op.with_start)
                op.key_start = fss(rio_read_fstr(in_h));
            if (op.with_end)
                op.key_end = fss(rio_read_fstr(in_h));
            //x-dbg/ DBGFN("scan op read, executing");
            qk_map_ctx_t* map = resolve_map_ctx(state, map_id);
            // Execute scan.
            bool eof;
            fstr_t band_mem = fss(fstr_alloc_buffer(1000 * PAGE_SIZE));
            uint64_t count = qk_scan(map, op, &band_mem, &eof);
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
            fstr_t map_id = fss(rio_read_fstr(in_h));
            fstr_t key = fss(rio_read_fstr(in_h));
            fstr_t value = fss(rio_read_fstr(in_h));
            qk_map_ctx_t* map = resolve_map_ctx(state, map_id);
            if (cmd == SQUARK_CMD_UPSERT) {
                // Attempt to update.
                //x-dbg/ DBGFN("update: [", key, "] => [", value, "]");
                if (qk_update(map, key, value)) {
                    state->is_dirty = true;
                    break;
                }
            }
            //x-dbg/ DBGFN("insert: [", key, "] => [", value, "]");
            bool insert_ok = qk_insert(map, key, value);
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
        } case SQUARK_CMD_PERFORM: {
            // Run abstract operation.
            fstr_t arg = fss(rio_read_fstr(in_h));
            squark_cb_perform(state->cb_ctx_ptr, arg);
            // Assume all perform operations are dirty.
            state->is_dirty = true;
            break;
        } case SQUARK_CMD_STATUS: {
            // Request to read status with a specific id.
            uint128_t request_id = rio_read_u128(in_h);
            json_value_t stats = jobj_new();
            dict_foreach(state->maps, qk_map_ctx_t*, map_id, map) {
                JSON_SET(stats, map_id, qk_get_stats(map));
            }
            // Write result back.
            rio_write_u16(state->out_h, SQUARK_RES_STATUS, true);
            rio_write_u128(state->out_h, request_id, true);
            rio_write_fstr(state->out_h, fss(json_stringify(stats)));
            break;
        } default: {
            throw(concs("unknown command! [", cmd, "]"), exception_fatal);
            break;
        }}
        if (!rio_poll(in_h, true, false))
            return;
    }}
}

fiber_main squark_stdin_reader(fiber_main_attr, rcd_fid_t main_fid, rio_t* in_h) { try {
    try {
        for (;;) {
            rio_poll(in_h, true, true);
            squark_read(in_h, main_fid);
        }
    } catch_eio (rio_eos, e) {
        // End of pipe stream means that parent closed and we will be terminated.
        // We use the exit code 8 to signal that we where not shut down in the
        // manner we would have actually preferred (SIGTERM/SIGKILL).
        lwt_exit(8);
    }
} catch (exception_desync, e); }

void squark_main(list(fstr_t)* main_args, list(fstr_t)* main_env) {
    fstr_t arg0, db_path;
    if (!list_unpack(main_args, fstr_t, &arg0, &db_path))
        return;
    if (!fstr_equal(arg0, "squark"))
        return;
    try {
        // Open acid and quark handle.
        fstr_t data_path = concs(db_path, ".data");
        fstr_t journal_path = concs(db_path, ".journal");
        acid_h* ah = acid_open(data_path, journal_path, ACID_ADDR_0, 0);
        qk_ctx_t* qk = qk_open(ah);
        // Initialize state.
        sq_state_t* state = new(sq_state_t);
        state->main_fid = rcd_self;
        state->ah = ah;
        state->qk = qk;
        // Open in_h/out_h.
        // TODO: Perhaps use ib pipe for fast buffered out_h.
        rio_t* in_h = rio_stdin();
        state->out_h = rio_stdout();
        // Read schema and open all maps.
        state->maps = new_dict(qk_map_ctx_t*);
        sub_heap_txn(heap) {
            fstr_t jschema = fss(rio_read_fstr(in_h));
            json_value_t schema = json_parse(jschema)->value;
            JSON_OBJ_FOREACH(schema, map_key, map_cfg) {
                qk_opt_t opt = {
                    .target_ipp = jnumv(JSON_REF(map_cfg, "ipp")),
                };
                switch_heap(heap) {
                    qk_map_ctx_t* map = qk_open_map(qk, map_key, &opt);
                    dict_inserta(state->maps, qk_map_ctx_t*, map_key, map);
                }
            }
        }
        // Initialize callback context. This can leak all sorts of memory.
        state->cb_ctx_ptr = squark_cb_init_ctx(state->ah, state->qk, state->maps);
        // Spawn fiber that waits on in_h and reads.
        // It would be raceful to use a system epoll as we already use the rio handle
        // above to read the schema. We therefore import the in handle to the reader
        // fiber and wait with a proper librcd rio poll instead.
        fmitosis {
            spawn_static_fiber(squark_stdin_reader("", rcd_self, import(in_h)));
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

join_locked(void) has_status_res(fstr_mem_t* status, join_server_params, fstr_mem_t** out_status) { server_heap_flip {
    *out_status = import(status);
}}

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
        } case SQUARK_RES_STATUS: {
            uint128_t status_res_fid = rio_read_u128(in_h);
            fstr_mem_t* status_res = rio_read_fstr(in_h);
            try {
                // Send result back to waiting fiber.
                has_status_res(status_res, status_res_fid);
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

join_locked(void) squark_write(vec(fstr_t)* io_v, join_server_params, rio_t* out_h) { uninterruptible {
    size_t len = vec_count(io_v, fstr_t);
    vec_foreach(io_v, fstr_t, i, chunk) {
        rio_write_part(out_h, chunk, i < (len - 1));
    }
}}

fiber_main squark_writer(fiber_main_attr, rio_t* out_h) { try {
    auto_accept_join(squark_write, join_server_params, out_h);
} catch(exception_desync, e); }

/// Watches a squark.
fiber_main squark_watcher(fiber_main_attr, rio_proc_t* proc) { try {
    int32_t rcode = rio_proc_wait(proc);
    throw(concs("squark crashed with [", rcode, "]"), exception_fatal);
} catch (exception_desync, e); }

squark_t* squark_spawn(fstr_t db_dir, fstr_t index_id, json_value_t schema, list(fstr_t)* unix_env) { sub_heap {
    fstr_t db_path = concs(db_dir, "/", index_id);
    //x-dbg/ DBGFN("starting squark [", db_path, "]");
    // Execute squark subprocess.
    rio_t *stdin_pipe_r, *stdin_pipe_w;
    rio_realloc_split(rio_open_pipe(), &stdin_pipe_r, &stdin_pipe_w);
    rio_t *stdout_pipe_r, *stdout_pipe_w;
    rio_realloc_split(rio_open_pipe(), &stdout_pipe_r, &stdout_pipe_w);
    rio_sub_exec_t se = { .exec = {
        .path = rio_self_path,
        .args = new_list(fstr_t, "squark", db_path),
        .env = unix_env,
        .io_in = stdin_pipe_r,
        .io_out = stdout_pipe_w,
    }};
    rio_proc_t* proc_h = escape(rio_proc_execute(se));
    // Write schema.
    rio_write_fstr(stdin_pipe_w, fss(json_stringify(schema)));
    // Return handle to squark.
    lwt_heap_t* heap = lwt_alloc_heap();
    squark_t* sq;
    switch_heap (heap) {
        sq = new(squark_t);
        sq->is_dirty = false;
        sq->heap = heap;
        sq->proc = import(proc_h);
        fmitosis {
            sq->reader = spawn_fiber(squark_reader("", import(stdout_pipe_r)));
        }
        fmitosis {
            sq->writer = spawn_fiber(squark_writer("", import(stdin_pipe_w)));
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
    switch_heap (sq->heap) {
        // Clean up fibers first to avoid io exception.
        lwt_alloc_free(sq->watcher);
        lwt_alloc_free(sq->reader);
        // Now kill process.
        rio_proc_signal(sq->proc, SIGKILL);
        rio_proc_wait(sq->proc);
    }
    lwt_alloc_free(sq->heap);
}

fiber_main squark_barrier_fiber(fiber_main_attr, rcd_fid_t watcher_fid) { try {
    // Keep barrier fiber alive as long as squark is alive.
    ifc_wait(watcher_fid);
} catch (exception_desync, e); }

rcd_fid_t squark_op_barrier(squark_t* sq) { sub_heap {
    rcd_fid_t barrier_fid;
    fmitosis {
        rcd_fid_t watcher_fid = sfid(sq->watcher);
        barrier_fid = spawn_static_fiber(squark_barrier_fiber("", watcher_fid));
    }
    vec(fstr_t)* io_v = new_vec(fstr_t);
    rio_iov_write_u16(io_v, SQUARK_CMD_BARRIER);
    rio_iov_write_u128(io_v, barrier_fid);
    squark_write(io_v, sfid(sq->writer));
    return barrier_fid;
}}

void squark_op_insert(squark_t* sq, fstr_t map_id, fstr_t key, fstr_t value) { sub_heap {
    vec(fstr_t)* io_v = new_vec(fstr_t);
    rio_iov_write_u16(io_v, SQUARK_CMD_INSERT_IMM);
    rio_iov_write_fstr(io_v, map_id);
    rio_iov_write_fstr(io_v, key);
    rio_iov_write_fstr(io_v, value);
    squark_write(io_v, sfid(sq->writer));
}}

void squark_op_upsert(squark_t* sq, fstr_t map_id, fstr_t key, fstr_t value) { sub_heap {
    vec(fstr_t)* io_v = new_vec(fstr_t);
    rio_iov_write_u16(io_v, SQUARK_CMD_UPSERT);
    rio_iov_write_fstr(io_v, map_id);
    rio_iov_write_fstr(io_v, key);
    rio_iov_write_fstr(io_v, value);
    squark_write(io_v, sfid(sq->writer));
}}

void squark_op_perform(squark_t* sq, fstr_t op_arg) { sub_heap {
    vec(fstr_t)* io_v = new_vec(fstr_t);
    rio_iov_write_u16(io_v, SQUARK_CMD_PERFORM);
    rio_iov_write_fstr(io_v, op_arg);
    squark_write(io_v, sfid(sq->writer));
}}

join_locked(fstr_mem_t*) get_status_res(join_server_params, fstr_mem_t* status_res) {
    return import(status_res);
}

fiber_main status_op_fiber(fiber_main_attr) { try {
    fstr_mem_t* status_res;
    accept_join(has_status_res, join_server_params, &status_res);
    accept_join(get_status_res, join_server_params, status_res);
} catch (exception_desync, e); }

rcd_sub_fiber_t* squark_op_status(squark_t* sq) {
    fmitosis {
        sub_heap {
            vec(fstr_t)* io_v = new_vec(fstr_t);
            rio_iov_write_u16(io_v, SQUARK_CMD_STATUS);
            rio_iov_write_u128(io_v, new_fid);
            squark_write(io_v, sfid(sq->writer));
        }
        return spawn_fiber(status_op_fiber(""));
    }
}

fstr_mem_t* squark_get_status_res(rcd_fid_t status_fid) { sub_heap {
    try {
        return escape(get_status_res(status_fid));
    } catch (exception_inner_join_fail, e) {
        // Expected when squark is deleted.
        return escape(fstr_cpy(""));
    }
}}

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

rcd_sub_fiber_t* squark_op_scan(squark_t* sq, fstr_t map_id, qk_scan_op_t op) {
    fmitosis {
        // We could design this so the scan op fiber does the write asynchronously instead
        // but it's not necessary because deadlock is impossible anyway as the reader is never
        // really blocking on anything. Scan results are already passed asynchronously back.
        sub_heap {
            vec(fstr_t)* io_v = new_vec(fstr_t);
            rio_iov_write_u16(io_v, SQUARK_CMD_SCAN);
            rio_iov_write_fstr(io_v, map_id);
            rio_iov_write_u128(io_v, new_fid);
            vec_append(io_v, fstr_t, FSTR_PACK(op));
            if (op.with_start)
                rio_iov_write_fstr(io_v, op.key_start);
            if (op.with_end)
                rio_iov_write_fstr(io_v, op.key_end);
            //x-dbg/ DBGFN("written scan op");
            squark_write(io_v, sfid(sq->writer));
        }
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

bool squark_scan(squark_t* sq, fstr_t map_id, qk_scan_op_t op, fstr_mem_t** out_band, uint64_t* out_count) { sub_heap {
    rcd_sub_fiber_t* scan_sf = squark_op_scan(sq, map_id, op);
    bool eof;
    fstr_mem_t* band = squark_get_scan_res(sfid(scan_sf), out_count, &eof);
    //x-dbg/ DBGFN("got segment scan result count:[", (*out_count), "], eof:[", (eof? "t": "f"), "]");
    if (!eof) {
        // Out of band.
        if (out_count == 0) {
            throw("scanning data failed, out of squark band and no progress made (ent larger than band buffer)", exception_io);
        }
    }
    *out_band = escape(band);
    return eof;
}}

void squark_rm_index(fstr_t db_dir, fstr_t index_id) { sub_heap {
    fstr_t db_path = concs(db_dir, "/", index_id);
    fstr_t data_path = concs(db_path, ".data");
    fstr_t journal_path = concs(db_path, ".journal");
    // Due to race neither file may actually exist.
    // The squark subprocess creates the files asynchronously in respect to the squark spawn.
    // We want to delete the data file first. If only the journal is deleted the database
    // could be left in a silently corrupt state. If only the data is deleted we are
    // only leaking disk space in the worst case.
    if (rio_file_exists(data_path)) {
        rio_file_unlink(data_path);
    }
    if (rio_file_exists(journal_path)) {
        rio_file_unlink(journal_path);
    }
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
