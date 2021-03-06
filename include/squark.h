/*
 * Helper library for running multiple quarks in subprocesses.
 */

#ifndef SQUARK_H
#define SQUARK_H

#include "quark.h"

#define SQUARK_SCAN(SQUARK, INIT_OP, MAP_ID, KEY_NAME, VALUE_NAME) \
    _QUARK_SCAN(INIT_OP, KEY_NAME, VALUE_NAME, \
        LET(fstr_mem_t* _band_mem = 0), \
        lwt_alloc_free(_band_mem), \
        _eof = squark_scan(SQUARK, MAP_ID, _op, &_band_mem, &_count), \
        fss(_band_mem) \
    )

typedef struct squark {
    bool is_dirty;
    lwt_heap_t* heap;
    rio_proc_t* proc;
    rcd_sub_fiber_t* reader;
    rcd_sub_fiber_t* writer;
    rcd_sub_fiber_t* watcher;
} squark_t;

dict(qk_map_ctx_t);

/// Implement this function to initialize necessary context to any callback functions like squark_cb_perform().
/// Any memory leaked from the function will persist for as long as the squark is running.
/// The returned pointer will be passed as ctx_ptr to callback functions.
/// When this function is not implemented the ctx_ptr will be 0.
void* squark_cb_init_ctx(acid_h* ah, qk_ctx_t* qk, dict(qk_map_ctx_t*)* maps);

/// Implement this function to use with squark_op_perform().
void squark_cb_perform(void* ctx_ptr, fstr_t op_arg);

/// Squark main. Should be called from program main.
/// Will not return if main arguments indicate a squark spawn, i.e. first argument is "squark".
/// May thrown exceptions during normal squark lifecycle.
void squark_main(list(fstr_t)* main_args, list(fstr_t)* main_env);

/// Spawns a new squark. Invokes the own process with first argument "squark" plus additional arguments.
/// The schema maps map ids (to be created/initialized) to configuration objects.
/// Example schema: {
///     "foo": {"ipp": 40},
///     "bar": {"ipp": 200},
/// }
squark_t* squark_spawn(fstr_t db_dir, fstr_t index_id, json_value_t schema, list(fstr_t)* unix_env);

/// Kills a running squark and frees associated resources. Does not wait for sync.
void squark_kill(squark_t* sq);

/// Used to throttle insert to disk speed and ensure persistence to external systems.
/// Returns a static fid that is exited with the guarantee that sync completed.
/// This call will uninterruptibly block if pipe is full.
rcd_fid_t squark_op_barrier(squark_t* sq);

/// Inserts an element. Buffers data in the squark pipe without waiting for reply.
/// This call will uninterruptibly block if pipe is full.
void squark_op_insert(squark_t* sq, fstr_t map_id, fstr_t key, fstr_t value);

/// Upserts an element. Buffers data in the squark pipe without waiting for reply.
/// This call will uninterruptibly block if pipe is full.
void squark_op_upsert(squark_t* sq, fstr_t map_id, fstr_t key, fstr_t value);

/// Performs an abstract operation. Calls the custom implementation of squark_cb_perform().
/// Buffers data in the squark pipe without waiting for reply.
/// This call will uninterruptibly block if pipe is full.
void squark_op_perform(squark_t* sq, fstr_t op_arg);

/// Starts an asynchronous status operation. Call squark_get_scan_res() with returned
/// fiber id to block while waiting for the result.
/// This call will uninterruptibly block if pipe is full.
rcd_sub_fiber_t* squark_op_status(squark_t* sq);

/// Returns the status result from a squark_op_status() operation.
/// The returned string is a json structured returned from qk_get_stats().
/// Killing the squark while calling this function is fine.
/// In this situation the function will stop blocking and return an empty string.
fstr_mem_t* squark_get_status_res(rcd_fid_t scan_fid);

/// Starts an asynchronous scan operation. Call squark_get_scan_res() with returned
/// fiber id to block while waiting for the result.
/// This call will uninterruptibly block if pipe is full.
rcd_sub_fiber_t* squark_op_scan(squark_t* sq, fstr_t map_id, qk_scan_op_t op);

/// Returns the scan result from a squark_op_scan() operation.
/// Killing the squark while calling this function is fine.
/// In this situation the function will stop blocking and return an empty string.
fstr_mem_t* squark_get_scan_res(rcd_fid_t scan_fid, uint64_t* out_count, bool* out_eof);

/// More compact squark scan that sends operation and synchronously waits for result.
/// Returns end of file (true when end of file is reached).
bool squark_scan(squark_t* sq, fstr_t map_id, qk_scan_op_t op, fstr_mem_t** out_band, uint64_t* out_count);

/// Removes a squark index permanently.
void squark_rm_index(fstr_t db_dir, fstr_t index_id);

/// Lists all squark indexes in the database directory.
/// Returns a list of index_id's.
list(fstr_t)* squark_get_indexes(fstr_t db_dir);

#endif /* SQUARK_H */
