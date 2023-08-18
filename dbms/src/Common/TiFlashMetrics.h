// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/TiFlashBuildInfo.h>
#include <Common/nocopyable.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <ext/scope_guard.h>


// to make GCC 11 happy
#include <cassert>

namespace DB
{
/// Central place to define metrics across all subsystems.
/// Refer to gtest_tiflash_metrics.cpp for more sample defines.
/// Usage:
/// GET_METRIC(tiflash_coprocessor_response_bytes).Increment(1);
/// GET_METRIC(tiflash_coprocessor_request_count, type_cop).Set(1);
/// Maintenance notes:
/// 1. Use same name prefix for metrics in same subsystem (coprocessor/schema/storage/raft/etc.).
/// 2. Keep metrics with same prefix next to each other.
/// 3. Add metrics of new subsystems at tail.
/// 4. Keep it proper formatted using clang-format.
// clang-format off
#define APPLY_FOR_METRICS(M, F)                                                                                                           \
    M(tiflash_coprocessor_request_count, "Total number of request", Counter, F(type_cop, {"type", "cop"}),                                \
        F(type_cop_executing, {"type", "cop_executing"}), F(type_batch, {"type", "batch"}),                                               \
        F(type_batch_executing, {"type", "batch_executing"}), F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}),                   \
        F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}), F(type_cancel_mpp_task, {"type", "cancel_mpp_task"}),                 \
        F(type_run_mpp_task, {"type", "run_mpp_task"}), F(type_remote_read, {"type", "remote_read"}),                                     \
        F(type_remote_read_constructed, {"type", "remote_read_constructed"}), F(type_remote_read_sent, {"type", "remote_read_sent"}))     \
    M(tiflash_coprocessor_handling_request_count, "Number of handling request", Gauge, F(type_cop, {"type", "cop"}),                      \
        F(type_cop_executing, {"type", "cop_executing"}), F(type_batch, {"type", "batch"}),                                               \
        F(type_batch_executing, {"type", "batch_executing"}), F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}),                   \
        F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}), F(type_cancel_mpp_task, {"type", "cancel_mpp_task"}),                 \
        F(type_run_mpp_task, {"type", "run_mpp_task"}), F(type_remote_read, {"type", "remote_read"}),                                     \
        F(type_remote_read_executing, {"type", "remote_read_executing"}))                                                                 \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, F(type_ts, {"type", "table_scan"}),                   \
        F(type_sel, {"type", "selection"}), F(type_agg, {"type", "aggregation"}), F(type_topn, {"type", "top_n"}),                        \
        F(type_limit, {"type", "limit"}), F(type_join, {"type", "join"}), F(type_exchange_sender, {"type", "exchange_sender"}),           \
        F(type_exchange_receiver, {"type", "exchange_receiver"}), F(type_projection, {"type", "projection"}),                             \
        F(type_partition_ts, {"type", "partition_table_scan"}),                                                                           \
        F(type_window, {"type", "window"}), F(type_window_sort, {"type", "window_sort"}))                                                 \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of request duration", Histogram,                                  \
        F(type_cop, {{"type", "cop"}}, ExpBuckets{0.001, 2, 20}),                                                                         \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
        F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                             \
        F(type_mpp_establish_conn, {{"type", "mpp_establish_conn"}}, ExpBuckets{0.001, 2, 20}),                                           \
        F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                                 \
        F(type_run_mpp_task, {{"type", "run_mpp_task"}}, ExpBuckets{0.001, 2, 20}))                                                       \
    M(tiflash_coprocessor_request_memory_usage, "Bucketed histogram of request memory usage", Histogram,                                  \
        F(type_cop, {{"type", "cop"}}, ExpBuckets{1024 * 1024, 2, 16}),                                                                   \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{1024 * 1024, 2, 20}),                                                               \
        F(type_run_mpp_task, {{"type", "run_mpp_task"}}, ExpBuckets{1024 * 1024, 2, 20}))                                                 \
    M(tiflash_coprocessor_request_error, "Total number of request error", Counter, F(reason_meet_lock, {"reason", "meet_lock"}),          \
        F(reason_region_not_found, {"reason", "region_not_found"}), F(reason_epoch_not_match, {"reason", "epoch_not_match"}),             \
        F(reason_kv_client_error, {"reason", "kv_client_error"}), F(reason_internal_error, {"reason", "internal_error"}),                 \
        F(reason_other_error, {"reason", "other_error"}))                                                                                 \
    M(tiflash_coprocessor_request_handle_seconds, "Bucketed histogram of request handle duration", Histogram,                             \
        F(type_cop, {{"type", "cop"}}, ExpBuckets{0.001, 2, 20}),                                                                         \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
        F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                             \
        F(type_mpp_establish_conn, {{"type", "mpp_establish_conn"}}, ExpBuckets{0.001, 2, 20}),                                           \
        F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                                 \
        F(type_run_mpp_task, {{"type", "run_mpp_task"}}, ExpBuckets{0.001, 2, 20}))                                                       \
    M(tiflash_coprocessor_response_bytes, "Total bytes of response body", Counter,                                                        \
        F(type_cop, {{"type", "cop"}}),                                                                                                   \
        F(type_batch_cop, {{"type", "batch_cop"}}),                                                                                       \
        F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}),                                                                       \
        F(type_mpp_establish_conn, {{"type", "mpp_tunnel"}}),                                                                             \
        F(type_mpp_establish_conn_local, {{"type", "mpp_tunnel_local"}}),                                                                 \
        F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}))                                                                           \
    M(tiflash_schema_version, "Current version of tiflash cached schema", Gauge)                                                          \
    M(tiflash_schema_applying, "Whether the schema is applying or not (holding lock)", Gauge)                                             \
    M(tiflash_schema_apply_count, "Total number of each kinds of apply", Counter, F(type_diff, {"type", "diff"}),                         \
        F(type_full, {"type", "full"}), F(type_failed, {"type", "failed"}))                                                               \
    M(tiflash_schema_trigger_count, "Total number of each kinds of schema sync trigger", Counter, /**/                                    \
        F(type_timer, {"type", "timer"}), F(type_raft_decode, {"type", "raft_decode"}), F(type_cop_read, {"type", "cop_read"}))           \
    M(tiflash_schema_internal_ddl_count, "Total number of each kinds of internal ddl operations", Counter,                                \
        F(type_create_table, {"type", "create_table"}), F(type_create_db, {"type", "create_db"}),                                         \
        F(type_drop_table, {"type", "drop_table"}), F(type_drop_db, {"type", "drop_db"}), F(type_rename_table, {"type", "rename_table"}), \
        F(type_add_column, {"type", "add_column"}), F(type_drop_column, {"type", "drop_column"}),                                         \
        F(type_alter_column_tp, {"type", "alter_column_type"}), F(type_rename_column, {"type", "rename_column"}),                         \
        F(type_exchange_partition, {"type", "exchange_partition"}))                                                                       \
    M(tiflash_schema_apply_duration_seconds, "Bucketed histogram of ddl apply duration", Histogram,                                       \
        F(type_ddl_apply_duration, {{"req", "ddl_apply_duration"}}, ExpBuckets{0.001, 2, 20}))                                            \
    M(tiflash_raft_read_index_count, "Total number of raft read index", Counter)                                                          \
    M(tiflash_raft_read_index_duration_seconds, "Bucketed histogram of raft read index duration", Histogram,                              \
        F(type_raft_read_index_duration, {{"type", "tmt_raft_read_index_duration"}}, ExpBuckets{0.001, 2, 20}))                           \
    M(tiflash_raft_wait_index_duration_seconds, "Bucketed histogram of raft wait index duration", Histogram,                              \
        F(type_raft_wait_index_duration, {{"type", "tmt_raft_wait_index_duration"}}, ExpBuckets{0.001, 2, 20}))                           \
    M(tiflash_syncing_data_freshness, "The freshness of tiflash data with tikv data", Histogram,                                          \
        F(type_syncing_data_freshness, {{"type", "data_freshness"}}, ExpBuckets{0.001, 2, 20}))                                           \
    M(tiflash_storage_read_tasks_count, "Total number of storage engine read tasks", Counter)                                             \
    M(tiflash_storage_command_count, "Total number of storage's command, such as delete range / shutdown /startup", Counter,              \
        F(type_delete_range, {"type", "delete_range"}), F(type_ingest, {"type", "ingest"}))                                               \
    M(tiflash_storage_subtask_count, "Total number of storage's sub task", Counter,                                                       \
        F(type_delta_merge_bg, {"type", "delta_merge_bg"}),                                                                               \
        F(type_delta_merge_bg_gc, {"type", "delta_merge_bg_gc"}),                                                                         \
        F(type_delta_merge_fg, {"type", "delta_merge_fg"}),                                                                               \
        F(type_delta_merge_manual, {"type", "delta_merge_manual"}),                                                                       \
        F(type_delta_compact, {"type", "delta_compact"}),                                                                                 \
        F(type_delta_flush, {"type", "delta_flush"}),                                                                                     \
        F(type_seg_split_bg, {"type", "seg_split_bg"}),                                                                                   \
        F(type_seg_split_fg, {"type", "seg_split_fg"}),                                                                                   \
        F(type_seg_split_ingest, {"type", "seg_split_ingest"}),                                                                           \
        F(type_seg_merge_bg_gc, {"type", "seg_merge_bg_gc"}),                                                                             \
        F(type_place_index_update, {"type", "place_index_update"}))                                                                       \
    M(tiflash_storage_subtask_duration_seconds, "Bucketed histogram of storage's sub task duration", Histogram,                           \
        F(type_delta_merge_bg, {{"type", "delta_merge_bg"}}, ExpBuckets{0.001, 2, 20}),                                                   \
        F(type_delta_merge_bg_gc, {{"type", "delta_merge_bg_gc"}}, ExpBuckets{0.001, 2, 20}),                                             \
        F(type_delta_merge_fg, {{"type", "delta_merge_fg"}}, ExpBuckets{0.001, 2, 20}),                                                   \
        F(type_delta_merge_manual, {{"type", "delta_merge_manual"}}, ExpBuckets{0.001, 2, 20}),                                           \
        F(type_delta_compact, {{"type", "delta_compact"}}, ExpBuckets{0.001, 2, 20}),                                                     \
        F(type_delta_flush, {{"type", "delta_flush"}}, ExpBuckets{0.001, 2, 20}),                                                         \
        F(type_seg_split_bg, {{"type", "seg_split_bg"}}, ExpBuckets{0.001, 2, 20}),                                                       \
        F(type_seg_split_fg, {{"type", "seg_split_fg"}}, ExpBuckets{0.001, 2, 20}),                                                       \
        F(type_seg_split_ingest, {{"type", "seg_split_ingest"}}, ExpBuckets{0.001, 2, 20}),                                               \
        F(type_seg_merge_bg_gc, {{"type", "seg_merge_bg_gc"}}, ExpBuckets{0.001, 2, 20}),                                                 \
        F(type_place_index_update, {{"type", "place_index_update"}}, ExpBuckets{0.001, 2, 20}))                                           \
    M(tiflash_storage_throughput_bytes, "Calculate the throughput of tasks of storage in bytes", Gauge,           /**/                    \
        F(type_write, {"type", "write"}),                                                                         /**/                    \
        F(type_ingest, {"type", "ingest"}),                                                                       /**/                    \
        F(type_delta_merge, {"type", "delta_merge"}),                                                             /**/                    \
        F(type_split, {"type", "split"}),                                                                         /**/                    \
        F(type_merge, {"type", "merge"}))                                                                         /**/                    \
    M(tiflash_storage_throughput_rows, "Calculate the throughput of tasks of storage in rows", Gauge,             /**/                    \
        F(type_write, {"type", "write"}),                                                                         /**/                    \
        F(type_ingest, {"type", "ingest"}),                                                                       /**/                    \
        F(type_delta_merge, {"type", "delta_merge"}),                                                             /**/                    \
        F(type_split, {"type", "split"}),                                                                         /**/                    \
        F(type_merge, {"type", "merge"}))                                                                         /**/                    \
    M(tiflash_storage_write_stall_duration_seconds, "The write stall duration of storage, in seconds", Histogram,                         \
        F(type_write, {{"type", "write"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
        F(type_delta_merge_by_write, {{"type", "delta_merge_by_write"}}, ExpBuckets{0.001, 2, 20}),                                       \
        F(type_delta_merge_by_delete_range, {{"type", "delta_merge_by_delete_range"}}, ExpBuckets{0.001, 2, 20}),                         \
        F(type_flush, {{"type", "flush"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
        F(type_split, {{"type", "split"}}, ExpBuckets{0.001, 2, 20}))                                                                     \
    M(tiflash_storage_page_gc_count, "Total number of page's gc execution.", Counter,                                                     \
        F(type_v2, {"type", "v2"}),                                                                                                       \
        F(type_v2_low, {"type", "v2_low"}),                                                                                               \
        F(type_v3, {"type", "v3"}),                                                                                                       \
        F(type_v3_mvcc_dumped, {"type", "v3_mvcc_dumped"}),                                                                               \
        F(type_v3_bs_full_gc, {"type", "v3_bs_full_gc"}))                                                                                 \
    M(tiflash_storage_page_gc_duration_seconds, "Bucketed histogram of page's gc task duration", Histogram,                               \
        F(type_v2, {{"type", "v2"}}, ExpBuckets{0.0005, 2, 20}),                                                                          \
        F(type_v2_data_compact, {{"type", "v2_data_compact"}}, ExpBuckets{0.0005, 2, 20}),                                                \
        F(type_v2_ver_compact, {{"type", "v2_ver_compact"}}, ExpBuckets{0.0005, 2, 20}),                                                  \
        /* Below are metrics for PageStorage V3 */                                                                                        \
        F(type_compact_wal, {{"type", "compact_wal"}},             ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_compact_directory, {{"type", "compact_directory"}}, ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_compact_spacemap, {{"type", "compact_spacemap"}},   ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_fullgc_rewrite, {{"type", "fullgc_rewrite"}},       ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_fullgc_commit, {{"type", "fullgc_commit"}},         ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_clean_external, {{"type", "clean_external"}},       ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_v3, {{"type", "v3"}}, ExpBuckets{0.0005, 2, 20}))                                                                          \
    M(tiflash_storage_page_command_count, "Total number of PageStorage's command, such as write / read / scan / snapshot", Counter,       \
        F(type_write, {"type", "write"}), F(type_read, {"type", "read"}),  F(type_read_page_dir, {"type", "read_page_dir"}),              \
        F(type_read_blob, {"type", "read_blob"}), F(type_scan, {"type", "scan"}), F(type_snapshot, {"type", "snapshot"}))                 \
    M(tiflash_storage_page_write_batch_size, "The size of each write batch in bytes", Histogram,                                          \
        F(type_v3, {{"type", "v3"}}, ExpBuckets{4 * 1024, 4, 10}))                                                                        \
    M(tiflash_storage_page_write_duration_seconds, "The duration of each write batch", Histogram,                                         \
        F(type_total, {{"type", "total"}}, ExpBuckets{0.0001, 2, 20}),                                                                    \
        /* the bucket range for apply in memory is 50us ~ 120s */                                                                         \
        F(type_choose_stat, {{"type", "choose_stat"}}, ExpBuckets{0.00005, 1.8, 26}),                                                     \
        F(type_search_pos,  {{"type", "search_pos"}},  ExpBuckets{0.00005, 1.8, 26}),                                                     \
        F(type_blob_write,  {{"type", "blob_write"}},  ExpBuckets{0.00005, 1.8, 26}),                                                     \
        F(type_latch,       {{"type", "latch"}},       ExpBuckets{0.00005, 1.8, 26}),                                                     \
        F(type_wait_in_group, {{"type", "wait_in_group"}}, ExpBuckets{0.00005, 1.8, 26}),                                                 \
        F(type_wal,         {{"type", "wal"}},         ExpBuckets{0.00005, 1.8, 26}),                                                     \
        F(type_commit,      {{"type", "commit"}},      ExpBuckets{0.00005, 1.8, 26}))                                                     \
    M(tiflash_storage_logical_throughput_bytes, "The logical throughput of read tasks of storage in bytes", Histogram,                    \
        F(type_read, {{"type", "read"}}, EqualWidthBuckets{1 * 1024 * 1024, 60, 50 * 1024 * 1024}))                                       \
    M(tiflash_storage_io_limiter, "Storage I/O limiter metrics", Counter, F(type_fg_read_req_bytes, {"type", "fg_read_req_bytes"}),       \
        F(type_fg_read_alloc_bytes, {"type", "fg_read_alloc_bytes"}), F(type_bg_read_req_bytes, {"type", "bg_read_req_bytes"}),           \
        F(type_bg_read_alloc_bytes, {"type", "bg_read_alloc_bytes"}), F(type_fg_write_req_bytes, {"type", "fg_write_req_bytes"}),         \
        F(type_fg_write_alloc_bytes, {"type", "fg_write_alloc_bytes"}), F(type_bg_write_req_bytes, {"type", "bg_write_req_bytes"}),       \
        F(type_bg_write_alloc_bytes, {"type", "bg_write_alloc_bytes"}))                                                                   \
    M(tiflash_storage_rough_set_filter_rate, "Bucketed histogram of rough set filter rate", Histogram,                                    \
        F(type_dtfile_pack, {{"type", "dtfile_pack"}}, EqualWidthBuckets{0, 6, 20}))                                                      \
    M(tiflash_raft_command_duration_seconds, "Bucketed histogram of some raft command: apply snapshot",                                   \
        Histogram, /* these command usually cost servel seconds, increase the start bucket to 50ms */                                     \
        F(type_ingest_sst, {{"type", "ingest_sst"}}, ExpBuckets{0.05, 2, 10}),                                                            \
        F(type_apply_snapshot_predecode, {{"type", "snapshot_predecode"}}, ExpBuckets{0.05, 2, 10}),                                      \
        F(type_apply_snapshot_predecode_sst2dt, {{"type", "snapshot_predecode_sst2dt"}}, ExpBuckets{0.05, 2, 10}),                        \
        F(type_apply_snapshot_flush, {{"type", "snapshot_flush"}}, ExpBuckets{0.05, 2, 10}))                                              \
    M(tiflash_raft_process_keys, "Total number of keys processed in some types of Raft commands", Counter,                                \
        F(type_apply_snapshot, {"type", "apply_snapshot"}), F(type_ingest_sst, {"type", "ingest_sst"}))                                   \
    M(tiflash_raft_apply_write_command_duration_seconds, "Bucketed histogram of applying write command Raft logs", Histogram,             \
        F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}),                                                                    \
        F(type_admin, {{"type", "admin"}}, ExpBuckets{0.0005, 2, 20}),                                                                    \
        F(type_flush_region, {{"type", "flush_region"}}, ExpBuckets{0.0005, 2, 20}))                                                      \
    M(tiflash_raft_upstream_latency, "The latency that tikv sends raft log to tiflash.", Histogram,                                       \
        F(type_write, {{"type", "write"}}, ExpBuckets{0.001, 2, 30}))                                                                     \
    M(tiflash_raft_write_data_to_storage_duration_seconds, "Bucketed histogram of writting region into storage layer", Histogram,         \
        F(type_decode, {{"type", "decode"}}, ExpBuckets{0.0005, 2, 20}), F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}))   \
    /* required by DBaaS */                                                                                                               \
    M(tiflash_server_info, "Indicate the tiflash server info, and the value is the start timestamp (s).", Gauge,                          \
        F(start_time, {"version", TiFlashBuildInfo::getReleaseVersion()}, {"hash", TiFlashBuildInfo::getGitHash()}))                      \
    M(tiflash_object_count, "Number of objects", Gauge,                                                                                   \
        F(type_count_of_establish_calldata, {"type", "count_of_establish_calldata"}),                                                     \
        F(type_count_of_mpptunnel, {"type", "count_of_mpptunnel"}))                                                                       \
    M(tiflash_thread_count, "Number of threads", Gauge,                                                                                   \
        F(type_max_threads_of_thdpool, {"type", "thread_pool_total_max"}),                                                                \
        F(type_active_threads_of_thdpool, {"type", "thread_pool_active"}),                                                                \
        F(type_max_active_threads_of_thdpool, {"type", "thread_pool_active_max"}),                                                        \
        F(type_total_threads_of_thdpool, {"type", "thread_pool_total"}),                                                                  \
        F(type_max_threads_of_raw, {"type", "total_max"}),                                                                                \
        F(type_total_threads_of_raw, {"type", "total"}),                                                                                  \
        F(type_threads_of_client_cq_pool, {"type", "rpc_client_cq_pool"}),                                                                \
        F(type_threads_of_receiver_read_loop, {"type", "rpc_receiver_read_loop"}),                                                        \
        F(type_threads_of_receiver_reactor, {"type", "rpc_receiver_reactor"}),                                                            \
        F(type_max_threads_of_establish_mpp, {"type", "rpc_establish_mpp_max"}),                                                          \
        F(type_active_threads_of_establish_mpp, {"type", "rpc_establish_mpp"}),                                                           \
        F(type_max_threads_of_dispatch_mpp, {"type", "rpc_dispatch_mpp_max"}),                                                            \
        F(type_active_threads_of_dispatch_mpp, {"type", "rpc_dispatch_mpp"}),                                                             \
        F(type_active_rpc_async_worker, {"type", "rpc_async_worker_active"}),                                                             \
        F(type_total_rpc_async_worker, {"type", "rpc_async_worker_total"}))                                                               \
    M(tiflash_task_scheduler, "Min-tso task scheduler", Gauge,                                                                            \
        F(type_min_tso, {"type", "min_tso"}),                                                                                             \
        F(type_waiting_queries_count, {"type", "waiting_queries_count"}),                                                                 \
        F(type_active_queries_count, {"type", "active_queries_count"}),                                                                   \
        F(type_waiting_tasks_count, {"type", "waiting_tasks_count"}),                                                                     \
        F(type_active_tasks_count, {"type", "active_tasks_count"}),                                                                       \
        F(type_estimated_thread_usage, {"type", "estimated_thread_usage"}),                                                               \
        F(type_thread_soft_limit, {"type", "thread_soft_limit"}),                                                                         \
        F(type_thread_hard_limit, {"type", "thread_hard_limit"}),                                                                         \
        F(type_hard_limit_exceeded_count, {"type", "hard_limit_exceeded_count"}))                                                         \
    M(tiflash_task_scheduler_waiting_duration_seconds, "Bucketed histogram of task waiting for scheduling duration", Histogram,           \
        F(type_task_scheduler_waiting_duration, {{"type", "task_waiting_duration"}}, ExpBuckets{0.001, 2, 20}))                           \
    M(tiflash_storage_read_thread_counter, "The counter of storage read thread", Counter,                                                 \
        F(type_sche_no_pool, {"type", "sche_no_pool"}),                                                                                   \
        F(type_sche_no_slot, {"type", "sche_no_slot"}),                                                                                   \
        F(type_sche_no_segment, {"type", "sche_no_segment"}),                                                                             \
        F(type_sche_from_cache, {"type", "sche_from_cache"}),                                                                             \
        F(type_sche_new_task, {"type", "sche_new_task"}),                                                                                 \
        F(type_add_cache_succ, {"type", "add_cache_succ"}),                                                                               \
        F(type_add_cache_stale, {"type", "add_cache_stale"}),                                                                             \
        F(type_get_cache_miss, {"type", "get_cache_miss"}),                                                                               \
        F(type_get_cache_part, {"type", "get_cache_part"}),                                                                               \
        F(type_get_cache_hit, {"type", "get_cache_hit"}),                                                                                 \
        F(type_get_cache_copy, {"type", "get_cache_copy"}))                                                                               \
    M(tiflash_storage_read_thread_gauge, "The gauge of storage read thread", Gauge,                                                       \
        F(type_merged_task, {"type", "merged_task"}))                                                                                     \
    M(tiflash_storage_read_thread_seconds, "Bucketed histogram of read thread", Histogram,                                                \
        F(type_merged_task, {{"type", "merged_task"}}, ExpBuckets{0.001, 2, 20}))                                                         \
    M(tiflash_mpp_task_manager, "The gauge of mpp task manager", Gauge,                                                                   \
        F(type_mpp_query_count, {"type", "mpp_query_count"}))

// clang-format on

/// Buckets with boundaries [start * base^0, start * base^1, ..., start * base^(size-1)]
struct ExpBuckets
{
    const double start;
    const double base;
    const size_t size;

    // NOLINTNEXTLINE(google-explicit-constructor)
    inline operator prometheus::Histogram::BucketBoundaries() const &&
    {
        prometheus::Histogram::BucketBoundaries buckets(size);
        double current = start;
        std::for_each(buckets.begin(), buckets.end(), [&](auto & e) {
            e = current;
            current *= base;
        });
        return buckets;
    }
};

// Buckets with same width
struct EqualWidthBuckets
{
    const size_t start;
    const int num_buckets;
    const size_t step;

    // NOLINTNEXTLINE(google-explicit-constructor)
    inline operator prometheus::Histogram::BucketBoundaries() const &&
    {
        // up to `num_buckets` * `step`
        assert(step > 1);
        prometheus::Histogram::BucketBoundaries buckets(num_buckets);
        size_t idx = 0;
        for (auto & e : buckets)
        {
            e = start + step * idx;
            idx++;
        }
        return buckets;
    }
};

template <typename T>
struct MetricFamilyTrait
{
};
template <>
struct MetricFamilyTrait<prometheus::Counter>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildCounter(); }
    static auto & add(prometheus::Family<prometheus::Counter> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
    static auto & add(prometheus::Family<prometheus::Gauge> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Histogram>
{
    using ArgType = std::tuple<std::map<std::string, std::string>, prometheus::Histogram::BucketBoundaries>;
    static auto build() { return prometheus::BuildHistogram(); }
    static auto & add(prometheus::Family<prometheus::Histogram> & family, ArgType && arg)
    {
        return family.Add(std::move(std::get<0>(arg)), std::move(std::get<1>(arg)));
    }
};

template <typename T>
struct MetricFamily
{
    using MetricTrait = MetricFamilyTrait<T>;
    using MetricArgType = typename MetricTrait::ArgType;

    MetricFamily(
        prometheus::Registry & registry,
        const std::string & name,
        const std::string & help,
        std::initializer_list<MetricArgType> args)
    {
        auto & family = MetricTrait::build().Name(name).Help(help).Register(registry);
        metrics.reserve(args.size() ? args.size() : 1);
        for (auto arg : args)
        {
            auto & metric = MetricTrait::add(family, std::forward<MetricArgType>(arg));
            metrics.emplace_back(&metric);
        }
        if (metrics.empty())
        {
            auto & metric = MetricTrait::add(family, MetricArgType{});
            metrics.emplace_back(&metric);
        }
    }

    T & get(size_t idx = 0) { return *(metrics[idx]); }

private:
    std::vector<T *> metrics;
};

/// Centralized registry of TiFlash metrics.
/// Cope with MetricsPrometheus by registering
/// profile events, current metrics and customized metrics (as individual member for caller to access) into registry ahead of being updated.
/// Asynchronous metrics will be however registered by MetricsPrometheus itself due to the life cycle difference.
class TiFlashMetrics
{
public:
    static TiFlashMetrics & instance();

private:
    TiFlashMetrics();

    static constexpr auto profile_events_prefix = "tiflash_system_profile_event_";
    static constexpr auto current_metrics_prefix = "tiflash_system_current_metric_";
    static constexpr auto async_metrics_prefix = "tiflash_system_asynchronous_metric_";

    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

    std::vector<prometheus::Gauge *> registered_profile_events;
    std::vector<prometheus::Gauge *> registered_current_metrics;
    std::unordered_map<std::string, prometheus::Gauge *> registered_async_metrics;

public:
#define MAKE_METRIC_MEMBER_M(family_name, help, type, ...) \
    MetricFamily<prometheus::type> family_name = MetricFamily<prometheus::type>(*registry, #family_name, #help, {__VA_ARGS__});
#define MAKE_METRIC_MEMBER_F(field_name, ...) \
    {                                         \
        __VA_ARGS__                           \
    }
    APPLY_FOR_METRICS(MAKE_METRIC_MEMBER_M, MAKE_METRIC_MEMBER_F)

    DISALLOW_COPY_AND_MOVE(TiFlashMetrics);

    friend class MetricsPrometheus;
};

#define MAKE_METRIC_ENUM_M(family_name, help, type, ...) \
    namespace family_name##_metrics                      \
    {                                                    \
        enum                                             \
        {                                                \
            invalid = -1,                                \
            ##__VA_ARGS__                                \
        };                                               \
    }
#define MAKE_METRIC_ENUM_F(field_name, ...) field_name
APPLY_FOR_METRICS(MAKE_METRIC_ENUM_M, MAKE_METRIC_ENUM_F)
#undef APPLY_FOR_METRICS

// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_MACRO(_1, _2, NAME, ...) NAME
#ifndef GTEST_TIFLASH_METRICS
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_0(family) TiFlashMetrics::instance().family.get()
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_1(family, metric) TiFlashMetrics::instance().family.get(family##_metrics::metric)
#else
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_0(family) TestMetrics::instance().family.get()
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_1(family, metric) TestMetrics::instance().family.get(family##_metrics::metric)
#endif
#define GET_METRIC(...)                                             \
    __GET_METRIC_MACRO(__VA_ARGS__, __GET_METRIC_1, __GET_METRIC_0) \
    (__VA_ARGS__)

#define UPDATE_CUR_AND_MAX_METRIC(family, metric, metric_max)                                                                 \
    GET_METRIC(family, metric).Increment();                                                                                   \
    GET_METRIC(family, metric_max).Set(std::max(GET_METRIC(family, metric_max).Value(), GET_METRIC(family, metric).Value())); \
    SCOPE_EXIT({                                                                                                              \
        GET_METRIC(family, metric).Decrement();                                                                               \
    })
} // namespace DB
