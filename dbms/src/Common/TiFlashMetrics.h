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

#include <Common/ComputeLabelHolder.h>
#include <Common/ProcessCollector.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/nocopyable.h>
#include <common/types.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <ext/scope_guard.h>
#include <mutex>
#include <shared_mutex>

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
#define APPLY_FOR_METRICS(M, F)                                                                                                     \
    M(tiflash_coprocessor_request_count,                                                                                            \
      "Total number of request",                                                                                                    \
      Counter,                                                                                                                      \
      F(type_cop, {"type", "cop"}),                                                                                                 \
      F(type_cop_executing, {"type", "cop_executing"}),                                                                             \
      F(type_cop_stream, {"type", "cop_stream"}),                                                                                   \
      F(type_cop_stream_executing, {"type", "cop_stream_executing"}),                                                               \
      F(type_batch, {"type", "batch"}),                                                                                             \
      F(type_batch_executing, {"type", "batch_executing"}),                                                                         \
      F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}),                                                                     \
      F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}),                                                                   \
      F(type_cancel_mpp_task, {"type", "cancel_mpp_task"}),                                                                         \
      F(type_run_mpp_task, {"type", "run_mpp_task"}),                                                                               \
      F(type_remote_read, {"type", "remote_read"}),                                                                                 \
      F(type_remote_read_constructed, {"type", "remote_read_constructed"}),                                                         \
      F(type_remote_read_sent, {"type", "remote_read_sent"}),                                                                       \
      F(type_disagg_establish_task, {"type", "disagg_establish_task"}),                                                             \
      F(type_disagg_fetch_pages, {"type", "disagg_fetch_pages"}))                                                                   \
    M(tiflash_coprocessor_handling_request_count,                                                                                   \
      "Number of handling request",                                                                                                 \
      Gauge,                                                                                                                        \
      F(type_cop, {"type", "cop"}),                                                                                                 \
      F(type_cop_executing, {"type", "cop_executing"}),                                                                             \
      F(type_cop_stream, {"type", "cop_stream"}),                                                                                   \
      F(type_cop_stream_executing, {"type", "cop_stream_executing"}),                                                               \
      F(type_batch, {"type", "batch"}),                                                                                             \
      F(type_batch_executing, {"type", "batch_executing"}),                                                                         \
      F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}),                                                                     \
      F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}),                                                                   \
      F(type_cancel_mpp_task, {"type", "cancel_mpp_task"}),                                                                         \
      F(type_run_mpp_task, {"type", "run_mpp_task"}),                                                                               \
      F(type_remote_read, {"type", "remote_read"}),                                                                                 \
      F(type_remote_read_executing, {"type", "remote_read_executing"}),                                                             \
      F(type_disagg_establish_task, {"type", "disagg_establish_task"}),                                                             \
      F(type_disagg_fetch_pages, {"type", "disagg_fetch_pages"}))                                                                   \
    M(tiflash_coprocessor_executor_count,                                                                                           \
      "Total number of each executor",                                                                                              \
      Counter,                                                                                                                      \
      F(type_ts, {"type", "table_scan"}),                                                                                           \
      F(type_sel, {"type", "selection"}),                                                                                           \
      F(type_agg, {"type", "aggregation"}),                                                                                         \
      F(type_topn, {"type", "top_n"}),                                                                                              \
      F(type_limit, {"type", "limit"}),                                                                                             \
      F(type_join, {"type", "join"}),                                                                                               \
      F(type_exchange_sender, {"type", "exchange_sender"}),                                                                         \
      F(type_exchange_receiver, {"type", "exchange_receiver"}),                                                                     \
      F(type_projection, {"type", "projection"}),                                                                                   \
      F(type_partition_ts, {"type", "partition_table_scan"}),                                                                       \
      F(type_window, {"type", "window"}),                                                                                           \
      F(type_window_sort, {"type", "window_sort"}),                                                                                 \
      F(type_expand, {"type", "expand"}))                                                                                           \
    M(tiflash_memory_exceed_quota_count, "Total number of cases where memory exceeds quota", Counter)                               \
    M(tiflash_coprocessor_request_duration_seconds,                                                                                 \
      "Bucketed histogram of request duration",                                                                                     \
      Histogram,                                                                                                                    \
      F(type_cop, {{"type", "cop"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
      F(type_cop_stream, {{"type", "cop_stream"}}, ExpBuckets{0.001, 2, 20}),                                                       \
      F(type_batch, {{"type", "batch"}}, ExpBuckets{0.001, 2, 20}),                                                                 \
      F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                         \
      F(type_mpp_establish_conn, {{"type", "mpp_establish_conn"}}, ExpBuckets{0.001, 2, 20}),                                       \
      F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                             \
      F(type_run_mpp_task, {{"type", "run_mpp_task"}}, ExpBuckets{0.001, 2, 20}),                                                   \
      F(type_disagg_establish_task, {{"type", "disagg_establish_task"}}, ExpBuckets{0.001, 2, 20}),                                 \
      F(type_disagg_fetch_pages, {{"type", "type_disagg_fetch_pages"}}, ExpBuckets{0.001, 2, 20}))                                  \
    M(tiflash_coprocessor_request_memory_usage,                                                                                     \
      "Bucketed histogram of request memory usage",                                                                                 \
      Histogram,                                                                                                                    \
      F(type_cop, {{"type", "cop"}}, ExpBuckets{1024 * 1024, 2, 16}),                                                               \
      F(type_cop_stream, {{"type", "cop_stream"}}, ExpBuckets{1024 * 1024, 2, 16}),                                                 \
      F(type_batch, {{"type", "batch"}}, ExpBuckets{1024 * 1024, 2, 20}),                                                           \
      F(type_run_mpp_task, {{"type", "run_mpp_task"}}, ExpBuckets{1024 * 1024, 2, 20}),                                             \
      F(type_run_mpp_query, {{"type", "run_mpp_query"}}, ExpBuckets{1024 * 1024, 2, 20}))                                           \
    M(tiflash_coprocessor_request_error,                                                                                            \
      "Total number of request error",                                                                                              \
      Counter,                                                                                                                      \
      F(reason_meet_lock, {"reason", "meet_lock"}),                                                                                 \
      F(reason_region_not_found, {"reason", "region_not_found"}),                                                                   \
      F(reason_epoch_not_match, {"reason", "epoch_not_match"}),                                                                     \
      F(reason_kv_client_error, {"reason", "kv_client_error"}),                                                                     \
      F(reason_internal_error, {"reason", "internal_error"}),                                                                       \
      F(reason_other_error, {"reason", "other_error"}))                                                                             \
    M(tiflash_coprocessor_request_handle_seconds,                                                                                   \
      "Bucketed histogram of request handle duration",                                                                              \
      Histogram,                                                                                                                    \
      F(type_cop, {{"type", "cop"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
      F(type_cop_stream, {{"type", "cop_stream"}}, ExpBuckets{0.001, 2, 20}),                                                       \
      F(type_batch, {{"type", "batch"}}, ExpBuckets{0.001, 2, 20}))                                                                 \
    M(tiflash_coprocessor_response_bytes,                                                                                           \
      "Total bytes of response body",                                                                                               \
      Counter,                                                                                                                      \
      F(type_cop, {{"type", "cop"}}),                                                                                               \
      F(type_cop_stream, {{"type", "cop_stream"}}),                                                                                 \
      F(type_batch_cop, {{"type", "batch_cop"}}),                                                                                   \
      F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}),                                                                   \
      F(type_mpp_establish_conn, {{"type", "mpp_tunnel"}}),                                                                         \
      F(type_mpp_establish_conn_local, {{"type", "mpp_tunnel_local"}}),                                                             \
      F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}),                                                                       \
      F(type_disagg_establish_task, {{"type", "type_disagg_establish_task"}}))                                                      \
    M(tiflash_exchange_data_bytes,                                                                                                  \
      "Total bytes sent by exchange operators",                                                                                     \
      Counter,                                                                                                                      \
      F(type_hash_original, {"type", "hash_original"}),                                                                             \
      F(type_hash_none_compression_remote, {"type", "hash_none_compression_remote"}),                                               \
      F(type_hash_none_compression_local, {"type", "hash_none_compression_local"}),                                                 \
      F(type_hash_lz4_compression, {"type", "hash_lz4_compression"}),                                                               \
      F(type_hash_zstd_compression, {"type", "hash_zstd_compression"}),                                                             \
      F(type_broadcast_original, {"type", "broadcast_original"}),                                                                   \
      F(type_broadcast_none_compression_local, {"type", "broadcast_none_compression_local"}),                                       \
      F(type_broadcast_none_compression_remote, {"type", "broadcast_none_compression_remote"}),                                     \
      F(type_broadcast_lz4_compression, {"type", "broadcast_lz4_compression"}),                                                     \
      F(type_broadcast_zstd_compression, {"type", "broadcast_zstd_compression"}),                                                   \
      F(type_passthrough_original, {"type", "passthrough_original"}),                                                               \
      F(type_passthrough_none_compression_local, {"type", "passthrough_none_compression_local"}),                                   \
      F(type_passthrough_none_compression_remote, {"type", "passthrough_none_compression_remote"}),                                 \
      F(type_passthrough_lz4_compression, {"type", "passthrough_lz4_compression"}),                                                 \
      F(type_passthrough_zstd_compression, {"type", "passthrough_zstd_compression"}))                                               \
    M(tiflash_sync_schema_applying, "Whether the schema is applying or not (holding lock)", Gauge)                                  \
    M(tiflash_schema_trigger_count,                                                                                                 \
      "Total number of each kinds of schema sync trigger",                                                                          \
      Counter,                                                                                                                      \
      F(type_timer, {"type", "timer"}),                                                                                             \
      F(type_raft_decode, {"type", "raft_decode"}),                                                                                 \
      F(type_cop_read, {"type", "cop_read"}),                                                                                       \
      F(type_sync_table_schema, {"type", "sync_table_schema"}))                                                                     \
    M(tiflash_schema_internal_ddl_count,                                                                                            \
      "Total number of each kinds of internal ddl operations",                                                                      \
      Counter,                                                                                                                      \
      F(type_create_table, {"type", "create_table"}),                                                                               \
      F(type_create_db, {"type", "create_db"}),                                                                                     \
      F(type_drop_table, {"type", "drop_table"}),                                                                                   \
      F(type_drop_db, {"type", "drop_db"}),                                                                                         \
      F(type_rename_table, {"type", "rename_table"}),                                                                               \
      F(type_modify_column, {"type", "modify_column"}),                                                                             \
      F(type_apply_partition, {"type", "apply_partition"}),                                                                         \
      F(type_exchange_partition, {"type", "exchange_partition"}))                                                                   \
    M(tiflash_schema_apply_duration_seconds,                                                                                        \
      "Bucketed histogram of ddl apply duration",                                                                                   \
      Histogram,                                                                                                                    \
      F(type_sync_schema_apply_duration, {{"type", "sync_schema_duration"}}, ExpBuckets{0.001, 2, 20}),                             \
      F(type_sync_table_schema_apply_duration, {{"type", "sync_table_schema_duration"}}, ExpBuckets{0.001, 2, 20}))                 \
    M(tiflash_raft_read_index_count, "Total number of raft read index", Counter)                                                    \
    M(tiflash_stale_read_count, "Total number of stale read", Counter)                                                              \
    M(tiflash_raft_read_index_duration_seconds,                                                                                     \
      "Bucketed histogram of raft read index duration",                                                                             \
      Histogram,                                                                                                                    \
      F(type_raft_read_index_duration, {{"type", "tmt_raft_read_index_duration"}}, ExpBuckets{0.001, 2, 20}))                       \
    M(tiflash_raft_wait_index_duration_seconds,                                                                                     \
      "Bucketed histogram of raft wait index duration",                                                                             \
      Histogram,                                                                                                                    \
      F(type_raft_wait_index_duration, {{"type", "tmt_raft_wait_index_duration"}}, ExpBuckets{0.001, 2, 20}))                       \
    M(tiflash_raft_eager_gc_duration_seconds,                                                                                       \
      "Bucketed histogram of RaftLog eager",                                                                                        \
      Histogram,                                                                                                                    \
      F(type_run, {{"type", "run"}}, ExpBuckets{0.0005, 2, 20}))                                                                    \
    M(tiflash_raft_eager_gc_count,                                                                                                  \
      "Total number processed in RaftLog eager GC",                                                                                 \
      Counter,                                                                                                                      \
      F(type_num_raft_logs, {"type", "num_raft_logs"}),                                                                             \
      F(type_num_skip_regions, {"type", "num_skip_regions"}),                                                                       \
      F(type_num_process_regions, {"type", "num_process_regions"}))                                                                 \
    M(tiflash_syncing_data_freshness,                                                                                               \
      "The freshness of tiflash data with tikv data",                                                                               \
      Histogram,                                                                                                                    \
      F(type_syncing_data_freshness, {{"type", "data_freshness"}}, ExpBuckets{0.001, 2, 20}))                                       \
    M(tiflash_storage_read_tasks_count, "Total number of storage engine read tasks", Counter)                                       \
    M(tiflash_storage_command_count,                                                                                                \
      "Total number of storage's command, such as delete range / shutdown /startup",                                                \
      Counter,                                                                                                                      \
      F(type_delete_range, {"type", "delete_range"}),                                                                               \
      F(type_ingest, {"type", "ingest"}),                                                                                           \
      F(type_ingest_checkpoint, {"type", "ingest_check_point"}))                                                                    \
    M(tiflash_storage_subtask_count,                                                                                                \
      "Total number of storage's sub task",                                                                                         \
      Counter,                                                                                                                      \
      F(type_delta_merge_bg, {"type", "delta_merge_bg"}),                                                                           \
      F(type_delta_merge_bg_gc, {"type", "delta_merge_bg_gc"}),                                                                     \
      F(type_delta_merge_fg, {"type", "delta_merge_fg"}),                                                                           \
      F(type_delta_merge_manual, {"type", "delta_merge_manual"}),                                                                   \
      F(type_delta_compact, {"type", "delta_compact"}),                                                                             \
      F(type_delta_flush, {"type", "delta_flush"}),                                                                                 \
      F(type_seg_split_bg, {"type", "seg_split_bg"}),                                                                               \
      F(type_seg_split_fg, {"type", "seg_split_fg"}),                                                                               \
      F(type_seg_split_ingest, {"type", "seg_split_ingest"}),                                                                       \
      F(type_seg_merge_bg_gc, {"type", "seg_merge_bg_gc"}),                                                                         \
      F(type_place_index_update, {"type", "place_index_update"}))                                                                   \
    M(tiflash_storage_subtask_duration_seconds,                                                                                     \
      "Bucketed histogram of storage's sub task duration",                                                                          \
      Histogram,                                                                                                                    \
      F(type_delta_merge_bg, {{"type", "delta_merge_bg"}}, ExpBuckets{0.001, 2, 20}),                                               \
      F(type_delta_merge_bg_gc, {{"type", "delta_merge_bg_gc"}}, ExpBuckets{0.001, 2, 20}),                                         \
      F(type_delta_merge_fg, {{"type", "delta_merge_fg"}}, ExpBuckets{0.001, 2, 20}),                                               \
      F(type_delta_merge_manual, {{"type", "delta_merge_manual"}}, ExpBuckets{0.001, 2, 20}),                                       \
      F(type_delta_compact, {{"type", "delta_compact"}}, ExpBuckets{0.001, 2, 20}),                                                 \
      F(type_delta_flush, {{"type", "delta_flush"}}, ExpBuckets{0.001, 2, 20}),                                                     \
      F(type_seg_split_bg, {{"type", "seg_split_bg"}}, ExpBuckets{0.001, 2, 20}),                                                   \
      F(type_seg_split_fg, {{"type", "seg_split_fg"}}, ExpBuckets{0.001, 2, 20}),                                                   \
      F(type_seg_split_ingest, {{"type", "seg_split_ingest"}}, ExpBuckets{0.001, 2, 20}),                                           \
      F(type_seg_merge_bg_gc, {{"type", "seg_merge_bg_gc"}}, ExpBuckets{0.001, 2, 20}),                                             \
      F(type_place_index_update, {{"type", "place_index_update"}}, ExpBuckets{0.001, 2, 20}))                                       \
    M(tiflash_storage_subtask_throughput_bytes,                                                                                     \
      "Calculate the throughput of (maybe foreground) tasks of storage in bytes",                                                   \
      Counter, /**/                                                                                                                 \
      F(type_delta_flush, {"type", "delta_flush"}), /**/                                                                            \
      F(type_delta_compact, {"type", "delta_compact"}), /**/                                                                        \
      F(type_write_to_cache, {"type", "write_to_cache"}), /**/                                                                      \
      F(type_write_to_disk, {"type", "write_to_disk"})) /**/                                                                        \
    M(tiflash_storage_subtask_throughput_rows,                                                                                      \
      "Calculate the throughput of (maybe foreground) tasks of storage in rows",                                                    \
      Counter, /**/                                                                                                                 \
      F(type_delta_flush, {"type", "delta_flush"}), /**/                                                                            \
      F(type_delta_compact, {"type", "delta_compact"}), /**/                                                                        \
      F(type_write_to_cache, {"type", "write_to_cache"}), /**/                                                                      \
      F(type_write_to_disk, {"type", "write_to_disk"})) /**/                                                                        \
    M(tiflash_storage_throughput_bytes,                                                                                             \
      "Calculate the throughput of tasks of storage in bytes",                                                                      \
      Gauge, /**/                                                                                                                   \
      F(type_write, {"type", "write"}), /**/                                                                                        \
      F(type_ingest, {"type", "ingest"}), /**/                                                                                      \
      F(type_delta_merge, {"type", "delta_merge"}), /**/                                                                            \
      F(type_split, {"type", "split"}), /**/                                                                                        \
      F(type_merge, {"type", "merge"})) /**/                                                                                        \
    M(tiflash_storage_throughput_rows,                                                                                              \
      "Calculate the throughput of tasks of storage in rows",                                                                       \
      Gauge, /**/                                                                                                                   \
      F(type_write, {"type", "write"}), /**/                                                                                        \
      F(type_ingest, {"type", "ingest"}), /**/                                                                                      \
      F(type_delta_merge, {"type", "delta_merge"}), /**/                                                                            \
      F(type_split, {"type", "split"}), /**/                                                                                        \
      F(type_merge, {"type", "merge"})) /**/                                                                                        \
    M(tiflash_storage_write_stall_duration_seconds,                                                                                 \
      "The write stall duration of storage, in seconds",                                                                            \
      Histogram,                                                                                                                    \
      F(type_write, {{"type", "write"}}, ExpBuckets{0.001, 2, 20}),                                                                 \
      F(type_delta_merge_by_write, {{"type", "delta_merge_by_write"}}, ExpBuckets{0.001, 2, 20}),                                   \
      F(type_delta_merge_by_delete_range, {{"type", "delta_merge_by_delete_range"}}, ExpBuckets{0.001, 2, 20}),                     \
      F(type_flush, {{"type", "flush"}}, ExpBuckets{0.001, 2, 20}),                                                                 \
      F(type_split, {{"type", "split"}}, ExpBuckets{0.001, 2, 20}))                                                                 \
    M(tiflash_storage_page_gc_count,                                                                                                \
      "Total number of page's gc execution.",                                                                                       \
      Counter,                                                                                                                      \
      F(type_v2, {"type", "v2"}),                                                                                                   \
      F(type_v2_low, {"type", "v2_low"}),                                                                                           \
      F(type_v3, {"type", "v3"}),                                                                                                   \
      F(type_v3_mvcc_dumped, {"type", "v3_mvcc_dumped"}),                                                                           \
      F(type_v3_bs_full_gc, {"type", "v3_bs_full_gc"}))                                                                             \
    M(tiflash_storage_page_gc_duration_seconds,                                                                                     \
      "Bucketed histogram of page's gc task duration",                                                                              \
      Histogram,                                                                                                                    \
      F(type_v2, {{"type", "v2"}}, ExpBuckets{0.0005, 2, 20}),                                                                      \
      F(type_v2_data_compact, {{"type", "v2_data_compact"}}, ExpBuckets{0.0005, 2, 20}),                                            \
      F(type_v2_ver_compact,                                                                                                        \
        {{"type", "v2_ver_compact"}},                                                                                               \
        ExpBuckets{0.0005, 2, 20}), /* Below are metrics for PageStorage V3 */                                                      \
      F(type_compact_wal, {{"type", "compact_wal"}}, ExpBuckets{0.0005, 2, 20}),                                                    \
      F(type_compact_directory, {{"type", "compact_directory"}}, ExpBuckets{0.0005, 2, 20}),                                        \
      F(type_compact_spacemap, {{"type", "compact_spacemap"}}, ExpBuckets{0.0005, 2, 20}),                                          \
      F(type_fullgc_rewrite, {{"type", "fullgc_rewrite"}}, ExpBuckets{0.0005, 2, 20}),                                              \
      F(type_fullgc_commit, {{"type", "fullgc_commit"}}, ExpBuckets{0.0005, 2, 20}),                                                \
      F(type_clean_external, {{"type", "clean_external"}}, ExpBuckets{0.0005, 2, 20}),                                              \
      F(type_v3, {{"type", "v3"}}, ExpBuckets{0.0005, 2, 20}))                                                                      \
    M(tiflash_storage_page_command_count,                                                                                           \
      "Total number of PageStorage's command, such as write / read / scan / snapshot",                                              \
      Counter,                                                                                                                      \
      F(type_write, {"type", "write"}),                                                                                             \
      F(type_read, {"type", "read"}),                                                                                               \
      F(type_read_page_dir, {"type", "read_page_dir"}),                                                                             \
      F(type_read_blob, {"type", "read_blob"}),                                                                                     \
      F(type_scan, {"type", "scan"}),                                                                                               \
      F(type_snapshot, {"type", "snapshot"}))                                                                                       \
    M(tiflash_storage_page_write_batch_size,                                                                                        \
      "The size of each write batch in bytes",                                                                                      \
      Histogram,                                                                                                                    \
      F(type_v3, {{"type", "v3"}}, ExpBuckets{4 * 1024, 4, 10}))                                                                    \
    M(tiflash_storage_page_write_duration_seconds,                                                                                  \
      "The duration of each write batch",                                                                                           \
      Histogram,                                                                                                                    \
      F(type_total,                                                                                                                 \
        {{"type", "total"}},                                                                                                        \
        ExpBuckets{0.0001, 2, 20}), /* the bucket range for apply in memory is 50us ~ 120s */                                       \
      F(type_choose_stat, {{"type", "choose_stat"}}, ExpBuckets{0.00005, 1.8, 26}),                                                 \
      F(type_search_pos, {{"type", "search_pos"}}, ExpBuckets{0.00005, 1.8, 26}),                                                   \
      F(type_blob_write, {{"type", "blob_write"}}, ExpBuckets{0.00005, 1.8, 26}),                                                   \
      F(type_latch, {{"type", "latch"}}, ExpBuckets{0.00005, 1.8, 26}),                                                             \
      F(type_wait_in_group, {{"type", "wait_in_group"}}, ExpBuckets{0.00005, 1.8, 26}),                                             \
      F(type_wal, {{"type", "wal"}}, ExpBuckets{0.00005, 1.8, 26}),                                                                 \
      F(type_commit, {{"type", "commit"}}, ExpBuckets{0.00005, 1.8, 26}))                                                           \
    M(tiflash_storage_logical_throughput_bytes,                                                                                     \
      "The logical throughput of read tasks of storage in bytes",                                                                   \
      Histogram,                                                                                                                    \
      F(type_read, {{"type", "read"}}, EqualWidthBuckets{1 * 1024 * 1024, 60, 50 * 1024 * 1024}))                                   \
    M(tiflash_storage_io_limiter,                                                                                                   \
      "Storage I/O limiter metrics",                                                                                                \
      Counter,                                                                                                                      \
      F(type_fg_read_req_bytes, {"type", "fg_read_req_bytes"}),                                                                     \
      F(type_fg_read_alloc_bytes, {"type", "fg_read_alloc_bytes"}),                                                                 \
      F(type_bg_read_req_bytes, {"type", "bg_read_req_bytes"}),                                                                     \
      F(type_bg_read_alloc_bytes, {"type", "bg_read_alloc_bytes"}),                                                                 \
      F(type_fg_write_req_bytes, {"type", "fg_write_req_bytes"}),                                                                   \
      F(type_fg_write_alloc_bytes, {"type", "fg_write_alloc_bytes"}),                                                               \
      F(type_bg_write_req_bytes, {"type", "bg_write_req_bytes"}),                                                                   \
      F(type_bg_write_alloc_bytes, {"type", "bg_write_alloc_bytes"}))                                                               \
    M(tiflash_storage_rough_set_filter_rate,                                                                                        \
      "Bucketed histogram of rough set filter rate",                                                                                \
      Histogram,                                                                                                                    \
      F(type_dtfile_pack, {{"type", "dtfile_pack"}}, EqualWidthBuckets{0, 6, 20}))                                                  \
    M(tiflash_disaggregated_object_lock_request_count,                                                                              \
      "Total number of S3 object lock/delete request",                                                                              \
      Counter,                                                                                                                      \
      F(type_lock, {"type", "lock"}),                                                                                               \
      F(type_delete, {"type", "delete"}),                                                                                           \
      F(type_owner_changed, {"type", "owner_changed"}),                                                                             \
      F(type_error, {"type", "error"}),                                                                                             \
      F(type_lock_conflict, {"type", "lock_conflict"}),                                                                             \
      F(type_delete_conflict, {"type", "delete_conflict"}),                                                                         \
      F(type_delete_risk, {"type", "delete_risk"}))                                                                                 \
    M(tiflash_disaggregated_object_lock_request_duration_seconds,                                                                   \
      "Bucketed histogram of S3 object lock/delete request duration",                                                               \
      Histogram,                                                                                                                    \
      F(type_lock, {{"type", "lock"}}, ExpBuckets{0.001, 2, 20}),                                                                   \
      F(type_delete, {{"type", "delete"}}, ExpBuckets{0.001, 2, 20}))                                                               \
    M(tiflash_disaggregated_read_tasks_count, "Total number of storage engine disaggregated read tasks", Counter)                   \
    M(tiflash_disaggregated_breakdown_duration_seconds,                                                                             \
      "",                                                                                                                           \
      Histogram,                                                                                                                    \
      F(type_rpc_establish, {{"type", "rpc_establish"}}, ExpBuckets{0.01, 2, 20}),                                                  \
      F(type_total_establish_backoff, {{"type", "total_establish_backoff"}}, ExpBuckets{0.01, 2, 20}),                              \
      F(type_resolve_lock, {{"type", "resolve_lock"}}, ExpBuckets{0.01, 2, 20}),                                                    \
      F(type_rpc_fetch_page, {{"type", "rpc_fetch_page"}}, ExpBuckets{0.01, 2, 20}),                                                \
      F(type_write_page_cache, {{"type", "write_page_cache"}}, ExpBuckets{0.01, 2, 20}),                                            \
      F(type_cache_occupy, {{"type", "cache_occupy"}}, ExpBuckets{0.01, 2, 20}),                                                    \
      F(type_worker_fetch_page, {{"type", "worker_fetch_page"}}, ExpBuckets{0.01, 2, 20}),                                          \
      F(type_worker_prepare_stream, {{"type", "worker_prepare_stream"}}, ExpBuckets{0.01, 2, 20}),                                  \
      F(type_stream_wait_next_task, {{"type", "stream_wait_next_task"}}, ExpBuckets{0.01, 2, 20}),                                  \
      F(type_stream_read, {{"type", "stream_read"}}, ExpBuckets{0.01, 2, 20}))                                                      \
    M(tiflash_disaggregated_details,                                                                                                \
      "",                                                                                                                           \
      Counter,                                                                                                                      \
      F(type_cftiny_read, {{"type", "cftiny_read"}}),                                                                               \
      F(type_cftiny_fetch, {{"type", "cftiny_fetch"}}))                                                                             \
    M(tiflash_raft_command_duration_seconds,                                                                                        \
      "Bucketed histogram of some raft command: apply snapshot and ingest SST",                                                     \
      Histogram, /* these command usually cost several seconds, increase the start bucket to 50ms */                                \
      F(type_remove_peer, {{"type", "remove_peer"}}, ExpBuckets{0.05, 2, 10}),                                                      \
      F(type_ingest_sst, {{"type", "ingest_sst"}}, ExpBuckets{0.05, 2, 10}),                                                        \
      F(type_ingest_sst_sst2dt, {{"type", "ingest_sst_sst2dt"}}, ExpBuckets{0.05, 2, 10}),                                          \
      F(type_ingest_sst_upload, {{"type", "ingest_sst_upload"}}, ExpBuckets{0.05, 2, 10}),                                          \
      F(type_apply_snapshot_predecode, {{"type", "snapshot_predecode"}}, ExpBuckets{0.05, 2, 15}),                                  \
      F(type_apply_snapshot_predecode_sst2dt, {{"type", "snapshot_predecode_sst2dt"}}, ExpBuckets{0.05, 2, 15}),                    \
      F(type_apply_snapshot_predecode_upload, {{"type", "snapshot_predecode_upload"}}, ExpBuckets{0.05, 2, 10}),                    \
      F(type_apply_snapshot_flush, {{"type", "snapshot_flush"}}, ExpBuckets{0.05, 2, 10}))                                          \
    M(tiflash_raft_process_keys,                                                                                                    \
      "Total number of keys processed in some types of Raft commands",                                                              \
      Counter,                                                                                                                      \
      F(type_write_put, {"type", "write_put"}),                                                                                     \
      F(type_write_del, {"type", "write_del"}),                                                                                     \
      F(type_apply_snapshot, {"type", "apply_snapshot"}),                                                                           \
      F(type_ingest_sst, {"type", "ingest_sst"}))                                                                                   \
    M(tiflash_raft_apply_write_command_duration_seconds,                                                                            \
      "Bucketed histogram of applying write command Raft logs",                                                                     \
      Histogram,                                                                                                                    \
      F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}),                                                                \
      F(type_admin, {{"type", "admin"}}, ExpBuckets{0.0005, 2, 20}),                                                                \
      F(type_admin_batch_split, {{"type", "admin_batch_split"}}, ExpBuckets{0.0005, 2, 20}),                                        \
      F(type_admin_prepare_merge, {{"type", "admin_prepare_merge"}}, ExpBuckets{0.0005, 2, 20}),                                    \
      F(type_admin_commit_merge, {{"type", "admin_commit_merge"}}, ExpBuckets{0.0005, 2, 20}),                                      \
      F(type_admin_change_peer, {{"type", "admin_change_peer"}}, ExpBuckets{0.0005, 2, 20}),                                        \
      F(type_flush_region, {{"type", "flush_region"}}, ExpBuckets{0.0005, 2, 20}))                                                  \
    M(tiflash_raft_upstream_latency,                                                                                                \
      "The latency that tikv sends raft log to tiflash.",                                                                           \
      Histogram,                                                                                                                    \
      F(type_write, {{"type", "write"}}, ExpBuckets{0.001, 2, 30}))                                                                 \
    M(tiflash_raft_write_data_to_storage_duration_seconds,                                                                          \
      "Bucketed histogram of writting region into storage layer",                                                                   \
      Histogram,                                                                                                                    \
      F(type_decode, {{"type", "decode"}}, ExpBuckets{0.0005, 2, 20}),                                                              \
      F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}))                                                                \
    M(tiflash_raft_raft_log_gap_count,                                                                                              \
      "Bucketed histogram raft index gap between applied and truncated index",                                                      \
      Histogram,                                                                                                                    \
      F(type_applied_index, {{"type", "applied_index"}}, EqualWidthBuckets{0, 100, 15}),                                            \
      F(type_eager_gc_applied_index, {{"type", "eager_gc_applied_index"}}, EqualWidthBuckets{0, 100, 10}),                          \
      F(type_unflushed_applied_index, {{"type", "unflushed_applied_index"}}, EqualWidthBuckets{0, 100, 15}))                        \
    M(tiflash_raft_raft_events_count,                                                                                               \
      "Raft event counter",                                                                                                         \
      Counter,                                                                                                                      \
      F(type_pre_exec_compact, {{"type", "pre_exec_compact"}}),                                                                     \
      F(type_flush_apply_snapshot, {{"type", "flush_apply_snapshot"}}),                                                             \
      F(type_flush_ingest_sst, {{"type", "flush_ingest_sst"}}),                                                                     \
      F(type_flush_useless_admin, {{"type", "flush_useless_admin"}}),                                                               \
      F(type_flush_useful_admin, {{"type", "flush_useful_admin"}}),                                                                 \
      F(type_flush_passive, {{"type", "flush_passive"}}),                                                                           \
      F(type_flush_proactive, {{"type", "flush_proactive"}}),                                                                       \
      F(type_flush_log_gap, {{"type", "flush_log_gap"}}),                                                                           \
      F(type_flush_size, {{"type", "flush_size"}}),                                                                                 \
      F(type_flush_rowcount, {{"type", "flush_rowcount"}}),                                                                         \
      F(type_flush_eager_gc, {{"type", "flush_eager_gc"}}))                                                                         \
    M(tiflash_raft_raft_frequent_events_count,                                                                                      \
      "Raft frequent event counter",                                                                                                \
      Counter,                                                                                                                      \
      F(type_write, {{"type", "write"}}))                                                                                           \
    M(tiflash_raft_region_flush_bytes,                                                                                              \
      "Bucketed histogram of region flushed bytes",                                                                                 \
      Histogram,                                                                                                                    \
      F(type_flushed, {{"type", "flushed"}}, ExpBuckets{32, 2, 21}),                                                                \
      F(type_unflushed, {{"type", "unflushed"}}, ExpBuckets{32, 2, 21}))                                                            \
    M(tiflash_raft_entry_size,                                                                                                      \
      "Bucketed histogram entry size",                                                                                              \
      Histogram,                                                                                                                    \
      F(type_normal, {{"type", "normal"}}, ExpBuckets{1, 2, 13}))                                                                   \
    M(tiflash_raft_ongoing_snapshot_total_bytes,                                                                                    \
      "Ongoing snapshot total size",                                                                                                \
      Gauge,                                                                                                                        \
      F(type_raft_snapshot, {{"type", "raft_snapshot"}}),                                                                           \
      F(type_dt_on_disk, {{"type", "dt_on_disk"}}),                                                                                 \
      F(type_dt_total, {{"type", "dt_total"}}))                                                                                     \
    M(tiflash_raft_snapshot_total_bytes,                                                                                            \
      "Bucketed snapshot total size",                                                                                               \
      Histogram,                                                                                                                    \
      F(type_approx_raft_snapshot, {{"type", "approx_raft_snapshot"}}, ExpBuckets{1024, 2, 24})) /* 16G */                          \
    /* required by DBaaS */                                                                                                         \
    M(tiflash_server_info,                                                                                                          \
      "Indicate the tiflash server info, and the value is the start timestamp (s).",                                                \
      Gauge,                                                                                                                        \
      F(start_time, {"version", TiFlashBuildInfo::getReleaseVersion()}, {"hash", TiFlashBuildInfo::getGitHash()}))                  \
    M(tiflash_object_count,                                                                                                         \
      "Number of objects",                                                                                                          \
      Gauge,                                                                                                                        \
      F(type_count_of_establish_calldata, {"type", "count_of_establish_calldata"}),                                                 \
      F(type_count_of_mpptunnel, {"type", "count_of_mpptunnel"}))                                                                   \
    M(tiflash_thread_count,                                                                                                         \
      "Number of threads",                                                                                                          \
      Gauge,                                                                                                                        \
      F(type_max_threads_of_thdpool, {"type", "thread_pool_total_max"}),                                                            \
      F(type_active_threads_of_thdpool, {"type", "thread_pool_active"}),                                                            \
      F(type_max_active_threads_of_thdpool, {"type", "thread_pool_active_max"}),                                                    \
      F(type_total_threads_of_thdpool, {"type", "thread_pool_total"}),                                                              \
      F(type_max_threads_of_raw, {"type", "total_max"}),                                                                            \
      F(type_total_threads_of_raw, {"type", "total"}),                                                                              \
      F(type_threads_of_client_cq_pool, {"type", "rpc_client_cq_pool"}),                                                            \
      F(type_threads_of_receiver_read_loop, {"type", "rpc_receiver_read_loop"}),                                                    \
      F(type_threads_of_receiver_reactor, {"type", "rpc_receiver_reactor"}),                                                        \
      F(type_max_threads_of_establish_mpp, {"type", "rpc_establish_mpp_max"}),                                                      \
      F(type_active_threads_of_establish_mpp, {"type", "rpc_establish_mpp"}),                                                       \
      F(type_max_threads_of_dispatch_mpp, {"type", "rpc_dispatch_mpp_max"}),                                                        \
      F(type_active_threads_of_dispatch_mpp, {"type", "rpc_dispatch_mpp"}),                                                         \
      F(type_active_rpc_async_worker, {"type", "rpc_async_worker_active"}),                                                         \
      F(type_total_rpc_async_worker, {"type", "rpc_async_worker_total"}))                                                           \
    M(tiflash_task_scheduler,                                                                                                       \
      "Min-tso task scheduler",                                                                                                     \
      Gauge,                                                                                                                        \
      F(type_min_tso, {"type", "min_tso"}),                                                                                         \
      F(type_waiting_queries_count, {"type", "waiting_queries_count"}),                                                             \
      F(type_active_queries_count, {"type", "active_queries_count"}),                                                               \
      F(type_waiting_tasks_count, {"type", "waiting_tasks_count"}),                                                                 \
      F(type_active_tasks_count, {"type", "active_tasks_count"}),                                                                   \
      F(type_global_estimated_thread_usage, {"type", "global_estimated_thread_usage"}),                                             \
      F(type_estimated_thread_usage, {"type", "estimated_thread_usage"}),                                                           \
      F(type_thread_soft_limit, {"type", "thread_soft_limit"}),                                                                     \
      F(type_thread_hard_limit, {"type", "thread_hard_limit"}),                                                                     \
      F(type_hard_limit_exceeded_count, {"type", "hard_limit_exceeded_count"}),                                                     \
      F(type_group_entry_count, {"type", "group_entry_count"}))                                                                     \
    M(tiflash_task_scheduler_waiting_duration_seconds,                                                                              \
      "Bucketed histogram of task waiting for scheduling duration",                                                                 \
      Histogram,                                                                                                                    \
      F(type_task_scheduler_waiting_duration, {{"type", "task_waiting_duration"}}, ExpBuckets{0.001, 2, 20}))                       \
    M(tiflash_storage_read_thread_counter,                                                                                          \
      "The counter of storage read thread",                                                                                         \
      Counter,                                                                                                                      \
      F(type_sche_no_pool, {"type", "sche_no_pool"}),                                                                               \
      F(type_sche_no_slot, {"type", "sche_no_slot"}),                                                                               \
      F(type_sche_no_ru, {"type", "sche_no_ru"}),                                                                                   \
      F(type_sche_no_segment, {"type", "sche_no_segment"}),                                                                         \
      F(type_sche_active_segment_limit, {"type", "sche_active_segment_limit"}),                                                     \
      F(type_sche_from_cache, {"type", "sche_from_cache"}),                                                                         \
      F(type_sche_new_task, {"type", "sche_new_task"}),                                                                             \
      F(type_ru_exhausted, {"type", "ru_exhausted"}),                                                                               \
      F(type_push_block_bytes, {"type", "push_block_bytes"}),                                                                       \
      F(type_add_cache_succ, {"type", "add_cache_succ"}),                                                                           \
      F(type_add_cache_stale, {"type", "add_cache_stale"}),                                                                         \
      F(type_add_cache_reach_count_limit, {"type", "add_cache_reach_count_limit"}),                                                 \
      F(type_add_cache_total_bytes_limit, {"type", "add_cache_total_bytes_limit"}),                                                 \
      F(type_get_cache_miss, {"type", "get_cache_miss"}),                                                                           \
      F(type_get_cache_part, {"type", "get_cache_part"}),                                                                           \
      F(type_get_cache_hit, {"type", "get_cache_hit"}),                                                                             \
      F(type_get_cache_copy, {"type", "get_cache_copy"}))                                                                           \
    M(tiflash_storage_read_thread_gauge,                                                                                            \
      "The gauge of storage read thread",                                                                                           \
      Gauge,                                                                                                                        \
      F(type_merged_task, {"type", "merged_task"}))                                                                                 \
    M(tiflash_storage_read_thread_seconds,                                                                                          \
      "Bucketed histogram of read thread",                                                                                          \
      Histogram,                                                                                                                    \
      F(type_merged_task, {{"type", "merged_task"}}, ExpBuckets{0.001, 2, 20}))                                                     \
    M(tiflash_mpp_task_manager,                                                                                                     \
      "The gauge of mpp task manager",                                                                                              \
      Gauge,                                                                                                                        \
      F(type_mpp_query_count, {"type", "mpp_query_count"}))                                                                         \
    M(tiflash_mpp_task_monitor,                                                                                                     \
      "Monitor the lifecycle of MPP Task",                                                                                          \
      Gauge,                                                                                                                        \
      F(type_longest_live_time, {"type", "longest_live_time"}), )                                                                   \
    M(tiflash_exchange_queueing_data_bytes,                                                                                         \
      "Total bytes of data contained in the queue",                                                                                 \
      Gauge,                                                                                                                        \
      F(type_send, {{"type", "send_queue"}}),                                                                                       \
      F(type_receive, {{"type", "recv_queue"}}))                                                                                    \
    M(tiflash_compute_request_unit,                                                                                                 \
      "Request Unit used by tiflash compute",                                                                                       \
      Counter,                                                                                                                      \
      F(type_mpp,                                                                                                                   \
        {{"type", "mpp"},                                                                                                           \
         ComputeLabelHolder::instance().getClusterIdLabel(),                                                                        \
         ComputeLabelHolder::instance().getProcessIdLabel()}),                                                                      \
      F(type_cop,                                                                                                                   \
        {{"type", "cop"},                                                                                                           \
         ComputeLabelHolder::instance().getClusterIdLabel(),                                                                        \
         ComputeLabelHolder::instance().getProcessIdLabel()}),                                                                      \
      F(type_cop_stream,                                                                                                            \
        {{"type", "cop_stream"},                                                                                                    \
         ComputeLabelHolder::instance().getClusterIdLabel(),                                                                        \
         ComputeLabelHolder::instance().getProcessIdLabel()}),                                                                      \
      F(type_batch,                                                                                                                 \
        {{"type", "batch"},                                                                                                         \
         ComputeLabelHolder::instance().getClusterIdLabel(),                                                                        \
         ComputeLabelHolder::instance().getProcessIdLabel()}))                                                                      \
    M(tiflash_shared_block_schemas,                                                                                                 \
      "statistics about shared block schemas of ColumnFiles",                                                                       \
      Gauge,                                                                                                                        \
      F(type_current_size, {{"type", "current_size"}}),                                                                             \
      F(type_still_used_when_evict, {{"type", "still_used_when_evict"}}),                                                           \
      F(type_miss_count, {{"type", "miss_count"}}),                                                                                 \
      F(type_hit_count, {{"type", "hit_count"}}))                                                                                   \
    M(tiflash_storage_remote_stats,                                                                                                 \
      "The file stats on remote store",                                                                                             \
      Gauge,                                                                                                                        \
      F(type_total_size, {"type", "total_size"}),                                                                                   \
      F(type_valid_size, {"type", "valid_size"}),                                                                                   \
      F(type_num_files, {"type", "num_files"}))                                                                                     \
    M(tiflash_storage_checkpoint_seconds,                                                                                           \
      "PageStorage checkpoint elapsed time",                                                                                        \
      Histogram, /* these command usually cost several seconds, increase the start bucket to 50ms */                                \
      F(type_dump_checkpoint_snapshot, {{"type", "dump_checkpoint_snapshot"}}, ExpBuckets{0.05, 2, 20}),                            \
      F(type_dump_checkpoint_data, {{"type", "dump_checkpoint_data"}}, ExpBuckets{0.05, 2, 20}),                                    \
      F(type_upload_checkpoint, {{"type", "upload_checkpoint"}}, ExpBuckets{0.05, 2, 20}),                                          \
      F(type_copy_checkpoint_info, {{"type", "copy_checkpoint_info"}}, ExpBuckets{0.05, 2, 20}))                                    \
    M(tiflash_storage_checkpoint_flow,                                                                                              \
      "The bytes flow cause by remote checkpoint",                                                                                  \
      Counter,                                                                                                                      \
      F(type_incremental, {"type", "incremental"}),                                                                                 \
      F(type_compaction, {"type", "compaction"}))                                                                                   \
    M(tiflash_storage_checkpoint_keys_by_types,                                                                                     \
      "The keys flow cause by remote checkpoint",                                                                                   \
      Counter,                                                                                                                      \
      F(type_raftengine, {"type", "raftengine"}),                                                                                   \
      F(type_kvengine, {"type", "kvengine"}),                                                                                       \
      F(type_kvstore, {"type", "kvstore"}),                                                                                         \
      F(type_data, {"type", "data"}),                                                                                               \
      F(type_log, {"type", "log"}),                                                                                                 \
      F(type_meta, {"type", "kvstore"}),                                                                                            \
      F(type_unknown, {"type", "unknown"}))                                                                                         \
    M(tiflash_storage_checkpoint_flow_by_types,                                                                                     \
      "The bytes flow cause by remote checkpoint",                                                                                  \
      Counter,                                                                                                                      \
      F(type_raftengine, {"type", "raftengine"}),                                                                                   \
      F(type_kvengine, {"type", "kvengine"}),                                                                                       \
      F(type_kvstore, {"type", "kvstore"}),                                                                                         \
      F(type_data, {"type", "data"}),                                                                                               \
      F(type_log, {"type", "log"}),                                                                                                 \
      F(type_meta, {"type", "kvstore"}),                                                                                            \
      F(type_unknown, {"type", "unknown"}))                                                                                         \
    M(tiflash_storage_page_data_by_types,                                                                                           \
      "The existing bytes stored in UniPageStorage",                                                                                \
      Gauge,                                                                                                                        \
      F(type_raftengine, {"type", "raftengine"}),                                                                                   \
      F(type_kvengine, {"type", "kvengine"}),                                                                                       \
      F(type_kvstore, {"type", "kvstore"}),                                                                                         \
      F(type_data, {"type", "data"}),                                                                                               \
      F(type_log, {"type", "log"}),                                                                                                 \
      F(type_meta, {"type", "kvstore"}),                                                                                            \
      F(type_unknown, {"type", "unknown"}))                                                                                         \
    M(tiflash_storage_s3_request_seconds,                                                                                           \
      "S3 request duration in seconds",                                                                                             \
      Histogram,                                                                                                                    \
      F(type_put_object, {{"type", "put_object"}}, ExpBuckets{0.001, 2, 20}),                                                       \
      F(type_put_dmfile, {{"type", "put_dmfile"}}, ExpBuckets{0.001, 2, 20}),                                                       \
      F(type_copy_object, {{"type", "copy_object"}}, ExpBuckets{0.001, 2, 20}),                                                     \
      F(type_get_object, {{"type", "get_object"}}, ExpBuckets{0.001, 2, 20}),                                                       \
      F(type_create_multi_part_upload, {{"type", "create_multi_part_upload"}}, ExpBuckets{0.001, 2, 20}),                           \
      F(type_upload_part, {{"type", "upload_part"}}, ExpBuckets{0.001, 2, 20}),                                                     \
      F(type_complete_multi_part_upload, {{"type", "complete_multi_part_upload"}}, ExpBuckets{0.001, 2, 20}),                       \
      F(type_list_objects, {{"type", "list_objects"}}, ExpBuckets{0.001, 2, 20}),                                                   \
      F(type_delete_object, {{"type", "delete_object"}}, ExpBuckets{0.001, 2, 20}),                                                 \
      F(type_head_object, {{"type", "head_object"}}, ExpBuckets{0.001, 2, 20}),                                                     \
      F(type_read_stream, {{"type", "read_stream"}}, ExpBuckets{0.0001, 2, 20}))                                                    \
    M(tiflash_storage_s3_http_request_seconds,                                                                                      \
      "S3 request duration breakdown in seconds",                                                                                   \
      Histogram,                                                                                                                    \
      F(type_dns, {{"type", "dns"}}, ExpBuckets{0.001, 2, 20}),                                                                     \
      F(type_connect, {{"type", "connect"}}, ExpBuckets{0.001, 2, 20}),                                                             \
      F(type_request, {{"type", "request"}}, ExpBuckets{0.001, 2, 20}),                                                             \
      F(type_response, {{"type", "response"}}, ExpBuckets{0.001, 2, 20}))                                                           \
    M(tiflash_pipeline_scheduler,                                                                                                   \
      "pipeline scheduler",                                                                                                         \
      Gauge,                                                                                                                        \
      F(type_waiting_tasks_count, {"type", "waiting_tasks_count"}),                                                                 \
      F(type_cpu_pending_tasks_count, {"type", "cpu_pending_tasks_count"}),                                                         \
      F(type_cpu_executing_tasks_count, {"type", "cpu_executing_tasks_count"}),                                                     \
      F(type_io_pending_tasks_count, {"type", "io_pending_tasks_count"}),                                                           \
      F(type_io_executing_tasks_count, {"type", "io_executing_tasks_count"}),                                                       \
      F(type_cpu_task_thread_pool_size, {"type", "cpu_task_thread_pool_size"}),                                                     \
      F(type_io_task_thread_pool_size, {"type", "io_task_thread_pool_size"}))                                                       \
    M(tiflash_pipeline_task_duration_seconds,                                                                                       \
      "Bucketed histogram of pipeline task duration in seconds",                                                                    \
      Histogram, /* these command usually cost several hundred milliseconds to several seconds, increase the start bucket to 5ms */ \
      F(type_cpu_execute, {{"type", "cpu_execute"}}, ExpBuckets{0.005, 2, 20}),                                                     \
      F(type_io_execute, {{"type", "io_execute"}}, ExpBuckets{0.005, 2, 20}),                                                       \
      F(type_cpu_queue, {{"type", "cpu_queue"}}, ExpBuckets{0.005, 2, 20}),                                                         \
      F(type_io_queue, {{"type", "io_queue"}}, ExpBuckets{0.005, 2, 20}),                                                           \
      F(type_await, {{"type", "await"}}, ExpBuckets{0.005, 2, 20}))                                                                 \
    M(tiflash_pipeline_task_execute_max_time_seconds_per_round,                                                                     \
      "Bucketed histogram of pipeline task execute max time per round in seconds",                                                  \
      Histogram, /* these command usually cost several hundred milliseconds to several seconds, increase the start bucket to 5ms */ \
      F(type_cpu, {{"type", "cpu"}}, ExpBuckets{0.005, 2, 20}),                                                                     \
      F(type_io, {{"type", "io"}}, ExpBuckets{0.005, 2, 20}))                                                                       \
    M(tiflash_pipeline_task_change_to_status,                                                                                       \
      "pipeline task change to status",                                                                                             \
      Counter,                                                                                                                      \
      F(type_to_waiting, {"type", "to_waiting"}),                                                                                   \
      F(type_to_running, {"type", "to_running"}),                                                                                   \
      F(type_to_io, {"type", "to_io"}),                                                                                             \
      F(type_to_finished, {"type", "to_finished"}),                                                                                 \
      F(type_to_error, {"type", "to_error"}),                                                                                       \
      F(type_to_cancelled, {"type", "to_cancelled"}))                                                                               \
    M(tiflash_storage_s3_gc_status,                                                                                                 \
      "S3 GC status",                                                                                                               \
      Gauge,                                                                                                                        \
      F(type_lifecycle_added, {{"type", "lifecycle_added"}}),                                                                       \
      F(type_lifecycle_failed, {{"type", "lifecycle_failed"}}),                                                                     \
      F(type_owner, {{"type", "owner"}}),                                                                                           \
      F(type_running, {{"type", "running"}}))                                                                                       \
    M(tiflash_storage_s3_gc_seconds,                                                                                                \
      "S3 GC subprocess duration in seconds",                                                                                       \
      Histogram, /* these command usually cost several seconds, increase the start bucket to 500ms */                               \
      F(type_total, {{"type", "total"}}, ExpBuckets{0.5, 2, 20}),                                                                   \
      F(type_one_store, {{"type", "one_store"}}, ExpBuckets{0.5, 2, 20}),                                                           \
      F(type_read_locks, {{"type", "read_locks"}}, ExpBuckets{0.5, 2, 20}),                                                         \
      F(type_clean_locks, {{"type", "clean_locks"}}, ExpBuckets{0.5, 2, 20}),                                                       \
      F(type_clean_manifests, {{"type", "clean_manifests"}}, ExpBuckets{0.5, 2, 20}),                                               \
      F(type_scan_then_clean_data_files, {{"type", "scan_then_clean_data_files"}}, ExpBuckets{0.5, 2, 20}),                         \
      F(type_clean_one_lock, {{"type", "clean_one_lock"}}, ExpBuckets{0.5, 2, 20}))                                                 \
    M(tiflash_storage_remote_cache,                                                                                                 \
      "Operations of remote cache",                                                                                                 \
      Counter,                                                                                                                      \
      F(type_dtfile_hit, {"type", "dtfile_hit"}),                                                                                   \
      F(type_dtfile_miss, {"type", "dtfile_miss"}),                                                                                 \
      F(type_dtfile_evict, {"type", "dtfile_evict"}),                                                                               \
      F(type_dtfile_full, {"type", "dtfile_full"}),                                                                                 \
      F(type_dtfile_download, {"type", "dtfile_download"}),                                                                         \
      F(type_dtfile_download_failed, {"type", "dtfile_download_failed"}),                                                           \
      F(type_page_hit, {"type", "page_hit"}),                                                                                       \
      F(type_page_miss, {"type", "page_miss"}),                                                                                     \
      F(type_page_evict, {"type", "page_evict"}),                                                                                   \
      F(type_page_full, {"type", "page_full"}),                                                                                     \
      F(type_page_download, {"type", "page_download"}))                                                                             \
    M(tiflash_storage_remote_cache_bytes,                                                                                           \
      "Flow of remote cache",                                                                                                       \
      Counter,                                                                                                                      \
      F(type_dtfile_evict_bytes, {"type", "dtfile_evict_bytes"}),                                                                   \
      F(type_dtfile_download_bytes, {"type", "dtfile_download_bytes"}),                                                             \
      F(type_dtfile_read_bytes, {"type", "dtfile_read_bytes"}),                                                                     \
      F(type_page_evict_bytes, {"type", "page_evict_bytes"}),                                                                       \
      F(type_page_download_bytes, {"type", "page_download_bytes"}),                                                                 \
      F(type_page_read_bytes, {"type", "page_read_bytes"}))                                                                         \
    M(tiflash_storage_io_limiter_pending_seconds,                                                                                   \
      "I/O limiter pending duration in seconds",                                                                                    \
      Histogram,                                                                                                                    \
      F(type_fg_read, {{"type", "fg_read"}}, ExpBuckets{0.001, 2, 20}),                                                             \
      F(type_bg_read, {{"type", "bg_read"}}, ExpBuckets{0.001, 2, 20}),                                                             \
      F(type_fg_write, {{"type", "fg_write"}}, ExpBuckets{0.001, 2, 20}),                                                           \
      F(type_bg_write, {{"type", "bg_write"}}, ExpBuckets{0.001, 2, 20}))                                                           \
    M(tiflash_system_seconds,                                                                                                       \
      "system calls duration in seconds",                                                                                           \
      Histogram,                                                                                                                    \
      F(type_fsync, {{"type", "fsync"}}, ExpBuckets{0.0001, 2, 20}))                                                                \
    M(tiflash_storage_delta_index_cache, "", Counter, F(type_hit, {"type", "hit"}), F(type_miss, {"type", "miss"}))                 \
    M(tiflash_resource_group,                                                                                                       \
      "meta info of resource group",                                                                                                \
      Gauge,                                                                                                                        \
      F(type_remaining_tokens, {"type", "remaining_tokens"}),                                                                       \
      F(type_avg_speed, {"type", "avg_speed"}),                                                                                     \
      F(type_total_consumption, {"type", "total_consumption"}),                                                                     \
      F(type_bucket_fill_rate, {"type", "bucket_fill_rate"}),                                                                       \
      F(type_bucket_capacity, {"type", "bucket_capacity"}),                                                                         \
      F(type_compute_ru_consumption, {"type", "compute_ru_consumption"}),                                                           \
      F(type_storage_ru_consumption, {"type", "storage_ru_consumption"}),                                                           \
      F(type_compute_ru_exhausted, {"type", "compute_ru_exhausted"}),                                                               \
      F(type_gac_req_acquire_tokens, {"type", "gac_req_acquire_tokens"}),                                                           \
      F(type_gac_req_ru_consumption_delta, {"type", "gac_req_ru_consumption_delta"}),                                               \
      F(type_gac_resp_tokens, {"type", "gac_resp_tokens"}),                                                                         \
      F(type_gac_resp_capacity, {"type", "gac_resp_capacity"}))                                                                     \
    M(tiflash_storage_io_limiter_pending_count,                                                                                     \
      "I/O limiter pending count",                                                                                                  \
      Counter,                                                                                                                      \
      F(type_fg_read, {"type", "fg_read"}),                                                                                         \
      F(type_bg_read, {"type", "bg_read"}),                                                                                         \
      F(type_fg_write, {"type", "fg_write"}),                                                                                       \
      F(type_bg_write, {"type", "bg_write"}))                                                                                       \
    M(tiflash_read_thread_internal_us,                                                                                              \
      "Durations of read thread internal components",                                                                               \
      Histogram,                                                                                                                    \
      F(type_block_queue_pop_latency, {{"type", "block_queue_pop_latency"}}, ExpBuckets{1, 2, 20}))


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

static const String METRIC_RESOURCE_GROUP_STR = "resource_group";

template <typename T>
struct MetricFamilyTrait
{
};
template <>
struct MetricFamilyTrait<prometheus::Counter>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildCounter(); }
    static auto & add(prometheus::Family<prometheus::Counter> & family, ArgType && arg)
    {
        return family.Add(std::forward<ArgType>(arg));
    }
    static auto & add(
        prometheus::Family<prometheus::Counter> & family,
        const String & resource_group_name,
        ArgType && arg)
    {
        std::map<String, String> args_map = {std::forward<ArgType>(arg)};
        args_map[METRIC_RESOURCE_GROUP_STR] = resource_group_name;
        return family.Add(args_map);
    }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
    static auto & add(prometheus::Family<prometheus::Gauge> & family, ArgType && arg)
    {
        return family.Add(std::forward<ArgType>(arg));
    }
    static auto & add(
        prometheus::Family<prometheus::Gauge> & family,
        const String & resource_group_name,
        ArgType && arg)
    {
        std::map<String, String> args_map = {std::forward<ArgType>(arg)};
        args_map[METRIC_RESOURCE_GROUP_STR] = resource_group_name;
        return family.Add(args_map);
    }
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
    static auto & add(
        prometheus::Family<prometheus::Histogram> & family,
        const String & resource_group_name,
        ArgType && arg)
    {
        std::map<String, String> args_map = std::get<0>(arg);
        args_map[METRIC_RESOURCE_GROUP_STR] = resource_group_name;
        return family.Add(args_map, std::move(std::get<1>(arg)));
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
        store_args = args;
        auto & family = MetricTrait::build().Name(name).Help(help).Register(registry);
        store_family = &family;

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
    T & get(size_t idx, const String & resource_group_name)
    {
        {
            std::shared_lock lock(resource_group_metrics_mu);
            if (resource_group_metrics_map.find(resource_group_name) != resource_group_metrics_map.end())
                return *(resource_group_metrics_map[resource_group_name][idx]);
        }

        std::lock_guard lock(resource_group_metrics_mu);
        if (resource_group_metrics_map.find(resource_group_name) == resource_group_metrics_map.end())
            addMetricsForResourceGroup(resource_group_name);

        return *(resource_group_metrics_map[resource_group_name][idx]);
    }

private:
    void addMetricsForResourceGroup(const String & resource_group_name)
    {
        std::vector<T *> metrics_temp;

        for (auto arg : store_args)
        {
            auto & metric = MetricTrait::add(*store_family, resource_group_name, std::forward<MetricArgType>(arg));
            metrics_temp.emplace_back(&metric);
        }

        if (store_args.size() == 0)
        {
            auto & metric = MetricTrait::add(*store_family, resource_group_name, MetricArgType{});
            metrics_temp.emplace_back(&metric);
        }
        resource_group_metrics_map[resource_group_name] = metrics_temp;
    }

    std::vector<T *> metrics;
    prometheus::Family<T> * store_family;
    std::vector<MetricArgType> store_args;
    // <resource_group_name, metrics>
    std::shared_mutex resource_group_metrics_mu;
    std::unordered_map<String, std::vector<T *>> resource_group_metrics_map;
};

/// Centralized registry of TiFlash metrics.
/// Cope with MetricsPrometheus by registering
/// profile events, current metrics and customized metrics (as individual member for caller to access) into registry ahead of being updated.
/// Asynchronous metrics will be however registered by MetricsPrometheus itself due to the life cycle difference.
class TiFlashMetrics
{
public:
    static TiFlashMetrics & instance();

    void addReplicaSyncRU(UInt32 keyspace_id, UInt64 ru);

private:
    TiFlashMetrics();

    prometheus::Counter * getReplicaSyncRUCounter(UInt32 keyspace_id, std::unique_lock<std::mutex> &);
    void removeReplicaSyncRUCounter(UInt32 keyspace_id);

    static constexpr auto profile_events_prefix = "tiflash_system_profile_event_";
    static constexpr auto current_metrics_prefix = "tiflash_system_current_metric_";
    static constexpr auto async_metrics_prefix = "tiflash_system_asynchronous_metric_";

    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();
    // Here we add a ProcessCollector to collect cpu/rss/vsize/start_time information.
    // Normally, these metrics will be collected by tiflash-proxy,
    // but in disaggregated compute mode with AutoScaler, tiflash-proxy will not start, so tiflash will collect these metrics itself.
    std::shared_ptr<ProcessCollector> cn_process_collector = std::make_shared<ProcessCollector>();

    std::vector<prometheus::Gauge *> registered_profile_events;
    std::vector<prometheus::Gauge *> registered_current_metrics;
    std::unordered_map<std::string, prometheus::Gauge *> registered_async_metrics;

    prometheus::Family<prometheus::Gauge> * registered_keypace_store_used_family;
    using KeyspaceID = UInt32;
    std::unordered_map<KeyspaceID, prometheus::Gauge *> registered_keypace_store_used_metrics;
    prometheus::Gauge * store_used_total_metric;

    prometheus::Family<prometheus::Counter> * registered_keyspace_sync_replica_ru_family;
    std::mutex replica_sync_ru_mtx;
    std::unordered_map<KeyspaceID, prometheus::Counter *> registered_keyspace_sync_replica_ru;

public:
#define MAKE_METRIC_MEMBER_M(family_name, help, type, ...) \
    MetricFamily<prometheus::type> family_name             \
        = MetricFamily<prometheus::type>(*registry, #family_name, #help, {__VA_ARGS__});
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
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_RESOURCE_GROUP_METRIC_MACRO(_1, _2, _3, NAME, ...) NAME

#ifndef GTEST_TIFLASH_METRICS
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_0(family) TiFlashMetrics::instance().family.get()
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_1(family, metric) TiFlashMetrics::instance().family.get(family##_metrics::metric)
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_RESOURCE_GROUP_METRIC_0(family, resource_group) TiFlashMetrics::instance().family.get(0, resource_group)
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_RESOURCE_GROUP_METRIC_1(family, metric, resource_group) \
    TiFlashMetrics::instance().family.get(family##_metrics::metric, resource_group)
#else
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_0(family) TestMetrics::instance().family.get()
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_METRIC_1(family, metric) TestMetrics::instance().family.get(family##_metrics::metric)
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_RESOURCE_GROUP_METRIC_0(family, resource_group) TestMetrics::instance().family.get(0, resource_group)
// NOLINTNEXTLINE(bugprone-reserved-identifier)
#define __GET_RESOURCE_GROUP_METRIC_1(family, metric, resource_group) \
    TestMetrics::instance().family.get(family##_metrics::metric, resource_group)
#endif

#define GET_METRIC(...)                                             \
    __GET_METRIC_MACRO(__VA_ARGS__, __GET_METRIC_1, __GET_METRIC_0) \
    (__VA_ARGS__)

#define GET_RESOURCE_GROUP_METRIC(...) \
    __GET_RESOURCE_GROUP_METRIC_MACRO( \
        __VA_ARGS__,                   \
        __GET_RESOURCE_GROUP_METRIC_1, \
        __GET_RESOURCE_GROUP_METRIC_0, \
        __GET_METRIC_0)                \
    (__VA_ARGS__)

#define UPDATE_CUR_AND_MAX_METRIC(family, metric, metric_max)                                       \
    GET_METRIC(family, metric).Increment();                                                         \
    GET_METRIC(family, metric_max)                                                                  \
        .Set(std::max(GET_METRIC(family, metric_max).Value(), GET_METRIC(family, metric).Value())); \
    SCOPE_EXIT({ GET_METRIC(family, metric).Decrement(); })
} // namespace DB
