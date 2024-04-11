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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB::DM
{

WriteLimiterPtr DMContext::getWriteLimiter() const
{
    return global_context.getWriteLimiter();
}

ReadLimiterPtr DMContext::getReadLimiter() const
{
    return global_context.getReadLimiter();
}

DMContext::DMContext(
    const Context & session_context_,
    const StoragePathPoolPtr & path_pool_,
    const StoragePoolPtr & storage_pool_,
    const DB::Timestamp min_version_,
    KeyspaceID keyspace_id_,
    TableID physical_table_id_,
    bool is_common_handle_,
    size_t rowkey_column_size_,
    const DB::Settings & settings,
    const ScanContextPtr & scan_context_,
    const String & tracing_id_)
    : global_context(session_context_.getGlobalContext()) // always save the global context
    , path_pool(path_pool_)
    , storage_pool(storage_pool_)
    , min_version(min_version_)
    , keyspace_id(keyspace_id_)
    , physical_table_id(physical_table_id_)
    , is_common_handle(is_common_handle_)
    , rowkey_column_size(rowkey_column_size_)
    , segment_limit_rows(settings.dt_segment_limit_rows)
    , segment_limit_bytes(settings.dt_segment_limit_size)
    , segment_force_split_bytes(settings.dt_segment_force_split_size)
    , delta_limit_rows(settings.dt_segment_delta_limit_rows)
    , delta_limit_bytes(settings.dt_segment_delta_limit_size)
    , delta_cache_limit_rows(settings.dt_segment_delta_cache_limit_rows)
    , delta_cache_limit_bytes(settings.dt_segment_delta_cache_limit_size)
    , delta_small_column_file_rows(settings.dt_segment_delta_small_column_file_rows)
    , delta_small_column_file_bytes(settings.dt_segment_delta_small_column_file_size)
    , stable_pack_rows(settings.dt_segment_stable_pack_rows)
    , enable_logical_split(settings.dt_enable_logical_split)
    , read_delta_only(settings.dt_read_delta_only)
    , read_stable_only(settings.dt_read_stable_only)
    , enable_relevant_place(settings.dt_enable_relevant_place)
    , enable_skippable_place(settings.dt_enable_skippable_place)
    , tracing_id(tracing_id_)
    , scan_context(scan_context_ ? scan_context_ : std::make_shared<ScanContext>())
{}

} // namespace DB::DM
