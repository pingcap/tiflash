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

#include <Common/Logger.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <memory>

namespace DB
{
class StoragePathPool;


namespace DM
{
class StoragePool;
using NotCompress = std::unordered_set<ColId>;
struct DMContext;
using DMContextPtr = std::shared_ptr<DMContext>;

/**
 * This context object carries table infos. And those infos are only meaningful to current context.
 */
struct DMContext : private boost::noncopyable
{
    const Context & db_context;

    StoragePathPool & path_pool;
    StoragePool & storage_pool;

    // gc safe-point, maybe update.
    DB::Timestamp min_version;

    const NotCompress & not_compress; // Not used currently.

    bool is_common_handle;

    // The number of columns in primary key if is_common_handle = true, otherwise, should always be 1.
    size_t rowkey_column_size;
    // The base rows of segment.
    const size_t segment_limit_rows;
    // The base bytes of segment.
    const size_t segment_limit_bytes;
    // The bytes threshold of fg split segment.
    const size_t segment_force_split_bytes;
    // The rows threshold of delta.
    const size_t delta_limit_rows;
    // The bytes threshold of delta.
    const size_t delta_limit_bytes;
    // The threshold of cache in delta.
    const size_t delta_cache_limit_rows;
    // The size threshold of cache in delta.
    const size_t delta_cache_limit_bytes;
    // Determine whether a column file is small or not in rows.
    const size_t delta_small_column_file_rows;
    // Determine whether a column file is small or not in bytes.
    const size_t delta_small_column_file_bytes;
    // The expected stable pack rows.
    const size_t stable_pack_rows;

    // The number of points to check for calculating region split.
    const size_t region_split_check_points = 128;

    const bool enable_logical_split;
    const bool read_delta_only;
    const bool read_stable_only;
    const bool enable_relevant_place;
    const bool enable_skippable_place;

    String tracing_id;

    ScanContextPtr scan_context;

public:
    DMContext(const Context & db_context_,
              StoragePathPool & path_pool_,
              StoragePool & storage_pool_,
              const DB::Timestamp min_version_,
              const NotCompress & not_compress_,
              bool is_common_handle_,
              size_t rowkey_column_size_,
              const DB::Settings & settings,
              const ScanContextPtr & scan_context_ = std::make_shared<ScanContext>(),
              const String & tracing_id_ = "")
        : db_context(db_context_)
        , path_pool(path_pool_)
        , storage_pool(storage_pool_)
        , min_version(min_version_)
        , not_compress(not_compress_)
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
        , scan_context(scan_context_)
    {
    }

    WriteLimiterPtr getWriteLimiter() const { return db_context.getWriteLimiter(); }
    ReadLimiterPtr getReadLimiter() const { return db_context.getReadLimiter(); }
    DM::DMConfigurationOpt createChecksumConfig(bool is_single_file) const
    {
        return DMChecksumConfig::fromDBContext(db_context, is_single_file);
    }
};

} // namespace DM
} // namespace DB
