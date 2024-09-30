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
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>

#include <memory>

namespace DB
{

class StoragePathPool;
using StoragePathPoolPtr = std::shared_ptr<StoragePathPool>;

class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

namespace DM
{
class StoragePool;
using StoragePoolPtr = std::shared_ptr<StoragePool>;
using NotCompress = std::unordered_set<ColId>;

/**
 * This context object carries table infos. And those infos are only meaningful to current context.
 */
struct DMContext : private boost::noncopyable
{
    const Context & global_context;

    // leaving these pointers possible to be nullptr is dangerous for only reading from/writing to local storage. Find a better way to handle it later
    StoragePathPoolPtr path_pool;
    StoragePoolPtr storage_pool;

    // gc safe-point, maybe update.
    DB::Timestamp min_version;

    const KeyspaceID keyspace_id;
    const TableID physical_table_id;

    /// The user-defined PK column. If multi-column PK, or no PK, it is 0.
    /// Note that user-defined PK will never be _tidb_rowid.
    ///
    /// @warning This field is later added. It is just set to 0 in existing tests
    /// for convenience. If you develop some feature rely on this field, remember
    /// to modify related unit tests.
    const ColumnID pk_col_id;

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

    const ScanContextPtr scan_context;

public:
    static DMContextPtr create(
        const Context & session_context_,
        const StoragePathPoolPtr & path_pool_,
        const StoragePoolPtr & storage_pool_,
        DB::Timestamp min_version_,
        KeyspaceID keyspace_id_,
        TableID physical_table_id_,
        ColumnID pk_col_id_,
        bool is_common_handle_,
        size_t rowkey_column_size_,
        const DB::Settings & settings,
        const ScanContextPtr & scan_context_,
        const String & tracing_id_)
    {
        return std::shared_ptr<DMContext>(new DMContext(
            session_context_,
            path_pool_,
            storage_pool_,
            min_version_,
            keyspace_id_,
            physical_table_id_,
            pk_col_id_,
            is_common_handle_,
            rowkey_column_size_,
            settings,
            scan_context_,
            tracing_id_));
    }

    static std::unique_ptr<DMContext> createUnique(
        const Context & session_context_,
        const StoragePathPoolPtr & path_pool_,
        const StoragePoolPtr & storage_pool_,
        DB::Timestamp min_version_,
        KeyspaceID keyspace_id_,
        TableID physical_table_id_,
        ColumnID pk_col_id_,
        bool is_common_handle_,
        size_t rowkey_column_size_,
        const DB::Settings & settings,
        const ScanContextPtr & scan_context = nullptr)
    {
        return std::unique_ptr<DMContext>(new DMContext(
            session_context_,
            path_pool_,
            storage_pool_,
            min_version_,
            keyspace_id_,
            physical_table_id_,
            pk_col_id_,
            is_common_handle_,
            rowkey_column_size_,
            settings,
            scan_context,
            ""));
    }

    WriteLimiterPtr getWriteLimiter() const;
    ReadLimiterPtr getReadLimiter() const;

    DM::DMConfigurationOpt createChecksumConfig() const { return DMChecksumConfig::fromDBContext(global_context); }

private:
    DMContext(
        const Context & session_context_,
        const StoragePathPoolPtr & path_pool_,
        const StoragePoolPtr & storage_pool_,
        DB::Timestamp min_version_,
        KeyspaceID keyspace_id_,
        TableID physical_table_id_,
        ColumnID pk_col_id_,
        bool is_common_handle_,
        size_t rowkey_column_size_,
        const DB::Settings & settings,
        const ScanContextPtr & scan_context_ = nullptr,
        const String & tracing_id_ = "");
};

} // namespace DM
} // namespace DB
