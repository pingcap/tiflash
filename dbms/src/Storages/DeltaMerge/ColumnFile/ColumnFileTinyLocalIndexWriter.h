// Copyright 2024 PingCAP, Inc.
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
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>


namespace DB::DM
{

using ColumnFileTinys = std::vector<ColumnFileTinyPtr>;

// ColumnFileTinyLocalIndexWriter write vector index store in PageStorage for ColumnFileTiny.
class ColumnFileTinyLocalIndexWriter
{
public:
    struct LocalIndexBuildInfo
    {
        std::vector<LocalIndexerScheduler::FileID> file_ids;
        std::vector<IndexID> index_ids;
        size_t estimated_memory_bytes = 0;
        LocalIndexInfosPtr indexes_to_build;
    };

    static LocalIndexBuildInfo getLocalIndexBuildInfo(
        const LocalIndexInfosSnapshot & index_infos,
        const ColumnFilePersistedSetPtr & file_set);

    struct Options
    {
        const StoragePoolPtr storage_pool;
        const WriteLimiterPtr write_limiter;
        const ColumnFiles & files;
        const IColumnFileDataProviderPtr data_provider;
        const LocalIndexInfosPtr index_infos;
        WriteBatches & wbs; // Write index and modify meta in the same batch.
    };

    explicit ColumnFileTinyLocalIndexWriter(const Options & options)
        : logger(Logger::get())
        , options(options)
    {}

    // Build vector index for all files in `options.files`.
    // Only return the files that have been built new indexes.
    using ProceedCheckFn = std::function<bool()>; // Return false to stop building index.
    ColumnFileTinys build(ProceedCheckFn should_proceed) const;

private:
    ColumnFileTinyPtr buildIndexForFile(
        const ColumnDefines & column_defines,
        const ColumnDefine & del_cd,
        const ColumnFileTiny * file,
        ProceedCheckFn should_proceed) const;

    const LoggerPtr logger;
    const Options options;
};

} // namespace DB::DM
