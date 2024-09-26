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
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB
{
class StoragePathPool;
using StoragePathPoolPtr = std::shared_ptr<StoragePathPool>;
} // namespace DB


namespace DB::DM
{

struct LocalIndexBuildInfo
{
    DMFiles dm_files;
    size_t estimated_memory_bytes = 0;
    LocalIndexInfosPtr indexes_to_build;

public:
    std::vector<LocalIndexerScheduler::FileID> filesIDs() const
    {
        std::vector<LocalIndexerScheduler::FileID> ids;
        ids.reserve(dm_files.size());
        for (const auto & dmf : dm_files)
        {
            ids.emplace_back(LocalIndexerScheduler::DMFileID(dmf->fileId()));
        }
        return ids;
    }
    std::vector<IndexID> indexesIDs() const
    {
        std::vector<IndexID> ids;
        if (indexes_to_build)
        {
            ids.reserve(indexes_to_build->size());
            for (const auto & index : *indexes_to_build)
            {
                ids.emplace_back(index.index_id);
            }
        }
        return ids;
    }
};

class DMFileIndexWriter
{
public:
    static LocalIndexBuildInfo getLocalIndexBuildInfo(
        const LocalIndexInfosSnapshot & index_infos,
        const DMFiles & dm_files);

    struct Options
    {
        const StoragePathPoolPtr path_pool;
        const LocalIndexInfosPtr index_infos;
        const DMFiles dm_files;
        const DMContext & dm_context;
    };

    using ProceedCheckFn = std::function<bool()>;

    explicit DMFileIndexWriter(const Options & options)
        : logger(Logger::get())
        , options(options)
    {}

    // Note: You cannot call build() multiple times, as duplicate meta version will result in exceptions.
    DMFiles build(ProceedCheckFn should_proceed) const;

    DMFiles build() const
    {
        return build([]() { return true; });
    }

private:
    size_t buildIndexForFile(const DMFilePtr & dm_file_mutable, ProceedCheckFn should_proceed) const;

private:
    const LoggerPtr logger;
    const Options options;
    mutable bool built = false;
};

} // namespace DB::DM
