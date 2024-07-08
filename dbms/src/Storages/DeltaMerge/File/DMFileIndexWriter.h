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
#include <Encryption/FileProvider_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg_fwd.h>
#include <Storages/DeltaMerge/Index/IndexInfo.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB
{
class StoragePathPool;
using StoragePathPoolPtr = std::shared_ptr<StoragePathPool>;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
} // namespace DB

namespace DB::DM
{
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
using DMFiles = std::vector<DMFilePtr>;
} // namespace DB::DM

namespace DB::DM
{

class DMFileIndexWriter
{
public:
    struct LocalIndexBuildInfo
    {
        std::vector<PageIdU64> file_ids;
        size_t estimated_memory_bytes = 0;
        IndexInfosPtr indexes_to_build;
    };

    static LocalIndexBuildInfo getLocalIndexBuildInfo(const IndexInfosPtr & index_infos, const DMFiles & dm_files);

    struct Options
    {
        const StoragePathPoolPtr path_pool;
        const FileProviderPtr file_provider;
        const WriteLimiterPtr write_limiter;
        const SharedContextDisaggPtr disagg_ctx;
        const IndexInfosPtr index_infos;
        const DMFiles dm_files;
        const Context & db_context;
        const bool is_common_handle;
        const size_t rowkey_column_size;
    };

    using ProceedCheckFn = std::function<bool()>;

    explicit DMFileIndexWriter(const Options & options)
        : logger(Logger::get())
        , options(options)
    {}

    // Note: You cannot call build() multiple times, as duplicate meta version will result in exceptions.
    // TODO: Add a better guard.
    DMFiles build(ProceedCheckFn should_proceed) const;

    DMFiles build() const
    {
        return build([]() { return true; });
    }

private:
    void buildIndexForFile(const DMFilePtr & dm_file_mutable, ProceedCheckFn should_proceed) const;

private:
    const LoggerPtr logger;
    const Options options;
};

} // namespace DB::DM
