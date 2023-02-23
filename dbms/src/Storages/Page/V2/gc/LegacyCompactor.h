// Copyright 2022 PingCAP, Ltd.
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

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/WriteBatch.h>

#include <boost/core/noncopyable.hpp>
#include <tuple>

namespace DB
{
class PSDiskDelegator;
namespace PS::V2
{
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
using WritingFilesSnapshot = PageStorage::WritingFilesSnapshot;

class LegacyCompactor : private boost::noncopyable
{
public:
    LegacyCompactor(const PageStorage & storage, const WriteLimiterPtr & write_limiter_, const ReadLimiterPtr & read_limiter_);

    std::tuple<PageFileSet, PageFileSet, size_t> //
    tryCompact(PageFileSet && page_files, const WritingFilesSnapshot & writing_files);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    // Return values: [files to remove, files to compact, compact sequence id]
    std::tuple<PageFileSet, PageFileSet, WriteBatch::SequenceID, std::optional<PageFile>> //
    collectPageFilesToCompact(const PageFileSet & page_files, const WritingFilesSnapshot & writing_files);

    static WriteBatch prepareCheckpointWriteBatch(
        const PageStorage::ConcreteSnapshotPtr & snapshot,
        const WriteBatch::SequenceID wb_sequence);
    [[nodiscard]] static size_t writeToCheckpoint(
        const String & storage_path,
        const PageFileIdAndLevel & file_id,
        WriteBatch && wb,
        FileProviderPtr & file_provider,
        Poco::Logger * log,
        const WriteLimiterPtr & write_limiter);
#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    const String & storage_name;

    PSDiskDelegatorPtr delegator;
    FileProviderPtr file_provider;

    const PageStorageConfig & config;

    Poco::Logger * log;
    Poco::Logger * page_file_log;

    PageStorage::VersionedPageEntries version_set;
    PageStorage::StatisticsInfo info;

    const WriteLimiterPtr write_limiter;
    const ReadLimiterPtr read_limiter;
};

} // namespace PS::V2
} // namespace DB
