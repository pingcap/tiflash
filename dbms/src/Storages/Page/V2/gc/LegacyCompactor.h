#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V2/WriteBatch.h>

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

    static WriteBatch prepareCheckpointWriteBatch(const PageStorage::SnapshotPtr snapshot, const WriteBatch::SequenceID wb_sequence);
    [[nodiscard]] static size_t writeToCheckpoint(const String & storage_path,
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

    const PageStorage::Config & config;

    Poco::Logger * log;
    Poco::Logger * page_file_log;

    PageStorage::VersionedPageEntries version_set;
    PageStorage::StatisticsInfo info;

    const WriteLimiterPtr write_limiter;
    const ReadLimiterPtr read_limiter;
};

} // namespace PS::V2
} // namespace DB