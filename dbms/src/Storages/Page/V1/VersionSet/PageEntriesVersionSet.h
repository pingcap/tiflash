#pragma once


#include <Storages/Page/V1/Page.h>
#include <Storages/Page/V1/PageEntries.h>
#include <Storages/Page/V1/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/V1/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/V1/WriteBatch.h>
#include <Storages/Page/V1/mvcc/VersionSet.h>

#include <set>
#include <vector>

namespace DB::PS::V1
{
class PageEntriesVersionSet : public MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>
{
public:
    explicit PageEntriesVersionSet(const MVCC::VersionSetConfig & config_, Poco::Logger * log)
        : MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>(config_)
    {
        (void)log;
    }

public:
    using SnapshotPtr = MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>::SnapshotPtr;

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> listAllLiveFiles(const std::unique_lock<std::shared_mutex> &) const;
};


} // namespace DB::PS::V1
