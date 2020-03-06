#pragma once

#include <Storages/Page/stable/Page.h>
#include <Storages/Page/stable/PageEntries.h>
#include <Storages/Page/stable/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/stable/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/stable/WriteBatch.h>
#include <Storages/Page/stable/mvcc/VersionSet.h>

#include <set>
#include <vector>

namespace DB::stable
{

class PageEntriesVersionSet : public DB::stable::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>
{
public:
    explicit PageEntriesVersionSet(const DB::stable::MVCC::VersionSetConfig & config_, Poco::Logger * log)
        : DB::stable::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>(config_)
    {
        (void)log;
    }

public:
    using SnapshotPtr = DB::stable::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>::SnapshotPtr;

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> gcApply(PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::pair<std::set<PageFileIdAndLevel>, std::set<PageId>> listAllLiveFiles(const std::unique_lock<std::shared_mutex> &) const;
};


} // namespace DB::stable
