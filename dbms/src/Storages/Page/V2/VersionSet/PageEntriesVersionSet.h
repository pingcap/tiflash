#pragma once

#include <Storages/Page/V2/Page.h>
#include <Storages/Page/V2/PageEntries.h>
#include <Storages/Page/V2/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/V2/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/V2/WriteBatch.h>
#include <Storages/Page/V2/mvcc/VersionSet.h>

#include <set>
#include <vector>

namespace DB::PS::V2
{
class PageEntriesVersionSet : public MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>
{
public:
    explicit PageEntriesVersionSet(String name_, const MVCC::VersionSetConfig & config_, Poco::Logger * log)
        : MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>(config_)
        , storage_name(std::move(name_))
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

private:
    const String storage_name;
};


} // namespace DB::PS::V2
