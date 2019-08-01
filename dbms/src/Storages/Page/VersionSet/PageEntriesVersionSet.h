#pragma once

#include <set>
#include <vector>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageEntries.h>
#include <Storages/Page/VersionSet/PageEntriesBuilder.h>
#include <Storages/Page/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/mvcc/VersionSet.h>

namespace DB
{

class PageEntriesVersionSet : public ::DB::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>
{
public:
    explicit PageEntriesVersionSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig())
        : ::DB::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>(config_)
    {
    }

public:
    using SnapshotPtr = ::DB::MVCC::VersionSet<PageEntries, PageEntriesEdit, PageEntriesBuilder>::SnapshotPtr;

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    std::set<PageFileIdAndLevel> gcApply(PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;
};


} // namespace DB
