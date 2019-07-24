#pragma once

#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/PageEntryMapVersionSet.h>
#include <Storages/Page/mvcc/VersionDeltaSet.h>
#include <Storages/Page/mvcc/VersionSet.h>

namespace DB
{

class PageEntryMapView;

class PageEntryMapDeltaBuilder;

class PageEntryMapDeltaVersionSet : public ::DB::MVCC::VersionDeltaSet< //
                                        PageEntryMapBase,
                                        PageEntryMapView,
                                        PageEntriesEdit,
                                        PageEntryMapDeltaBuilder>
{
public:
    using BaseType    = ::DB::MVCC::VersionDeltaSet<PageEntryMapBase, PageEntryMapView, PageEntriesEdit, PageEntryMapDeltaBuilder>;
    using BuilderType = BaseType::BuilderType;
    using VersionType = BaseType::VersionType;
    using VersionPtr  = BaseType::VersionPtr;

public:
    explicit PageEntryMapDeltaVersionSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig()) : BaseType(config_)
    {
    }

public:
    std::set<PageFileIdAndLevel> gcApply(PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;

private:
    void collectLiveFilesFromVersionList(VersionPtr tail, std::set<PageFileIdAndLevel> &liveFiles) const;
};

class PageEntryMapDeltaBuilder
{
public:
    explicit PageEntryMapDeltaBuilder(const PageEntryMapView * base_, //
                                      bool                     ignore_invalid_ref_ = false,
                                      Poco::Logger *           log_                = nullptr);

    ~PageEntryMapDeltaBuilder();

    void apply(PageEntriesEdit & edit);

    void gcApply(PageEntriesEdit & edit);

    static void applyInplace( //
        const PageEntryMapDeltaVersionSet::VersionPtr & current,
        const PageEntriesEdit &                         edit);

    static void gcApplyInplace(//
        const PageEntryMapDeltaVersionSet::VersionPtr & current,
        const PageEntriesEdit & edit);

    // Functions used when view release and do compact on version-list

    static PageEntryMapDeltaVersionSet::VersionPtr //
    compactDeltas(const PageEntryMapDeltaVersionSet::VersionPtr & tail);

    inline static bool //
    needCompactToBase( //
        const ::DB::MVCC::VersionSetConfig &            config,
        const PageEntryMapDeltaVersionSet::VersionPtr & delta)
    {
        assert(!delta->isBase());
        return delta->numDeletions() >= config.compact_hint_delta_deletions //
            || delta->numRefEntries() >= config.compact_hint_delta_entries
            || delta->numNormalEntries() >= config.compact_hint_delta_entries;
    }

    static PageEntryMapDeltaVersionSet::VersionPtr //
    compactDeltaAndBase(                           //
        const PageEntryMapDeltaVersionSet::VersionPtr & old_base,
        PageEntryMapDeltaVersionSet::VersionPtr &       delta);

private:

    void decreasePageRef(PageId page_id);

private:
    PageEntryMapView *                      base;
    PageEntryMapDeltaVersionSet::VersionPtr v;
    bool                                    ignore_invalid_ref;
    Poco::Logger *                          log;
};

using PageEntryMapViewBaseType = ::DB::MVCC::VersionViewBase<PageEntryMapDeltaVersionSet::BaseType, PageEntryMapDeltaBuilder>;

class PageEntryMapView : public PageEntryMapViewBaseType
{
public:
    PageEntryMapView(PageEntryMapDeltaVersionSet::BaseType * vset_, PageEntryMapDeltaVersionSet::VersionPtr tail_)
        : PageEntryMapViewBaseType(vset_, std::move(tail_))
    {
    }

    PageId maxId() const;

    const PageEntry & at(PageId page_id) const;

    const PageEntry * find(PageId page_id) const;

    bool isRefExists(PageId ref_id, PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id);

    PageId resolveRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId> validNormalPageIds() const;

private:
    const PageEntry * findNormalPageEntry(PageId page_id) const;

    friend class PageEntryMapDeltaBuilder;
};

} // namespace DB
