#pragma once

#include <Common/VersionDeltaSet.h>
#include <Common/VersionSet.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/PageEntryMapBaseDelta.h>
#include <Storages/Page/PageEntryMapVersionSet.h>

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
    explicit PageEntryMapDeltaVersionSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig())
        : ::DB::MVCC::VersionDeltaSet<PageEntryMapBase, PageEntryMapView, PageEntriesEdit, PageEntryMapDeltaBuilder>(config_)
    {
    }

public:
    std::set<PageFileIdAndLevel> gcApply(const PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;

public:
    friend class PageEntryMapView;
    using BaseType    = ::DB::MVCC::VersionDeltaSet<PageEntryMapBase, PageEntryMapView, PageEntriesEdit, PageEntryMapDeltaBuilder>;
    using BuilderType = BaseType::BuilderType;
    using VersionType = BaseType::VersionType;
    using VersionPtr  = BaseType::VersionPtr;
};

class PageEntryMapDeltaBuilder
{
public:
    explicit PageEntryMapDeltaBuilder(const PageEntryMapView * base_, //
                                      bool                     ignore_invalid_ref_ = false,
                                      Poco::Logger *           log_                = nullptr);

    ~PageEntryMapDeltaBuilder();

    void apply(PageEntriesEdit & edit);

    void gcApply(const PageEntriesEdit & edit);

    PageEntryMapDeltaVersionSet::VersionPtr build() { return v; }

    static void                                    mergeDeltaToBaseInplace(const PageEntryMapDeltaVersionSet::VersionPtr & base,
                                                                           const PageEntryMapDeltaVersionSet::VersionPtr & delta);
    static PageEntryMapDeltaVersionSet::VersionPtr compactDeltaAndBase(const PageEntryMapDeltaVersionSet::VersionPtr & old_base,
                                                                       PageEntryMapDeltaVersionSet::VersionPtr &       delta);

    static PageEntryMapDeltaVersionSet::VersionPtr compactDeltas(PageEntryMapDeltaVersionSet::BaseType *         vset,
                                                                 const PageEntryMapDeltaVersionSet::VersionPtr & tail);

    static bool needCompactToBase(const PageEntryMapDeltaVersionSet::BaseType *   vset,
                                  const PageEntryMapDeltaVersionSet::VersionPtr & delta);

private:
    PageEntryMapView *                      base;
    PageEntryMapDeltaVersionSet::VersionPtr v;
    bool                                    ignore_invalid_ref;
    Poco::Logger *                          log;
};

class PageEntryMapView : public ::DB::MVCC::VersionViewBase<PageEntryMapDeltaVersionSet::BaseType, PageEntryMapDeltaBuilder>
{
public:
    PageEntryMapView(PageEntryMapDeltaVersionSet::BaseType * vset_, PageEntryMapDeltaVersionSet::VersionPtr tail_)
        : ::DB::MVCC::VersionViewBase<PageEntryMapDeltaVersionSet::BaseType, PageEntryMapDeltaBuilder>(vset_, std::move(tail_))
    {
    }

    const PageEntry & at(const PageId page_id) const;

    PageId maxId() const;

    const PageEntry* find(PageId page_id) const;

    bool isRefExists(PageId ref_id, PageId page_id) const;

    std::set<PageId> validPageIds() const;

    std::set<PageId> validNormalPageIds() const;

    PageId resolveRefId(PageId page_id) const;
};

} // namespace DB
