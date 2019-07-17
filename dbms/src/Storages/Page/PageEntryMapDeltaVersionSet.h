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
                                        PageEntryMapDelta,
                                        PageEntryMapView,
                                        PageEntriesEdit,
                                        PageEntryMapDeltaBuilder>
{
public:
    std::set<PageFileIdAndLevel> gcApply(const PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;

public:
    friend class PageEntryMapView;
    using BaseType
        = ::DB::MVCC::VersionDeltaSet<PageEntryMapBase, PageEntryMapDelta, PageEntryMapView, PageEntriesEdit, PageEntryMapDeltaBuilder>;
};

class PageEntryMapDeltaBuilder
{
public:
    explicit PageEntryMapDeltaBuilder(const PageEntryMapView * base_, //
                                      bool                     ignore_invalid_ref_ = false,
                                      Poco::Logger *           log_                = nullptr);

    ~PageEntryMapDeltaBuilder();

    void apply(const PageEntriesEdit & edit);

    void gcApply(const PageEntriesEdit & edit);

    std::shared_ptr<PageEntryMapDelta> build() { return v; }

    static void mergeDeltaToBase(const std::shared_ptr<PageEntryMapBase> & base, const std::shared_ptr<PageEntryMapDelta> & delta);

    static std::shared_ptr<PageEntryMapDelta> mergeDeltas(PageEntryMapDeltaVersionSet::BaseType *    vset,
                                                          const std::shared_ptr<PageEntryMapDelta> & tail);

private:
    PageEntryMapView *                 base;
    std::shared_ptr<PageEntryMapDelta> v;
    bool                               ignore_invalid_ref;
    Poco::Logger *                     log;
};

class PageEntryMapView
    : public ::DB::MVCC::VersionViewBase<PageEntryMapDeltaVersionSet::BaseType, PageEntryMapDelta, PageEntryMapDeltaBuilder>
{
public:
    class const_iterator
    {
    public:
        explicit const_iterator(PageEntryMapBase::const_iterator cit) : _iter(cit._iter), _normal_pages(cit._normal_pages) {}
        explicit const_iterator(PageEntryMapDelta::const_iterator cit) : _iter(cit._iter), _normal_pages(cit._normal_pages) {}

        inline PageId            pageId() const { return _iter->first; }
        inline const PageEntry & pageEntry() const
        {
            auto iter = _normal_pages.find(_iter->second);
            if (likely(iter != _normal_pages.end()))
            {
                return iter->second;
            }
            else
            {
                throw DB::Exception("Accessing RefPage" + DB::toString(_iter->first) + " to non-exist Page" + DB::toString(_iter->second),
                                    ErrorCodes::LOGICAL_ERROR);
            }
        }

        bool operator==(const const_iterator & rhs) const { return _iter == rhs._iter; }
        bool operator!=(const const_iterator & rhs) const { return _iter != rhs._iter; }

    private:
        std::unordered_map<PageId, PageId>::const_iterator _iter;
        std::unordered_map<PageId, PageEntry> &            _normal_pages;
    };

public:
    PageEntryMapView(PageEntryMapDeltaVersionSet::BaseType * vset_, std::shared_ptr<PageEntryMapDelta> tail_)
        : ::DB::MVCC::VersionViewBase<PageEntryMapDeltaVersionSet::BaseType, PageEntryMapDelta, PageEntryMapDeltaBuilder>(vset_,
                                                                                                                          std::move(tail_))
    {
    }

    const PageEntry & at(const PageId page_id) const;

    PageId maxId() const;

    const_iterator find(PageId page_id) const;

    bool isRefExists(PageId ref_id, PageId page_id) const;

    const_iterator end() const;

    PageEntryMapBase::const_normal_page_iterator pages_cbegin() const;

    PageEntryMapBase::const_normal_page_iterator pages_cend() const;

    PageEntryMapBase::const_iterator cbegin() const;
    PageEntryMapBase::const_iterator cend() const;

private:
    PageId resolveRefId(PageId page_id) const;
};

} // namespace DB
