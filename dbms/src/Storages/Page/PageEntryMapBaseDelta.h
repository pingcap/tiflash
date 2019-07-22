#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include <Common/VersionDeltaSet.h>
#include <Common/VersionSet.h>
#include <IO/WriteHelpers.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class PageEntryMapBaseDelta_t : public ::DB::MVCC::MultiVersionDeltaCountable<PageEntryMapBaseDelta_t>
{
public:
    PageEntryMapBaseDelta_t(bool is_base_)
        : ::DB::MVCC::MultiVersionDeltaCountable<PageEntryMapBaseDelta_t>(),
          normal_pages(),
          page_ref(),
          max_page_id(0),
          is_base(is_base_),
          page_deletions()
    {
    }

    static std::shared_ptr<PageEntryMapBaseDelta_t> createBase()
    {
        return std::make_shared<PageEntryMapBaseDelta_t>(true);
    }

    static std::shared_ptr<PageEntryMapBaseDelta_t> createDelta()
    {
        return std::make_shared<PageEntryMapBaseDelta_t>(false);
    }

    bool isBase() const { return is_base; }

public:
    inline PageEntry & at(const PageId page_id)
    {
        PageId normal_page_id = resolveRefId(page_id);
        auto   iter           = normal_pages.find(normal_page_id);
        if (likely(iter != normal_pages.end()))
        {
            return iter->second;
        }
        else
        {
            throw DB::Exception("Accessing RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                                ErrorCodes::LOGICAL_ERROR);
        }
    }
    inline const PageEntry & at(const PageId page_id) const { return const_cast<PageEntryMapBaseDelta_t *>(this)->at(page_id); }

    inline bool empty() const
    {
        if (is_base)
        {
            return normal_pages.empty() && page_ref.empty();
        }
        else
        {
            return normal_pages.empty() && page_ref.empty() && page_deletions.empty();
        }
    }

    /** Update Page{page_id} / RefPage{page_id} entry. If it's a new page_id,
     *  create a RefPage{page_id} -> Page{page_id} at the same time.
     *  If page_id is a ref-id of RefPage, it will find corresponding Page
     *  and update that Page, all other RefPages reference to that Page get updated.
     */
    void put(PageId page_id, const PageEntry & entry, bool auto_gen_ref = true)
    {
        if (!is_base)
        {
            page_deletions.erase(page_id);
        }
        const PageId normal_page_id = resolveRefId(page_id);
        auto         ori_iter       = normal_pages.find(normal_page_id);
        if (ori_iter == normal_pages.end())
        {
            // Page{normal_page_id} not exist
            normal_pages[normal_page_id]     = entry;
            normal_pages[normal_page_id].ref = 1;
        }
        else
        {
            // replace ori Page{normal_page_id}'s entry but inherit ref-counting
            const UInt32 page_ref_count      = ori_iter->second.ref;
            normal_pages[normal_page_id]     = entry;
            normal_pages[normal_page_id].ref = page_ref_count;
        }
        if (auto_gen_ref)
        {
            // add a RefPage to Page
            page_ref.emplace(page_id, normal_page_id);
        }
        max_page_id = std::max(max_page_id, page_id);
    }

    /** Delete RefPage{page_id} and decrease corresponding Page ref-count.
     *  if origin Page ref-count down to 0, the Page is erased from entry map
     *  template must_exist = true ensure that corresponding Page must exist.
     *           must_exist = false just ignore if that corresponding Page is not exist.
     */
    template <bool must_exist = false>
    void del(PageId page_id);

    /** Bind RefPage{ref_id} to Page{page_id}.
     *  If page_id is a ref-id of RefPage, it will find corresponding Page
     *  and bind ref_id to that Page.
     *  template must_exist = true ensure that corresponding Page must exist.
     *           must_exist = false if corresponding Page not exist, just add a record for RefPage{ref_id} -> Page{page_id}
     */
    template <bool must_exist = false>
    void ref(PageId ref_id, PageId page_id);

    bool isRefExists(PageId ref_id, PageId page_id) const
    {
        const PageId normal_page_id = resolveRefId(page_id);
        const auto   ref_pair       = page_ref.find(ref_id);
        if (ref_pair != page_ref.end())
        {
            return ref_pair->second == normal_page_id;
        }
        else
        {
            // ref_id not exists.
            return false;
        }
    }

    std::pair<bool, PageId> isRefId(PageId page_id) const
    {
        auto ref_pair = page_ref.find(page_id);
        if (ref_pair == page_ref.end())
        {
            return {false, 0UL};
        }
        return {ref_pair->second != page_id, ref_pair->second};
    }

    bool isDeleted(PageId page_id) const { return page_deletions.count(page_id) > 0; }

    inline void clear()
    {
        page_ref.clear();
        normal_pages.clear();
        max_page_id = 0;
        page_deletions.clear();
    }

    size_t size() const { return page_ref.size(); }

    PageId maxId() const { return max_page_id; }

    void merge(PageEntryMapBaseDelta_t & rhs)
    {
        if (!is_base)
        {
            for (auto it : rhs.page_deletions)
                this->del<false>(it);
            for (auto it : rhs.page_ref)
                this->page_ref[it.first] = it.second;
            for (auto it : rhs.normal_pages)
                this->normal_pages[it.first] = it.second;
            this->max_page_id = std::max(this->max_page_id, rhs.max_page_id);
        }
    }

    size_t numDeletions() const {
        assert(!isBase()); // should only call by delta
        return page_deletions.size();
    }

    size_t numEntries() const
    {
        return page_ref.size();
    }

private:
    PageId resolveRefId(PageId page_id) const
    {
        // resolve RefPageId to normal PageId
        // if RefPage3 -> Page1, RefPage4 -> RefPage3
        // resolveRefPage(3) -> 1
        // resolveRefPage(4) -> 1
        for (auto ref_iter = page_ref.find(page_id);                    //
             ref_iter != page_ref.end() && ref_iter->second != page_id; //
             page_id = ref_iter->second)
        {
            // empty for loop
        }
        return page_id;
    }

    template <bool must_exist = true>
    void decreasePageRef(PageId page_id);

    void copyEntries(const PageEntryMapBaseDelta_t & rhs)
    {
        page_ref       = rhs.page_ref;
        normal_pages   = rhs.normal_pages;
        max_page_id    = rhs.max_page_id;
        page_deletions = rhs.page_deletions;
    }

public:
    /// iterator definition

    class iterator
    {
    public:
        iterator(const std::unordered_map<PageId, PageId>::iterator & iter, std::unordered_map<PageId, PageEntry> & normal_pages)
            : _iter(iter), _normal_pages(normal_pages)
        {
        }
        bool operator==(const iterator & rhs) const { return _iter == rhs._iter; }
        bool operator!=(const iterator & rhs) const { return _iter != rhs._iter; }
        // prefix incr
        inline iterator & operator++()
        {
            _iter++;
            return *this;
        }
        // suffix incr
        inline const iterator operator++(int)
        {
            iterator tmp(*this);
            _iter++;
            return tmp;
        }
        inline PageId      pageId() const { return _iter->first; }
        inline PageEntry & pageEntry()
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

    private:
        std::unordered_map<PageId, PageId>::iterator _iter;
        std::unordered_map<PageId, PageEntry> &      _normal_pages;
        friend class PageEntryMapView;
    };

    class const_iterator
    {
    public:
        const_iterator(const std::unordered_map<PageId, PageId>::const_iterator & iter,
                       const std::unordered_map<PageId, PageEntry> &              normal_pages)
            : _iter(iter), _normal_pages(const_cast<std::unordered_map<PageId, PageEntry> &>(normal_pages))
        {
        }
        bool operator==(const const_iterator & rhs) const { return _iter == rhs._iter; }
        bool operator!=(const const_iterator & rhs) const { return _iter != rhs._iter; }
        // prefix incr
        inline const_iterator & operator++()
        {
            _iter++;
            return *this;
        }
        // suffix incr
        inline const const_iterator operator++(int)
        {
            const_iterator tmp(*this);
            _iter++;
            return tmp;
        }
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

    private:
        std::unordered_map<PageId, PageId>::const_iterator _iter;
        std::unordered_map<PageId, PageEntry> &            _normal_pages;
        friend class PageEntryMapView;
    };

public:
    /// functions return iterator

    inline iterator       end() { return iterator(page_ref.end(), normal_pages); }
    inline const_iterator end() const { return const_iterator(page_ref.end(), normal_pages); }

    // read only scan
    inline const_iterator cend() const { return const_iterator(page_ref.cend(), normal_pages); }
    inline const_iterator cbegin() const { return const_iterator(page_ref.cbegin(), normal_pages); }

    inline iterator       find(const PageId page_id) { return iterator(page_ref.find(page_id), normal_pages); }
    inline const_iterator find(const PageId page_id) const { return const_iterator(page_ref.find(page_id), normal_pages); }

    using normal_page_iterator       = std::unordered_map<PageId, PageEntry>::iterator;
    using const_normal_page_iterator = std::unordered_map<PageId, PageEntry>::const_iterator;
    // only scan over normal Pages, excluding RefPages
    inline const_normal_page_iterator pages_cbegin() const { return normal_pages.cbegin(); }
    inline const_normal_page_iterator pages_cend() const { return normal_pages.cend(); }

private:
    std::unordered_map<PageId, PageEntry> normal_pages;
    std::unordered_map<PageId, PageId>    page_ref; // RefPageId -> PageId

    PageId max_page_id;
    bool is_base;

    // deletions
    std::unordered_set<PageId> page_deletions;


public:
    // no copying allowed
    PageEntryMapBaseDelta_t(const PageEntryMapBaseDelta_t &) = delete;
    PageEntryMapBaseDelta_t & operator=(const PageEntryMapBaseDelta_t &) = delete;
    // only move allowed
    PageEntryMapBaseDelta_t(PageEntryMapBaseDelta_t && rhs) noexcept : PageEntryMapBaseDelta_t(true) { *this = std::move(rhs); }
    PageEntryMapBaseDelta_t & operator=(PageEntryMapBaseDelta_t && rhs) noexcept
    {
        if (this != &rhs)
        {
            normal_pages.swap(rhs.normal_pages);
            page_ref.swap(rhs.page_ref);
            max_page_id = rhs.max_page_id;
            is_base = rhs.is_base;
            page_deletions.swap(rhs.page_deletions);
        }
        return *this;
    }

    friend class PageEntryMapVersionSet;
    template <typename Version_t, typename VersionEdit_t, typename Builder_t>
    friend class ::DB::MVCC::VersionSet;
    template <typename VersionBase_t, typename VersionDelta_t, typename VersionView_t, typename VersionEdit_t, typename Builder_t>
    friend class ::DB::MVCC::VersionDeltaSet;
    friend class PageEntryMapBuilder;
    friend class PageEntryMapDeltaBuilder;
    friend class PageEntryMapView;
};

using PageEntryMapBase  = PageEntryMapBaseDelta_t;
using PageEntryMapDelta = PageEntryMapBaseDelta_t;

template <bool must_exist>
void PageEntryMapBaseDelta_t::del(PageId page_id)
{
    if (!is_base)
    {
        page_deletions.insert(page_id);
    }
    // Note: must resolve ref-id before erasing entry in `page_ref`
    const PageId normal_page_id = resolveRefId(page_id);
    page_ref.erase(page_id);

    // decrease origin page's ref counting
    decreasePageRef<must_exist>(normal_page_id);
}

template <bool must_exist>
void PageEntryMapBaseDelta_t::ref(const PageId ref_id, const PageId page_id)
{
    if (!is_base)
    {
        page_deletions.erase(page_id);
    }
    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = resolveRefId(page_id);
    auto         iter           = normal_pages.find(normal_page_id);
    if (likely(iter != normal_pages.end()))
    {
        // if RefPage{ref_id} already exist, release that ref first
        const auto ori_ref = page_ref.find(ref_id);
        if (unlikely(ori_ref != page_ref.end()))
        {
            // if RefPage{ref-id} -> Page{normal_page_id} already exists, just ignore
            if (ori_ref->second == normal_page_id)
                return;
            decreasePageRef<must_exist>(ori_ref->second);
        }
        // build ref
        page_ref[ref_id] = normal_page_id;
        iter->second.ref += 1;
    }
    else
    {
        // The Page to be ref is not exist.
        if constexpr (must_exist)
        {
            throw Exception("Adding RefPage" + DB::toString(ref_id) + " to non-exist Page" + DB::toString(page_id),
                            ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            // else accept dangling ref if we are writing to a tmp entry map.
            // like entry map of WriteBatch or Gc or AnalyzeMeta
            page_ref[ref_id] = normal_page_id;
        }
    }
    max_page_id = std::max(max_page_id, std::max(ref_id, page_id));
}

template <bool must_exist>
void PageEntryMapBaseDelta_t::decreasePageRef(const PageId page_id)
{
    auto iter = normal_pages.find(page_id);
    if constexpr (must_exist)
    {
        assert(iter != normal_pages.end());
    }
    if (iter != normal_pages.end())
    {
        auto & cache = iter->second;
        cache.ref -= 1;
        if (cache.ref == 0)
        {
            normal_pages.erase(iter);
        }
    }
}

} // namespace DB
