// Copyright 2023 PingCAP, Inc.
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

#include <Common/CurrentMetrics.h>
#include <Common/nocopyable.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V2/PageDefines.h>
#include <common/likely.h>
#include <common/logger_useful.h>

#include <cassert>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>


namespace CurrentMetrics
{
extern const int PSMVCCNumDelta;
extern const int PSMVCCNumBase;
} // namespace CurrentMetrics
namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace PS::V2
{
/// Base type for VersionType of VersionSet
template <typename T>
struct MultiVersionCountable
{
public:
    uint32_t ref_count;
    T * next;
    T * prev;

public:
    explicit MultiVersionCountable(T * self)
        : ref_count(0)
        , next(self)
        , prev(self)
    {}
    virtual ~MultiVersionCountable()
    {
        assert(ref_count == 0);

        // Remove from linked list
        prev->next = next;
        next->prev = prev;
    }

    void increase(const std::unique_lock<std::shared_mutex> & lock)
    {
        (void)lock;
        ++ref_count;
    }

    void release(const std::unique_lock<std::shared_mutex> & lock)
    {
        (void)lock;
        assert(ref_count >= 1);
        if (--ref_count == 0)
        {
            // in case two neighbor nodes remove from linked list
            delete this;
        }
    }

    // Not thread-safe function. Only for VersionSet::Builder.

    // Not thread-safe, caller ensure.
    void increase() { ++ref_count; }

    // Not thread-safe, caller ensure.
    void release()
    {
        assert(ref_count >= 1);
        if (--ref_count == 0)
        {
            delete this; // remove this node from version set
        }
    }
};


/// Base type for VersionType of VersionSetWithDelta
template <typename T>
struct MultiVersionCountableForDelta
{
public:
    std::shared_ptr<T> prev;

public:
    explicit MultiVersionCountableForDelta()
        : prev(nullptr)
    {}

    virtual ~MultiVersionCountableForDelta() = default;
};

template <typename T>
class PageEntriesMixin
{
public:
    explicit PageEntriesMixin(bool is_base_)
        : max_page_id(0)
        , is_base(is_base_)
    {
        if (is_base)
        {
            CurrentMetrics::add(CurrentMetrics::PSMVCCNumBase);
        }
        else
        {
            CurrentMetrics::add(CurrentMetrics::PSMVCCNumDelta);
        }
    }

    virtual ~PageEntriesMixin()
    {
        if (is_base)
        {
            CurrentMetrics::sub(CurrentMetrics::PSMVCCNumBase);
        }
        else
        {
            CurrentMetrics::sub(CurrentMetrics::PSMVCCNumDelta);
        }
    }

public:
    static std::shared_ptr<T> createBase() { return std::make_shared<T>(true); }

    static std::shared_ptr<T> createDelta() { return std::make_shared<T>(false); }

    bool isBase() const { return is_base; }

public:
    /** Update Page{page_id} / RefPage{page_id} entry. If it's a new page_id,
     *  create a RefPage{page_id} -> Page{page_id} at the same time.
     *  If page_id is a ref-id of RefPage, it will find corresponding Page
     *  and update that Page, all other RefPages reference to that Page get updated.
     */
    void put(PageId page_id, const PageEntry & entry);

    /** Create or Update Page{normal_page_id}'s entry, if the entry is existed, this method
     *  will inherit the ref-counting of old entry, otherwise the ref count will be set to 0.
     *  Compare to method `put`, this method won't create RefPage{page_id} -> Page{page_id}.
     */
    void upsertPage(PageId normal_page_id, PageEntry entry);

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
    void ref(PageId ref_id, PageId page_id);

    inline std::optional<PageEntry> find(const PageId page_id) const
    {
        auto ref_iter = page_ref.find(page_id);
        if (ref_iter == page_ref.end())
            return std::nullopt;
        else
        {
            auto normal_iter = normal_pages.find(ref_iter->second);
            if (normal_iter == normal_pages.end())
                return std::nullopt;
            else
                return normal_iter->second;
        }
    }

    inline std::optional<PageEntry> findNormalPageEntry(PageId page_id) const
    {
        auto iter = normal_pages.find(page_id);
        if (iter == normal_pages.end())
            return std::nullopt;
        else
            return iter->second;
    }

    inline PageEntry & at(const PageId page_id)
    {
        PageId normal_page_id = resolveRefId(page_id);
        auto iter = normal_pages.find(normal_page_id);
        if (likely(iter != normal_pages.end()))
        {
            return iter->second;
        }
        else
        {
            throw DB::Exception(
                "Accessing RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(normal_page_id),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    inline const PageEntry & at(const PageId page_id) const
    {
        return const_cast<PageEntriesMixin *>(this)->at(page_id);
    }

    inline std::pair<bool, PageId> isRefId(PageId page_id) const
    {
        auto ref_pair = page_ref.find(page_id);
        if (ref_pair == page_ref.end())
        {
            return {false, 0UL};
        }
        return {true, ref_pair->second};
    }

    inline void clear()
    {
        page_ref.clear();
        normal_pages.clear();
        max_page_id = 0;
        ref_deletions.clear();
    }

    PageId maxId() const { return max_page_id; }

public:
    using const_normal_page_iterator = std::unordered_map<PageId, PageEntry>::const_iterator;
    // only scan over normal Pages, excluding RefPages
    inline const_normal_page_iterator pages_cbegin() const { return normal_pages.cbegin(); }
    inline const_normal_page_iterator pages_cend() const { return normal_pages.cend(); }

protected:
    std::unordered_map<PageId, PageEntry> normal_pages;
    std::unordered_map<PageId, PageId> page_ref; // RefPageId -> PageId
    // RefPageId deletions
    std::unordered_set<PageId> ref_deletions;

    PageId max_page_id;
    bool is_base;

protected:
    size_t numDeletions() const
    {
        assert(!isBase()); // should only call by delta
        return ref_deletions.size();
    }

    size_t numRefEntries() const { return page_ref.size(); }

    size_t numNormalEntries() const { return normal_pages.size(); }

    inline bool isRefDeleted(PageId page_id) const { return ref_deletions.count(page_id) > 0; }

protected:
    template <bool must_exist = true>
    void decreasePageRef(PageId page_id, bool keep_tombstone);

    void copyEntries(const PageEntriesMixin & rhs)
    {
        page_ref = rhs.page_ref;
        normal_pages = rhs.normal_pages;
        max_page_id = rhs.max_page_id;
        ref_deletions = rhs.ref_deletions;
    }

private:
    PageId resolveRefId(PageId page_id) const
    {
        // resolve RefPageId to normal PageId
        // if RefPage3 -> Page1, RefPage4 -> RefPage3
        // resolveRefId(3) -> 1
        // resolveRefId(4) -> 1
        auto [is_ref, normal_page_id] = isRefId(page_id);
        return is_ref ? normal_page_id : page_id;
    }

public:
    // no copying allowed
    DISALLOW_COPY(PageEntriesMixin);
    // only move allowed
    PageEntriesMixin(PageEntriesMixin && rhs) noexcept
        : PageEntriesMixin(true)
    {
        *this = std::move(rhs);
    }
    PageEntriesMixin & operator=(PageEntriesMixin && rhs) noexcept
    {
        if (this != &rhs)
        {
            normal_pages.swap(rhs.normal_pages);
            page_ref.swap(rhs.page_ref);
            max_page_id = rhs.max_page_id;
            is_base = rhs.is_base;
            ref_deletions.swap(rhs.ref_deletions);
        }
        return *this;
    }

    friend class PageEntriesBuilder;
    friend class DeltaVersionEditAcceptor;
    friend class PageEntriesView;
};

template <typename T>
void PageEntriesMixin<T>::put(PageId page_id, const PageEntry & entry)
{
    assert(is_base); // can only call by base
    const PageId normal_page_id = resolveRefId(page_id);

    // update ref-pairs
    bool is_new_ref_pair_inserted = false;
    {
        // add a RefPage to Page
        auto res = page_ref.emplace(page_id, normal_page_id);
        is_new_ref_pair_inserted = res.second;
    }

    // update normal page's entry
    auto ori_iter = normal_pages.find(normal_page_id);
    if (ori_iter == normal_pages.end())
    {
        // Page{normal_page_id} not exist
        normal_pages[normal_page_id] = entry;
        normal_pages[normal_page_id].ref = 1;
    }
    else
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        const UInt32 page_ref_count = ori_iter->second.ref;
        normal_pages[normal_page_id] = entry;
        normal_pages[normal_page_id].ref = page_ref_count + is_new_ref_pair_inserted;
    }

    // update max_page_id
    max_page_id = std::max(max_page_id, page_id);
}

template <typename T>
void PageEntriesMixin<T>::upsertPage(PageId normal_page_id, PageEntry entry)
{
    assert(is_base); // can only call by base

    // update normal page's entry
    auto ori_iter = normal_pages.find(normal_page_id);
    if (likely(ori_iter != normal_pages.end()))
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        const UInt32 page_ref_count = ori_iter->second.ref;
        entry.ref = page_ref_count;
        normal_pages[normal_page_id] = entry;
    }
    else
    {
        // Page{normal_page_id} not exist
        entry.ref = 0;
        normal_pages[normal_page_id] = entry;
    }

    // update max_page_id
    max_page_id = std::max(max_page_id, normal_page_id);
}

template <typename T>
template <bool must_exist>
void PageEntriesMixin<T>::del(PageId page_id)
{
    assert(is_base); // can only call by base
    // Note: must resolve ref-id before erasing entry in `page_ref`
    const PageId normal_page_id = resolveRefId(page_id);

    const size_t num_erase = page_ref.erase(page_id);
    if (num_erase > 0)
    {
        // decrease origin page's ref counting, this method can
        // only called by base, so we should remove the entry if
        // the ref count down to zero
        decreasePageRef<must_exist>(normal_page_id, /*keep_tombstone=*/false);
    }
}

template <typename T>
void PageEntriesMixin<T>::ref(const PageId ref_id, const PageId page_id)
{
    assert(is_base); // can only call by base
    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = resolveRefId(page_id);
    auto iter = normal_pages.find(normal_page_id);
    if (likely(iter != normal_pages.end()))
    {
        // if RefPage{ref_id} already exist, release that ref first
        const auto ori_ref = page_ref.find(ref_id);
        if (unlikely(ori_ref != page_ref.end()))
        {
            // if RefPage{ref-id} -> Page{normal_page_id} already exists, just ignore
            if (ori_ref->second == normal_page_id)
                return;
            // this method can only called by base, so we should remove the entry if
            // the ref count down to zero
            decreasePageRef<true>(ori_ref->second, /*keep_tombstone=*/false);
        }
        // build ref
        page_ref[ref_id] = normal_page_id;
        iter->second.ref += 1;
    }
    else
    {
        // The Page to be ref is not exist.
        throw Exception(
            "Adding RefPage" + DB::toString(ref_id) + " to non-exist Page" + DB::toString(page_id),
            ErrorCodes::LOGICAL_ERROR);
    }
    max_page_id = std::max(max_page_id, std::max(ref_id, page_id));
}

template <typename T>
template <bool must_exist>
void PageEntriesMixin<T>::decreasePageRef(const PageId page_id, bool keep_tombstone)
{
    auto iter = normal_pages.find(page_id);
    if constexpr (must_exist)
    {
        if (unlikely(iter == normal_pages.end()))
        {
            throw Exception(
                "Decreasing non-exist normal page[" + DB::toString(page_id) + "] ref-count",
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    if (iter != normal_pages.end())
    {
        auto & entry = iter->second;
        if (entry.ref > 0)
        {
            entry.ref -= 1;
        }
        if (!keep_tombstone && entry.ref == 0)
        {
            normal_pages.erase(iter);
        }
    }
}

/// For PageEntriesVersionSet
class PageEntries
    : public PageEntriesMixin<PageEntries>
    , public MultiVersionCountable<PageEntries>
{
public:
    explicit PageEntries(bool is_base_ = true)
        : PageEntriesMixin(true)
        , MultiVersionCountable<PageEntries>(this)
    {
        (void)is_base_;
    }

public:
    /// Iterator definition. Used for scan over all RefPages / NormalPages

    class iterator
    {
    public:
        iterator(
            const std::unordered_map<PageId, PageId>::iterator & iter,
            std::unordered_map<PageId, PageEntry> & normal_pages)
            : _iter(iter)
            , _normal_pages(normal_pages)
        {}
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
        inline PageId pageId() const { return _iter->first; }
        inline PageEntry & pageEntry()
        {
            auto iter = _normal_pages.find(_iter->second);
            if (likely(iter != _normal_pages.end()))
            {
                return iter->second;
            }
            else
            {
                throw DB::Exception(
                    "Accessing RefPage" + DB::toString(_iter->first) + " to non-exist Page"
                        + DB::toString(_iter->second),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }

    private:
        std::unordered_map<PageId, PageId>::iterator _iter;
        std::unordered_map<PageId, PageEntry> & _normal_pages;
        friend class PageEntriesView;
    };

    class const_iterator
    {
    public:
        const_iterator(
            const std::unordered_map<PageId, PageId>::const_iterator & iter,
            const std::unordered_map<PageId, PageEntry> & normal_pages)
            : _iter(iter)
            , _normal_pages(const_cast<std::unordered_map<PageId, PageEntry> &>(normal_pages))
        {}
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
        inline PageId pageId() const { return _iter->first; }
        inline const PageEntry & pageEntry() const
        {
            auto iter = _normal_pages.find(_iter->second);
            if (likely(iter != _normal_pages.end()))
            {
                return iter->second;
            }
            else
            {
                throw DB::Exception(
                    "Accessing RefPage" + DB::toString(_iter->first) + " to non-exist Page"
                        + DB::toString(_iter->second),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }

    private:
        std::unordered_map<PageId, PageId>::const_iterator _iter;
        std::unordered_map<PageId, PageEntry> & _normal_pages;
        friend class PageEntriesView;
    };

public:
    // Iterator to scan over all ref/normal pages (read only)
    inline const_iterator cend() const { return const_iterator(page_ref.cend(), normal_pages); }
    inline const_iterator cbegin() const { return const_iterator(page_ref.cbegin(), normal_pages); }
};

/// For PageEntriesVersionSetWithDelta
class PageEntriesForDelta;
using PageEntriesForDeltaPtr = std::shared_ptr<PageEntriesForDelta>;
class PageEntriesForDelta
    : public PageEntriesMixin<PageEntriesForDelta>
    , public MultiVersionCountableForDelta<PageEntriesForDelta>
{
public:
    explicit PageEntriesForDelta(bool is_base_)
        : PageEntriesMixin(is_base_)
        , MultiVersionCountableForDelta<PageEntriesForDelta>()
    {}

    bool shouldCompactToBase(const MVCC::VersionSetConfig & config)
    {
        assert(!this->isBase());
        return numDeletions() >= config.compact_hint_delta_deletions //
            || numRefEntries() >= config.compact_hint_delta_entries
            || numNormalEntries() >= config.compact_hint_delta_entries;
    }

    //==========================================================================================
    // Functions used when view release and do compact on version-list
    //==========================================================================================

    static PageEntriesForDeltaPtr compactDeltaAndBase( //
        const PageEntriesForDeltaPtr & old_base,
        const PageEntriesForDeltaPtr & delta)
    {
        PageEntriesForDeltaPtr base = createBase();
        base->copyEntries(*old_base);
        // apply delta edits
        base->merge(*delta);
        return base;
    }

    static PageEntriesForDeltaPtr compactDeltas(const PageEntriesForDeltaPtr & tail)
    {
        if (auto prev = std::atomic_load(&tail->prev); prev == nullptr || prev->isBase())
        {
            // Only one delta, do nothing
            return nullptr;
        }

        auto tmp = createDelta();

        std::stack<PageEntriesForDeltaPtr> nodes;
        for (auto node = tail; node != nullptr; node = std::atomic_load(&node->prev))
        {
            if (node->isBase())
            {
                // link `tmp` to `base` version
                tmp->prev = node;
            }
            else
            {
                nodes.push(node);
            }
        }
        // merge delta forward
        while (!nodes.empty())
        {
            auto node = nodes.top();
            tmp->merge(*node);
            nodes.pop();
        }

        return tmp;
    }

private:
    void merge(PageEntriesForDelta & rhs)
    {
        // TODO we need more test on this function
        assert(!rhs.isBase()); // rhs must be delta
        for (auto page_id : rhs.ref_deletions)
        {
            page_ref.erase(page_id);
            if (!is_base)
            {
                ref_deletions.insert(page_id);
            }
            // If this is the base version, we should remove the entry if
            // the ref count down to zero. Otherwise it is the delta version
            // we should keep a tombstone.
            decreasePageRef<false>(page_id, /*keep_tombstone=*/!this->isBase());
        }
        for (auto it : rhs.page_ref)
        {
            page_ref[it.first] = it.second;
        }
        for (auto it : rhs.normal_pages)
        {
            if (it.second.isTombstone() && is_base)
            {
                // A tombstone of normal page, delete this page
                normal_pages.erase(it.first);
            }
            else
            {
                normal_pages[it.first] = it.second;
            }
        }
        max_page_id = std::max(max_page_id, rhs.max_page_id);
    }
};
} // namespace PS::V2
} // namespace DB
