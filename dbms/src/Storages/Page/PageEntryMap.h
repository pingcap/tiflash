#pragma once

#include <unordered_map>

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>

namespace DB
{

class PageEntryMap
{
public:
    PageEntryMap() = default;

    inline PageEntry & at(const PageId page_id) { return normal_pages.at(resolveRefId(page_id)); }

    inline bool empty() const { return normal_pages.empty(); }

    // Update Page{page_id} / RefPage{page_id} entry
    // If page_id is a ref-id of RefPage, it will
    // find corresponding Page and update that Page,
    // all other RefPages reference to that Page get updated.
    void put(PageId page_id, const PageEntry & entry);

    // Delete RefPage{page_id} and decrease corresponding Page ref-count
    // if origin Page ref-count down to 0, the Page is gone forever
    void del(PageId page_id);

    // Bind RefPage{ref_id} to Page{page_id}
    // If page+id is a ref-id of RefPage, it will
    // find corresponding Page and bind ref-id to that Page.
    void ref(PageId ref_id, PageId page_id);

    inline void clear()
    {
        page_ref.clear();
        normal_pages.clear();
    }

    size_t size() const { return page_ref.size(); }

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

    void decreasePageRef(PageId page_id);

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
        inline PageId            pageId() const { return _iter->first; }
        inline PageEntry &       pageEntry() { return _normal_pages[_iter->second]; }
        inline const PageEntry & pageEntry() const { return _normal_pages[_iter->second]; }

    private:
        std::unordered_map<PageId, PageId>::iterator _iter;
        std::unordered_map<PageId, PageEntry> &      _normal_pages;
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
        inline const PageEntry & pageEntry() const { return _normal_pages[_iter->second]; }

    private:
        std::unordered_map<PageId, PageId>::const_iterator _iter;
        std::unordered_map<PageId, PageEntry> &            _normal_pages;
    };

public:
    /// functions return iterator

    inline iterator       end() { return iterator(page_ref.end(), normal_pages); }
    inline const_iterator end() const { return const_iterator(page_ref.end(), normal_pages); }
    inline iterator       begin() { return iterator(page_ref.begin(), normal_pages); }
    inline const_iterator begin() const { return const_iterator(page_ref.begin(), normal_pages); }

    inline const_iterator cend() const { return const_iterator(page_ref.cend(), normal_pages); }
    inline const_iterator cbegin() { return const_iterator(page_ref.cbegin(), normal_pages); }

    inline iterator       find(const PageId page_id) { return iterator(page_ref.find(page_id), normal_pages); }
    inline const_iterator find(const PageId page_id) const { return const_iterator(page_ref.find(page_id), normal_pages); }

private:
    std::unordered_map<PageId, PageEntry> normal_pages;
    std::unordered_map<PageId, PageId>    page_ref; // RefPageId -> PageId

public:
    // no copying allowed
    PageEntryMap(const PageEntryMap &) = delete;
    PageEntryMap & operator=(const PageEntryMap &) = delete;
    // only move allowed
    PageEntryMap(PageEntryMap && rhs) noexcept : PageEntryMap() { *this = std::move(rhs); }
    PageEntryMap & operator=(PageEntryMap && rhs) noexcept
    {
        if (this != &rhs)
        {
            normal_pages.swap(rhs.normal_pages);
            page_ref.swap(rhs.page_ref);
        }
        return *this;
    }
};

} // namespace DB