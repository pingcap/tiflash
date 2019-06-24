#pragma once

#include <unordered_map>

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefines.h>
#include <common/likely.h>

namespace DB
{

using MemHolder = std::shared_ptr<char>;
inline MemHolder createMemHolder(char * memory, const std::function<void(char *)> & free)
{
    return std::shared_ptr<char>(memory, free);
}

struct Page
{
    PageId     page_id;
    ByteBuffer data;

    MemHolder mem_holder;
};
using Pages       = std::vector<Page>;
using PageMap     = std::map<PageId, Page>;
using PageHandler = std::function<void(PageId page_id, const Page &)>;

// Indicate the page size && offset in PageFile. TODO: rename to `PageEntry`?
struct PageCache
{
    // if file_id == 0, means it is invalid
    PageFileId file_id  = 0;
    UInt32     level    = 0;
    UInt32     size     = 0;
    UInt64     offset   = 0;
    UInt64     tag      = 0;
    UInt64     checksum = 0;
    UInt32     ref      = 1; // for ref counting

    bool               isValid() const { return file_id != 0; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
};
static_assert(std::is_trivially_copyable_v<PageCache>);

//using PageCacheMap    = std::unordered_map<PageId, PageCache>;
class PageCacheMap
{
private:
    std::unordered_map<PageId, PageId>    page_ref;
    std::unordered_map<PageId, PageCache> normal_pages;

private:
    PageId resolveRefPageId(PageId page_id) const
    {
        // resolve RefPageId to normal PageId
        // if RefPage3 -> Page1, RefPage4 -> RefPage3
        // resolveRefPage(3) -> 1
        // resolveRefPage(4) -> 1
        for (auto ref_iter = page_ref.find(page_id); ref_iter != page_ref.end() && ref_iter->second != page_id; page_id = ref_iter->second)
            ;
        return page_id;
    }

public:
    class iterator
    {
    public:
        iterator(const std::unordered_map<PageId, PageId>::iterator & iter, std::unordered_map<PageId, PageCache> & normal_pages)
            : _iter(iter), _normal_pages(normal_pages)
        {
        }
        bool operator==(const iterator & rhs) const { return _iter == rhs._iter; }
        bool operator!=(const iterator & rhs) const { return _iter != rhs._iter; }
        // prefix incr
        iterator & operator++()
        {
            _iter++;
            return *this;
        }
        // suffix incr
        const iterator operator++(int)
        {
            iterator tmp(*this);
            _iter++;
            return tmp;
        }
        PageId            pageId() const { return _iter->first; }
        PageCache &       pageCache() { return _normal_pages[_iter->second]; }
        const PageCache & pageCache() const { return _normal_pages[_iter->second]; }

    private:
        std::unordered_map<PageId, PageId>::iterator _iter;
        std::unordered_map<PageId, PageCache> &      _normal_pages;
    };

    class const_iterator
    {
    public:
        const_iterator(const std::unordered_map<PageId, PageId>::const_iterator & iter,
                       const std::unordered_map<PageId, PageCache> &              normal_pages)
            : _iter(iter), _normal_pages(const_cast<std::unordered_map<PageId, PageCache> &>(normal_pages))
        {
        }
        bool operator==(const const_iterator & rhs) const { return _iter == rhs._iter; }
        bool operator!=(const const_iterator & rhs) const { return _iter != rhs._iter; }
        // prefix incr
        const_iterator & operator++()
        {
            _iter++;
            return *this;
        }
        // suffix incr
        const const_iterator operator++(int)
        {
            const_iterator tmp(*this);
            _iter++;
            return tmp;
        }
        PageId            pageId() const { return _iter->first; }
        const PageCache & pageCache() const { return _normal_pages[_iter->second]; }

    private:
        std::unordered_map<PageId, PageId>::const_iterator _iter;
        std::unordered_map<PageId, PageCache> &            _normal_pages;
    };

    iterator       end() { return iterator(page_ref.end(), normal_pages); }
    const_iterator end() const { return const_iterator(page_ref.end(), normal_pages); }
    iterator       begin() { return iterator(page_ref.begin(), normal_pages); }
    const_iterator begin() const { return const_iterator(page_ref.begin(), normal_pages); }

    const_iterator cend() const { return const_iterator(page_ref.cend(), normal_pages); }
    const_iterator cbegin() { return const_iterator(page_ref.cbegin(), normal_pages); }

    iterator       find(const PageId page_id) { return iterator(page_ref.find(page_id), normal_pages); }
    const_iterator find(const PageId page_id) const { return const_iterator(page_ref.find(page_id), normal_pages); }

    PageCache & at(const PageId page_id) { return normal_pages.at(resolveRefPageId(page_id)); }

    bool empty() const { return normal_pages.empty(); }

    void put(const PageId page_id, const PageCache & cache)
    {
        normal_pages[page_id] = cache;
        // add a RefPage to Page
        page_ref.emplace(page_id, page_id);
    }

    void del(const PageId page_id)
    {
        // decrease origin page's ref counting
        const PageId ori_page_id = resolveRefPageId(page_id);
        page_ref.erase(page_id);
        auto iter = normal_pages.find(ori_page_id);
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

    void ref(const PageId page_id, const PageId ori_page_id)
    {
        // if ori_page_id is a ref-id, collapse the ref-path to actual PageId
        const PageId normal_page_id = resolveRefPageId(ori_page_id);
        auto         iter           = normal_pages.find(normal_page_id);
        if (likely(iter != normal_pages.end()))
        {
            iter->second.ref += 1;
            // build ref
            page_ref.emplace(page_id, normal_page_id);
        }
        else
        {
            throw Exception("Add RefPage" + DB::toString(page_id) + " to non-exist Page" + DB::toString(ori_page_id),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    void clear()
    {
        page_ref.clear();
        normal_pages.clear();
    }
};

using PageCaches      = std::vector<PageCache>;
using PageIdAndCache  = std::pair<PageId, PageCache>;
using PageIdAndCaches = std::vector<PageIdAndCache>;

} // namespace DB
