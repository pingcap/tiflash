#pragma once

#include <unordered_map>

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <Storages/Page/PageDefines.h>

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

struct PageCache
{
    PageFileId file_id = 0;
    UInt32     level;
    UInt32     size;
    UInt64     offset;
    UInt64     tag;
    UInt64     checksum;

    bool               isValid() { return file_id; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
};
static_assert(std::is_trivially_copyable_v<PageCache>);

using PageCacheMap    = std::unordered_map<PageId, PageCache>;
using PageCaches      = std::vector<PageCache>;
using PageIdAndCache  = std::pair<PageId, PageCache>;
using PageIdAndCaches = std::vector<PageIdAndCache>;

} // namespace DB
