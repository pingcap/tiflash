#pragma once

#include <unordered_map>

#include <IO/BufferBase.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <Storages/Page/PageDefines.h>

namespace DB
{

using MemHolder = std::shared_ptr<char>;
inline MemHolder createMemHolder(char * memory, std::function<void(char *)> free)
{
    return std::shared_ptr<char>(memory, free);
}

struct Page
{
    PageId     page_id;
    ByteBuffer data;

    MemHolder mem_holder;
};
using Pages   = std::vector<Page>;
using PageMap = std::unordered_map<PageId, Page>;

// TODO REVIEW: this class has `cache` in its name but it don't hold any memory, whith is weird
struct PageCache
{
    PageFileId file_id;
    UInt32     level;
    UInt32     size;
    UInt64     version;
    UInt64     offset;
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
