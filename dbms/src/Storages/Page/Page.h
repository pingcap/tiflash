#pragma once

#include <map>
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

// Indicate the page size && offset in PageFile.
struct PageEntry
{
    // if file_id == 0, means it is invalid
    PageFileId file_id  = 0;
    UInt32     level    = 0;
    UInt32     size     = 0;
    UInt64     offset   = 0;
    UInt64     tag      = 0;
    UInt64     checksum = 0;
    UInt32     ref      = 1; // for ref counting

    inline bool               isValid() const { return file_id != 0; }
    inline bool               isTombstone() const { return ref == 0; }
    inline PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
};
static_assert(std::is_trivially_copyable_v<PageEntry>);

using PageIdAndEntry   = std::pair<PageId, PageEntry>;
using PageIdAndEntries = std::vector<PageIdAndEntry>;

} // namespace DB
