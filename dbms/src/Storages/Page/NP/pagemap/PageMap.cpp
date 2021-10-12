#include "PageMap.h"

#include <Common/Exception.h>
#include <Storages/Page/PageUtil.h>
#include <stdlib.h>
#include <sys/statvfs.h>

#include <memory>

#include "../NPageFileParser.h"

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
}

class NPageMapParser : public NPageFileParser
{
public:
    NPageMapParser(bitmaps * bitmap_, PageEntriesEdit * edit_, char * buffer_, UInt64 size)
        : NPageFileParser(edit_, buffer_, size)
        , bitmap(bitmap_){};

    void applyPU(PFMeta * meta, PageEntry & entry) override
    {
        NPageFileParser::applyPU(meta, entry);

        // FIXME : maybe should consider the same page_id in put
        bitmap->bitmap_ops->mark_bmap_extent(bitmap, entry.offset, entry.size);
    }

    void applyDel(PFMeta * meta) override
    {
        NPageFileParser::applyDel(meta);
        bitmap->bitmap_ops->unmark_bmap_extent(bitmap, meta->del.page_offset, meta->del.page_size);
    }

private:
    bitmaps * bitmap;
};

NPageMap::NPageMap(bitmaps * bitmap_, String & file_path_, FileProviderPtr file_provider_)
    : file_provider{file_provider_}
    , file_path(file_path_)
    , bitmap(bitmap_)
{
}

NPageMap::~NPageMap()
{
    free_bitmaps(bitmap);
}

// FIXME : remove this
#define PAGE_FILE_META "page_file_meta"

// FIXME : some problem to restore
PageEntriesEdit NPageMap::restore()
{
    auto meta_reader = file_provider->newRandomAccessFile(
        file_path + PAGE_FILE_META,
        EncryptionPath(file_path + PAGE_FILE_META, ""));

    UInt64 file_size = 0;
    Poco::File page_file_data(file_path + PAGE_FILE_META);
    if ((file_size = page_file_data.getSize()) <= 1)
    {
        return {};
    }

    /*
    char page_file_version;
    PageUtil::readFile(meta_reader,0, &page_file_version, 1, nullptr);
    (void)page_file_version;
    if (page_file_version != '3')
    {
        throw DB::Exception("It is not PageStorage v3 file.", DB::ErrorCodes::LOGICAL_ERROR);
    }
    */

    char * meta_buffer = (char *)alloc(file_size);
    SCOPE_EXIT({
        free(meta_buffer, file_size);
    });
    PageUtil::readFile(meta_reader, 0, meta_buffer, file_size, nullptr);

    PageEntriesEdit edit;

    // parse and restore
    NPageMapParser page_map_parser(bitmap, &edit, meta_buffer, file_size);
    page_map_parser.parse();

    return edit;
}

UInt64 NPageMap::getDataRange(UInt64 size, bool also_mark)
{
    if (size == 0)
    {
        return UINT64_MAX;
    }

    std::lock_guard<std::recursive_mutex> lock(query_metux);
    UInt64 fist_free_block = 0;

    /// Actually , we can cache in here. make it search faster.
    if (get_free_blocks(bitmap, bitmap->start, bitmap->end, size, &fist_free_block) != 0)
    {
        return UINT64_MAX;
    }

    if (also_mark)
        markDataRange(fist_free_block, size);

    return fist_free_block;
}

void NPageMap::getDataRange(UInt64 * sizes, size_t nums, UInt64 * offsets, bool also_mark)
{
    std::lock_guard<std::recursive_mutex> lock(query_metux);
    UInt64 start_position = bitmap->start;
    for (size_t i = 0; i < nums; i++)
    {
        if (sizes[i] == 0)
        {
            offsets[i] = 0;
            continue;
        }
        if (get_free_blocks(bitmap, start_position, bitmap->end, sizes[i], &(offsets[i])) != 0)
        {
            // todo handle it
        }
        start_position = offsets[i] + sizes[i];
    }

    if (also_mark)
        markDataRange(offsets, sizes, nums);
}

void NPageMap::splitDataInRange(UInt64 * sizes, size_t nums, UInt64 * offsets, UInt64 start_range, UInt64 range_len)
{
    UInt64 cur_range = start_range;
    for (size_t i = 0; i < nums; i++)
    {
        if (sizes[i] == 0)
        {
            offsets[i] = 0;
            continue;
        }
        offsets[i] = cur_range;
        cur_range += sizes[i];
    }

    if (cur_range - start_range != range_len)
    {
        // todo
    }
}

void NPageMap::markDataRange(UInt64 offsets, UInt64 size)
{
    std::lock_guard<std::recursive_mutex> lock(query_metux);
    if (mark_block_bmap_range(bitmap, offsets, size) != 0)
    {
        // todo handle it
    }
}


void NPageMap::unmarkDataRange(UInt64 offsets, UInt64 size)
{
    std::lock_guard<std::recursive_mutex> lock(query_metux);
    if (unmark_block_bmap_range(bitmap, offsets, size) != 0)
    {
        // todo handle it
    }
}

void NPageMap::markDataRange(UInt64 * offsets, UInt64 * sizes, size_t nums)
{
    std::lock_guard<std::recursive_mutex> lock(query_metux);

    for (size_t i = 0; i < nums; i++)
    {
        if (mark_block_bmap_range(bitmap, offsets[i], sizes[i]) != 0)
        {
            // todo handle it
        }
    }
}


NPageMapPtr NPageMap::newPageMap(String & path, int bitmap_type, FileProviderPtr file_provider)
{
    auto * bitmap = (struct bitmaps *)std::calloc(1, sizeof(struct bitmaps));
    if (bitmap == nullptr)
    {
        throw DB::Exception("Alloc bitmap failed.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    struct statvfs vfs_info;

    if (int code = statvfs(path.data(), &vfs_info); code != 0)
    {
        std::free(bitmap);
        throw DB::Exception("Can get available size from VFS.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    if (init_bitmaps(bitmap, bitmap_type, 0, vfs_info.f_bfree * vfs_info.f_frsize, vfs_info.f_bfree * vfs_info.f_frsize) != 0)
    {
        std::free(bitmap);
        throw DB::Exception("Init Bitmap failed.", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }

    NPageMapPtr page_map = std::make_shared<NPageMap>(bitmap, path, file_provider);
    return page_map;
}

} // namespace DB