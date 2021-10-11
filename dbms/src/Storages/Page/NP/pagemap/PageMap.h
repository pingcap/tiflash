#pragma once

#include "BitMap.h"
#include <Core/Types.h>
#include <Common/Allocator.h>
#include <Poco/Logger.h>
#include <Encryption/FileProvider.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSetWithDelta.h>

namespace DB 
{

class NPageMap;
using NPageMapPtr = std::shared_ptr<NPageMap>;

class NPageMap : public Allocator<false> {

public:
    NPageMap(bitmaps * bitmap, String & file_path, FileProviderPtr file_provider_);

    ~NPageMap();

    PageEntriesEdit restore();

    static NPageMapPtr newPageMap(String & path, int bitmap_type, FileProviderPtr file_provider);

    UInt64 getDataRange(UInt64 size, bool also_mark = false);

    void getDataRange(UInt64 * sizes,size_t nums, UInt64 * offsets, bool also_mark = false);

    void splitDataInRange(UInt64 * sizes, size_t nums, UInt64 *offsets, UInt64 start_range, UInt64 range_len);

    void markDataRange(UInt64 offsets, UInt64 size);

    void markDataRange(UInt64 * offsets,UInt64 *  sizes,size_t nums);

    void unmarkDataRange(UInt64 offsets, UInt64 size);

    String toString() const;

private:
    Poco::Logger * page_storage_log;

    // TODO : not support encryption yet.
    FileProviderPtr file_provider;
    String file_path;
    bitmaps * bitmap = nullptr;

    UInt64 page_nums = 0;
    std::recursive_mutex query_metux;
};

}