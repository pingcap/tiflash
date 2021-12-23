#pragma once

#include <Storages/Page/PageDefines.h>

namespace DB::PS::V3
{
struct PageEntryV3
{
public:
    BlobFileId file_id = 0; // The id of page data persisted in
    PageSize size = 0; // The size of page data
    BlobFileOffset offset = 0; // The offset of page data in file
    UInt64 checksum = 0; // The checksum of whole page data

    // The offset to the begining of specify field.
    PageFieldOffsetChecksums field_offsets{};
};
using PageIDAndEntryV3 = std::pair<PageId, PageEntryV3>;
using PageIDAndEntriesV3 = std::vector<PageIDAndEntryV3>;

} // namespace DB::PS::V3
