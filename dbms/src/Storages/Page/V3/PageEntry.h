#pragma once

#include <Storages/Page/PageDefines.h>

namespace DB::PS::V3
{
struct PageEntryV2
{
public:
    BlobFileID file_id = 0; // The id of page data persisted in
    PageSize size = 0; // The size of page data
    UInt64 offset = 0; // The offset of page data in file
    UInt64 checksum = 0; // The checksum of whole page data
};
using PageIDAndEntryV2 = std::pair<PageId, PageEntryV2>;
using PageIDAndEntriesV2 = std::vector<PageIDAndEntryV2>;

} // namespace DB::PS::V3
