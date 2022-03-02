#pragma once

#include <Storages/Page/PageDefines.h>
#include <fmt/format.h>

namespace DB::PS::V3
{
struct PageEntryV3
{
public:
    BlobFileId file_id = 0; // The id of page data persisted in
    PageSize size = 0; // The size of page data
    UInt64 tag = 0;
    BlobFileOffset offset = 0; // The offset of page data in file
    UInt64 checksum = 0; // The checksum of whole page data

    // The offset to the begining of specify field.
    PageFieldOffsetChecksums field_offsets{};
};
using PageEntriesV3 = std::vector<PageEntryV3>;
using PageIDAndEntryV3 = std::pair<PageId, PageEntryV3>;
using PageIDAndEntriesV3 = std::vector<PageIDAndEntryV3>;

inline String toDebugString(const PageEntryV3 & entry)
{
    return fmt::format("PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}}}", entry.file_id, entry.offset, entry.size, entry.checksum);
}

} // namespace DB::PS::V3
