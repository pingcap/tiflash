// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Exception.h>
#include <Storages/Page/PageDefines.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CHECKSUM_DOESNT_MATCH;
} // namespace ErrorCodes
namespace PS::V3
{
struct PageEntryV3
{
public:
    BlobFileId file_id = 0; // The id of page data persisted in
    PageSize size = 0; // The size of page data
    PageSize padded_size = 0; // The extra align size of page data
    UInt64 tag = 0;
    BlobFileOffset offset = 0; // The offset of page data in file
    UInt64 checksum = 0; // The checksum of whole page data

    // The offset to the beginning of specify field.
    PageFieldOffsetChecksums field_offsets{};

public:
    PageSize getTotalSize() const
    {
        return size + padded_size;
    }

    inline bool isValid() const { return file_id != INVALID_BLOBFILE_ID; }

    size_t getFieldSize(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(fmt::format("Try to getFieldData of PageEntry [blob_id={}] with invalid [index={}] [fields size={}]",
                                        file_id,
                                        index,
                                        field_offsets.size()),
                            ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return size - field_offsets.back().first;
        else
            return field_offsets[index + 1].first - field_offsets[index].first;
    }

    // Return field{index} offsets: [begin, end) of page data.
    std::pair<size_t, size_t> getFieldOffsets(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(
                fmt::format("Try to getFieldOffsets with invalid index [index={}] [fields_size={}]", index, field_offsets.size()),
                ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
            return {field_offsets.back().first, size};
        else
            return {field_offsets[index].first, field_offsets[index + 1].first};
    }
};
using PageEntriesV3 = std::vector<PageEntryV3>;
using PageIDAndEntryV3 = std::pair<PageIdV3Internal, PageEntryV3>;
using PageIDAndEntriesV3 = std::vector<PageIDAndEntryV3>;


inline String toDebugString(const PageEntryV3 & entry)
{
    FmtBuffer fmt_buf;
    fmt_buf.joinStr(
        entry.field_offsets.begin(),
        entry.field_offsets.end(),
        [](const auto & offset_checksum, FmtBuffer & fb) {
            fb.fmtAppend("{}", offset_checksum.first);
        },
        ",");
    return fmt::format("PageEntryV3{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}, tag: {}, field_offsets: [{}]}}",
                       entry.file_id,
                       entry.offset,
                       entry.size,
                       entry.checksum,
                       entry.tag,
                       fmt_buf.toString());
}

} // namespace PS::V3
} // namespace DB
