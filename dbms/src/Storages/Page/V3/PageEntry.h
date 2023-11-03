// Copyright 2023 PingCAP, Inc.
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
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
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

    /**
     * Whether this page entry's data is stored in a checkpoint and where it is stored.
     * If this page entry is not stored in a checkpoint file, this field.is_valid == false.
     */
    OptionalCheckpointInfo checkpoint_info;

    // The offset to the beginning of specify field.
    PageFieldOffsetChecksums field_offsets{};

public:
    PageSize getTotalSize() const { return size + padded_size; }

    inline bool isValid() const { return file_id != INVALID_BLOBFILE_ID || checkpoint_info.has_value(); }

    size_t getFieldSize(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(
                fmt::format(
                    "Try to getFieldData of PageEntry [blob_id={}] with invalid [index={}] [fields size={}]",
                    file_id,
                    index,
                    field_offsets.size()),
                ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
        {
            if (checkpoint_info.has_value() && checkpoint_info.is_local_data_reclaimed)
            {
                // entry.size is not reliable under this case, use the size_in_file in checkpoint_info instead
                return checkpoint_info.data_location.size_in_file - field_offsets.back().first;
            }
            else
            {
                return size - field_offsets.back().first;
            }
        }
        else
            return field_offsets[index + 1].first - field_offsets[index].first;
    }

    // Return field{index} offsets: [begin, end) of page data.
    std::pair<size_t, size_t> getFieldOffsets(size_t index) const
    {
        if (unlikely(index >= field_offsets.size()))
            throw Exception(
                fmt::format(
                    "Try to getFieldOffsets with invalid index [index={}] [fields_size={}]",
                    index,
                    field_offsets.size()),
                ErrorCodes::LOGICAL_ERROR);
        else if (index == field_offsets.size() - 1)
        {
            if (checkpoint_info.has_value() && checkpoint_info.is_local_data_reclaimed)
            {
                // entry.size is not reliable under this case, use the size_in_file in checkpoint_info instead
                return {field_offsets.back().first, checkpoint_info.data_location.size_in_file};
            }
            else
            {
                return {field_offsets.back().first, size};
            }
        }
        else
            return {field_offsets[index].first, field_offsets[index + 1].first};
    }
};
using PageEntriesV3 = std::vector<PageEntryV3>;
using PageIDAndEntryV3 = std::pair<PageIdV3Internal, PageEntryV3>;
using PageIDAndEntriesV3 = std::vector<PageIDAndEntryV3>;

} // namespace PS::V3
} // namespace DB

template <>
struct fmt::formatter<DB::PS::V3::PageEntryV3>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::PageEntryV3 & entry, FormatContext & ctx) const
    {
        using namespace DB;

        FmtBuffer fmt_buf;
        fmt_buf.joinStr(
            entry.field_offsets.begin(),
            entry.field_offsets.end(),
            [](const auto & offset_checksum, FmtBuffer & fb) { fb.fmtAppend("{}", offset_checksum.first); },
            ",");

        return fmt::format_to(
            ctx.out(),
            "PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}, tag: {}, field_offsets: [{}], "
            "checkpoint_info: {}}}",
            entry.file_id,
            entry.offset,
            entry.size,
            entry.checksum,
            entry.tag,
            fmt_buf.toString(),
            entry.checkpoint_info.toDebugString());
    }
};
