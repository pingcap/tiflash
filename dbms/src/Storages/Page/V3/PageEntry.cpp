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

#include <Storages/Page/V3/PageEntry.h>

namespace DB
{
namespace PS::V3
{

 size_t PageEntryV3::getFieldSize(size_t index) const
{
    const auto & field_offsets = getFieldOffsets();
    RUNTIME_CHECK_MSG(index < field_offsets.size(), //
                      "Try to getFieldData of PageEntry [blob_id={}] with invalid [index={}] [fields size={}]",
                      getFileId(),
                      index,
                      field_offsets.size());

    if (index == field_offsets.size() - 1)
        return getSize() - field_offsets.back().first;
    else
        return field_offsets[index + 1].first - field_offsets[index].first;
}

// Return field{index} offsets: [begin, end) of page data.
std::pair<size_t, size_t> PageEntryV3::getFieldOffsets(size_t index) const
{
    const auto & field_offsets = getFieldOffsets();
    RUNTIME_CHECK_MSG(index < field_offsets.size(), //
                      "Try to getFieldOffsets with invalid index [index={}] [fields_size={}]",
                      index,
                      field_offsets.size());

    if (index == field_offsets.size() - 1)
        return {field_offsets.back().first, getSize()};
    else
        return {field_offsets[index].first, field_offsets[index + 1].first};
}


PageEntryV3Ptr makePageEntry(BlobFileId file_id, //
                                    PageSize size,
                                    PageSize padded_size,
                                    UInt64 tag,
                                    BlobFileOffset offset,
                                    UInt64 checksum,
                                    PageFieldOffsetChecksums && field_offsets)
{
    if (file_id <= static_cast<BlobFileId>(std::numeric_limits<BlobFileIdTight>::max())
        && offset <= static_cast<BlobFileOffset>(std::numeric_limits<BlobFileOffsetTight>::max())
        && size <= static_cast<PageSize>(std::numeric_limits<PageSizeTight>::max())
        && padded_size == 0
        && tag == 0
        && field_offsets.empty())
    {
        return std::make_shared<PageEntryV3Tight>(static_cast<BlobFileIdTight>(file_id), //
                                                  static_cast<PageSizeTight>(size),
                                                  static_cast<BlobFileOffsetTight>(offset),
                                                  checksum);
    }
    else
    {
        return std::make_shared<PageEntryV3Loose>(file_id, //
                                                  size,
                                                  padded_size,
                                                  tag,
                                                  offset,
                                                  checksum,
                                                  std::move(field_offsets));
    }
}
}
}