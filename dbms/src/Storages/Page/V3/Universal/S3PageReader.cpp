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

#include <IO/ReadBufferFromFile.h>
#include <Storages/Page/V3/Universal/S3PageReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/S3/S3RandomAccessFile.h>

namespace DB::PS::V3
{
Page S3PageReader::read(const UniversalPageIdAndEntry & page_id_and_entry)
{
    const auto & page_entry = page_id_and_entry.second;
    RUNTIME_CHECK(page_entry.checkpoint_info.has_value());
    auto location = page_entry.checkpoint_info->data_location;
    S3::S3RandomAccessFile file(s3_client, bucket, *location.data_file_id);
    file.seek(location.offset_in_file, SEEK_SET);
    auto buf_size = location.size_in_file;
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });
    // TODO: support checksum verification
    size_t pos = 0;
    while (pos < buf_size)
    {
        auto n = file.read(data_buf + pos, buf_size - pos);
        pos += n;
    }
    Page page{UniversalPageIdFormat::getU64ID(page_id_and_entry.first)};
    page.data = ByteBuffer(data_buf, data_buf + buf_size);
    page.mem_holder = mem_holder;
    // Calculate the field_offsets from page entry
    for (size_t index = 0; index < page_entry.field_offsets.size(); index++)
    {
        const auto offset = page_entry.field_offsets[index].first;
        page.field_offsets.emplace(index, offset);
    }
    return page;
}

UniversalPageMap S3PageReader::read(const UniversalPageIdAndEntries & page_id_and_entries)
{
    UniversalPageMap page_map;
    for (const auto & page_id_and_entry : page_id_and_entries)
    {
        page_map.emplace(page_id_and_entry.first, read(page_id_and_entry));
    }
    return page_map;
}

std::pair<UniversalPageMap, UniversalPageMap> S3PageReader::read(const FieldReadInfos & to_read)
{
    UniversalPageMap complete_page_map;
    size_t read_fields_size = 0;
    for (const auto & read_info : to_read)
    {
        const auto & page_entry = read_info.entry;
        complete_page_map.emplace(read_info.page_id, read(std::make_pair(read_info.page_id, page_entry)));
        for (const auto field_index : read_info.fields)
        {
            read_fields_size += page_entry.getFieldSize(field_index);
        }
    }
    char * read_fields_buf = static_cast<char *>(alloc(read_fields_size));
    MemHolder read_fields_mem_holder = createMemHolder(read_fields_buf, [&, read_fields_size](char * p) {
        free(p, read_fields_size);
    });
    size_t data_pos = 0;
    UniversalPageMap read_fields_page_map;
    for (const auto & read_info : to_read)
    {
        const auto & complete_page = complete_page_map.at(read_info.page_id);
        const auto & page_entry = read_info.entry;
        std::set<FieldOffsetInsidePage> fields_offset_in_page;
        size_t page_begin = data_pos;
        for (const auto field_index : read_info.fields)
        {
            const auto [beg_offset, end_offset] = page_entry.getFieldOffsets(field_index);
            const auto size_to_read = end_offset - beg_offset;
            // TODO: copy continuous fields in one operation
            memcpy(read_fields_buf + data_pos, complete_page.data.begin() + beg_offset, size_to_read);
            fields_offset_in_page.emplace(field_index, data_pos);
            data_pos += size_to_read;
        }
        Page page{UniversalPageIdFormat::getU64ID(read_info.page_id)};
        page.data = ByteBuffer(read_fields_buf + page_begin, read_fields_buf + data_pos);
        page.mem_holder = read_fields_mem_holder;
        page.field_offsets.swap(fields_offset_in_page);
        read_fields_page_map.emplace(read_info.page_id, page);
    }
    RUNTIME_CHECK(data_pos == read_fields_size);
    return std::make_pair(complete_page_map, read_fields_page_map);
}
} // namespace DB::PS::V3
