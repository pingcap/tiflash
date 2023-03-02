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
#include <Storages/Page/V3/CheckpointFile/CPDataFileReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>

namespace DB::PS::V3
{
Page CPDataFileReader::read(const UniversalPageIdAndEntry & page_id_and_entry)
{
    const auto & page_entry = page_id_and_entry.second;
    RUNTIME_CHECK(page_entry.checkpoint_info.has_value());
    auto location = page_entry.checkpoint_info->data_location;
    auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + "/" + *location.data_file_id);
    buf->seek(location.offset_in_file);
    auto buf_size = location.size_in_file;
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });
    // TODO: support checksum verification
    buf->readStrict(data_buf, buf_size);
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

UniversalPageMap CPDataFileReader::read(const UniversalPageIdAndEntries & page_id_and_entries)
{
    UniversalPageMap page_map;
    for (const auto & page_id_and_entry : page_id_and_entries)
    {
        page_map.emplace(page_id_and_entry.first, read(page_id_and_entry));
    }
    return page_map;
}

UniversalPageMap CPDataFileReader::read(const FieldReadInfos & to_read)
{
    UniversalPageMap page_map;
    for (const auto & read_info : to_read)
    {
        const auto & page_entry = read_info.entry;
        RUNTIME_CHECK(page_entry.checkpoint_info.has_value());
        auto location = page_entry.checkpoint_info->data_location;
        size_t buf_size = 0;
        for (const auto field_index : read_info.fields)
        {
            buf_size += page_entry.getFieldSize(field_index);
        }
        RUNTIME_CHECK(buf_size <= location.size_in_file);
        char * data_buf = static_cast<char *>(alloc(buf_size));
        MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
            free(p, buf_size);
        });
        auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + "/" + *location.data_file_id);
        size_t data_buf_pos = 0;
        std::set<FieldOffsetInsidePage> fields_offset_in_page;
        // TODO: read continuous field in one read
        for (const auto field_index : read_info.fields)
        {
            const auto [beg_offset, end_offset] = page_entry.getFieldOffsets(field_index);
            const auto size_to_read = end_offset - beg_offset;
            buf->seek(location.offset_in_file + beg_offset);
            buf->readStrict(data_buf + data_buf_pos, size_to_read);
            fields_offset_in_page.emplace(field_index, data_buf_pos);
            data_buf_pos += size_to_read;
        }
        RUNTIME_CHECK(data_buf_pos == buf_size);
        Page page{UniversalPageIdFormat::getU64ID(read_info.page_id)};
        page.data = ByteBuffer(data_buf, data_buf + buf_size);
        page.mem_holder = mem_holder;
        page.field_offsets.swap(fields_offset_in_page);
        page_map.emplace(read_info.page_id, page);
    }
    return page_map;
}
} // namespace DB::PS::V3
