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

#include "RemotePageReader.h"
#include <IO/ReadBufferFromFile.h>


namespace DB
{
Page RemotePageReader::read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry)
{
    auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + *location.data_file_id);
    buf->seek(location.offset_in_file);
    auto buf_size = location.size_in_file;
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });
    buf->readStrict(data_buf, buf_size);
    Page page;
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

Page RemotePageReader::read(const PS::V3::RemoteDataLocation & location, const PS::V3::PageEntryV3 & page_entry, std::vector<size_t> fields)
{
    size_t buf_size = 0;
    for (const auto field_index : fields)
    {
        buf_size += page_entry.getFieldSize(field_index);
    }
    RUNTIME_CHECK(buf_size <= location.size_in_file);
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });
    // TODO: read continuous field in one read
    auto buf = std::make_shared<ReadBufferFromFile>(remote_directory + *location.data_file_id);
    size_t data_buf_pos = 0;
    std::set<FieldOffsetInsidePage> fields_offset_in_page;
    for (const auto field_index : fields)
    {
        const auto [beg_offset, end_offset] = page_entry.getFieldOffsets(field_index);
        const auto size_to_read = end_offset - beg_offset;
        buf->seek(location.offset_in_file + beg_offset);
        buf->readStrict(data_buf + data_buf_pos, size_to_read);
        fields_offset_in_page.emplace(field_index, data_buf_pos);
        data_buf_pos += size_to_read;
    }
    RUNTIME_CHECK(data_buf_pos == buf_size);
    Page page;
    page.data = ByteBuffer(data_buf, data_buf + buf_size);
    page.mem_holder = mem_holder;
    page.field_offsets.swap(fields_offset_in_page);
    return page;
}

}
