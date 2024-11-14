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

#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>
#include <Common/Exception.h>
#include <Core/Defines.h>
#include <Storages/Page/V3/Universal/S3PageReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>

namespace ProfileEvents
{
extern const Event S3PageReaderReusedFile;
extern const Event S3PageReaderNotReusedFile;
extern const Event S3PageReaderNotReusedFileReadback;
extern const Event S3PageReaderNotReusedFileChangeFile;
} // namespace ProfileEvents

namespace DB::PS::V3
{
Page S3PageReader::read(const UniversalPageIdAndEntry & page_id_and_entry)
{
    return std::get<0>(readFromS3File(page_id_and_entry, nullptr, DBMS_DEFAULT_BUFFER_SIZE));
}

std::tuple<Page, ReadBufferFromRandomAccessFilePtr> S3PageReader::readFromS3File(
    const UniversalPageIdAndEntry & page_id_and_entry,
    ReadBufferFromRandomAccessFilePtr file_buf,
    size_t prefetch_size)
{
    assert(prefetch_size != 0); // 0-size read buffer can not read any data, should not happen

    const auto & page_entry = page_id_and_entry.second;
    RUNTIME_CHECK(page_entry.checkpoint_info.has_value());
    const auto & location = page_entry.checkpoint_info.data_location;
    const auto & data_file_id = *location.data_file_id;
    const auto remote_fname_view = S3::S3FilenameView::fromKey(data_file_id);
    const auto remote_fname
        = remote_fname_view.isLockFile() ? remote_fname_view.asDataFile().toFullKey() : data_file_id;

    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    ReadBufferFromRandomAccessFilePtr read_buff;
    if (file_buf == nullptr || file_buf->getPositionInFile() < 0
        || location.offset_in_file < static_cast<size_t>(file_buf->getPositionInFile())
        // note that S3RandomAccessFile will prepand the bucket name as prefix, we should
        // use "getInitialFileName" instead of "getFileName"
        || remote_fname != file_buf->getInitialFileName())
    {
        if (file_buf != nullptr)
        {
            if (location.offset_in_file < static_cast<size_t>(file_buf->getPositionInFile()))
                ProfileEvents::increment(ProfileEvents::S3PageReaderNotReusedFileReadback, 1);
            else if (remote_fname != file_buf->getInitialFileName())
                ProfileEvents::increment(ProfileEvents::S3PageReaderNotReusedFileChangeFile, 1);
        }
        S3::S3RandomAccessFilePtr s3_remote_file;
        ProfileEvents::increment(ProfileEvents::S3PageReaderNotReusedFile, 1);
        if (remote_fname_view.isLockFile())
        {
            s3_remote_file = std::make_shared<S3::S3RandomAccessFile>(s3_client, remote_fname);
        }
        else
        {
#ifndef DBMS_PUBLIC_GTEST
            RUNTIME_CHECK_MSG(
                false,
                "Can not read from an invalid remote location, location={}",
                location.toDebugString());
#else
            // In unit test, we directly read from `location.data_file_id` which want to just focus on read write logic
            s3_remote_file = std::make_shared<S3::S3RandomAccessFile>(s3_client, remote_fname);
#endif
        }
        read_buff = std::make_shared<ReadBufferFromRandomAccessFile>(s3_remote_file, prefetch_size);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::S3PageReaderReusedFile, 1);
        // Reuse the previous read buffer on the same file
        read_buff = file_buf;
    }

    const auto buf_size = location.size_in_file;
    RUNTIME_CHECK(buf_size != 0, page_id_and_entry);
    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });
    try
    {
        RUNTIME_CHECK(read_buff != nullptr);
        read_buff->seek(location.offset_in_file, SEEK_SET);
        // TODO: support checksum verification
        read_buff->readStrict(data_buf, buf_size);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format(
            "while reading from S3, page_id={} location={}",
            page_id_and_entry.first,
            location.toDebugString()));
        e.rethrow();
    }
    Page page{UniversalPageIdFormat::getU64ID(page_id_and_entry.first)};
    page.data = std::string_view(data_buf, buf_size);
    page.mem_holder = mem_holder;
    // Calculate the field_offsets from page entry
    for (size_t index = 0; index < page_entry.field_offsets.size(); index++)
    {
        const auto offset = page_entry.field_offsets[index].first;
        page.field_offsets.emplace(index, offset);
    }
    return std::make_tuple(page, read_buff);
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

std::pair<UniversalPageMap, UniversalPageMap> S3PageReader::read(FieldReadInfos & to_read)
{
    UniversalPageMap complete_page_map;
    size_t read_fields_size = 0;
    for (auto & read_info : to_read)
    {
        std::sort(read_info.fields.begin(), read_info.fields.end());
        const auto & page_entry = read_info.entry;
        // read the whole page from S3 and save it as `complete_page`
        complete_page_map.emplace(read_info.page_id, read(std::make_pair(read_info.page_id, page_entry)));
        for (const auto field_index : read_info.fields)
        {
            read_fields_size += page_entry.getFieldSize(field_index);
        }
    }
    char * read_fields_buf = static_cast<char *>(alloc(read_fields_size));
    MemHolder read_fields_mem_holder
        = createMemHolder(read_fields_buf, [&, read_fields_size](char * p) { free(p, read_fields_size); });
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
        page.data = std::string_view(read_fields_buf + page_begin, data_pos - page_begin);
        page.mem_holder = read_fields_mem_holder;
        page.field_offsets.swap(fields_offset_in_page);
        read_fields_page_map.emplace(read_info.page_id, page);
    }
    RUNTIME_CHECK(data_pos == read_fields_size);
    return std::make_pair(complete_page_map, read_fields_page_map);
}
} // namespace DB::PS::V3
