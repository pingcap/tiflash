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

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V1/PageFile.h>

#include <ext/scope_guard.h>

namespace DB::PS::V1
{
// =========================================================
// Page Meta format
// =========================================================

static constexpr bool PAGE_CHECKSUM_ON_READ = true;

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace PageMetaFormat
{
using WBSize = UInt32;
using PageFileVersion = PageFile::Version;
// TODO we should align these alias with type in PageCache
using PageTag = UInt64;
using IsPut = std::underlying_type<WriteBatch::WriteType>::type;
using PageOffset = UInt64;
using Checksum = UInt64;

static const size_t PAGE_META_SIZE = sizeof(PageId) + sizeof(PageTag) + sizeof(PageOffset) + sizeof(PageSize) + sizeof(Checksum);

/// Return <data to write into meta file, data to write into data file>.
std::pair<ByteBuffer, ByteBuffer> genWriteData( //
    const WriteBatch & wb,
    PageFile & page_file,
    PageEntriesEdit & edit)
{
    WBSize meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    meta_write_bytes += sizeof(WBSize) + sizeof(PageFileVersion);

    for (const auto & write : wb.getWrites())
    {
        meta_write_bytes += sizeof(IsPut);
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
            data_write_bytes += write.size;
            meta_write_bytes += PAGE_META_SIZE;
            break;
        case WriteBatch::WriteType::DEL:
            // For delete page, store page id only. And don't need to write data file.
            meta_write_bytes += sizeof(PageId);
            break;
        case WriteBatch::WriteType::REF:
            // For ref page, store RefPageId -> PageId. And don't need to write data file.
            meta_write_bytes += (sizeof(PageId) + sizeof(PageId));
            break;
        }
    }

    meta_write_bytes += sizeof(Checksum);

    char * meta_buffer = static_cast<char *>(page_file.alloc(meta_write_bytes));
    char * data_buffer = static_cast<char *>(page_file.alloc(data_write_bytes));

    char * meta_pos = meta_buffer;
    char * data_pos = data_buffer;

    PageUtil::put(meta_pos, meta_write_bytes);
    PageUtil::put(meta_pos, PageFile::CURRENT_VERSION);

    PageOffset page_data_file_off = page_file.getDataFileAppendPos();
    for (const auto & write : wb.getWrites())
    {
        PageUtil::put(meta_pos, static_cast<IsPut>(write.type));
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
        {
            if (write.read_buffer) // In case read_buffer is nullptr
                write.read_buffer->readStrict(data_pos, write.size);
            Checksum page_checksum = CityHash_v1_0_2::CityHash64(data_pos, write.size);
            data_pos += write.size;

            PageEntry pc{};
            pc.file_id = page_file.getFileId();
            pc.level = page_file.getLevel();
            pc.tag = write.tag;
            pc.size = write.size;
            pc.offset = page_data_file_off;
            pc.checksum = page_checksum;

            if (write.type == WriteBatch::WriteType::PUT)
                edit.put(write.page_id, pc);
            else if (write.type == WriteBatch::WriteType::UPSERT)
                edit.upsertPage(write.page_id, pc);

            PageUtil::put(meta_pos, write.page_id);
            PageUtil::put(meta_pos, static_cast<PageTag>(write.tag));
            PageUtil::put(meta_pos, page_data_file_off);
            PageUtil::put(meta_pos, write.size);
            PageUtil::put(meta_pos, page_checksum);

            page_data_file_off += write.size;
            break;
        }
        case WriteBatch::WriteType::DEL:
            PageUtil::put(meta_pos, write.page_id);

            edit.del(write.page_id);
            break;
        case WriteBatch::WriteType::REF:
            PageUtil::put(meta_pos, static_cast<PageId>(write.page_id));
            PageUtil::put(meta_pos, static_cast<PageId>(write.ori_page_id));

            edit.ref(write.page_id, write.ori_page_id);
            break;
        }
    }

    const Checksum wb_checksum = CityHash_v1_0_2::CityHash64(meta_buffer, meta_write_bytes - sizeof(Checksum));
    PageUtil::put(meta_pos, wb_checksum);

    if (unlikely(meta_pos != meta_buffer + meta_write_bytes || data_pos != data_buffer + data_write_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return {{meta_buffer, meta_pos}, {data_buffer, data_pos}};
}

/// Analyze meta file, and return <available meta size, available data size>.
std::pair<UInt64, UInt64> analyzeMetaFile( //
    const String & path,
    PageFileId file_id,
    UInt32 level,
    const char * meta_data,
    const size_t meta_data_size,
    PageEntriesEdit & edit,
    Poco::Logger * log)
{
    const char * meta_data_end = meta_data + meta_data_size;

    UInt64 page_data_file_size = 0;
    char * pos = const_cast<char *>(meta_data);
    while (pos < meta_data_end)
    {
        if (pos + sizeof(WBSize) > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }
        const char * wb_start_pos = pos;
        const auto wb_bytes = PageUtil::get<WBSize>(pos);
        if (wb_start_pos + wb_bytes > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }

        // this field is always true now
        const auto version = PageUtil::get<PageFileVersion>(pos);
        if (version != PageFile::CURRENT_VERSION)
            throw Exception("Version not match, version: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);

        // check the checksum of WriteBatch
        const auto wb_bytes_without_checksum = wb_bytes - sizeof(Checksum);
        const auto wb_checksum = PageUtil::get<Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
        const auto checksum_calc = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
        if (wb_checksum != checksum_calc)
        {
            std::stringstream ss;
            ss << "expected: " << std::hex << wb_checksum << ", but: " << checksum_calc;
            throw Exception("Write batch checksum not match, path: " + path + ", offset: " + DB::toString(wb_start_pos - meta_data)
                                + ", bytes: " + DB::toString(wb_bytes) + ", " + ss.str(),
                            ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }

        // recover WriteBatch
        while (pos < wb_start_pos + wb_bytes_without_checksum)
        {
            const auto is_put = PageUtil::get<IsPut>(pos);
            const auto write_type = static_cast<WriteBatch::WriteType>(is_put);
            switch (write_type)
            {
            case WriteBatch::WriteType::PUT:
            case WriteBatch::WriteType::UPSERT:
            {
                auto page_id = PageUtil::get<PageId>(pos);
                PageEntry pc;
                pc.file_id = file_id;
                pc.level = level;
                pc.tag = PageUtil::get<PageTag>(pos);
                pc.offset = PageUtil::get<PageOffset>(pos);
                pc.size = PageUtil::get<PageSize>(pos);
                pc.checksum = PageUtil::get<Checksum>(pos);

                if (write_type == WriteBatch::WriteType::PUT)
                    edit.put(page_id, pc);
                else if (write_type == WriteBatch::WriteType::UPSERT)
                    edit.upsertPage(page_id, pc);

                page_data_file_size += pc.size;
                break;
            }
            case WriteBatch::WriteType::DEL:
            {
                auto page_id = PageUtil::get<PageId>(pos);
                edit.del(page_id); // Reserve the order of removal.
                break;
            }
            case WriteBatch::WriteType::REF:
            {
                const auto ref_id = PageUtil::get<PageId>(pos);
                const auto page_id = PageUtil::get<PageId>(pos);
                edit.ref(ref_id, page_id);
                break;
            }
            }
        }
        // move `pos` over the checksum of WriteBatch
        pos += sizeof(Checksum);

        if (pos != wb_start_pos + wb_bytes)
            throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);
    }
    return {pos - meta_data, page_data_file_size};
}
} // namespace PageMetaFormat

// =========================================================
// PageFile::Writer
// =========================================================

PageFile::Writer::Writer(PageFile & page_file_, bool sync_on_write_, bool create_new_file)
    : page_file(page_file_)
    , sync_on_write(sync_on_write_)
    , data_file(page_file.file_provider->newWritableFile(page_file.dataPath(), page_file.dataEncryptionPath(), create_new_file, create_new_file))
    , meta_file(page_file.file_provider->newWritableFile(page_file.metaPath(), page_file.metaEncryptionPath(), create_new_file, create_new_file))
{}

PageFile::Writer::~Writer()
{
    SCOPE_EXIT({
        data_file->close();
        meta_file->close();
    });
    PageUtil::syncFile(data_file);
    PageUtil::syncFile(meta_file);
}

void PageFile::Writer::write(const WriteBatch & wb, PageEntriesEdit & edit)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    // TODO: investigate if not copy data into heap, write big pages can be faster?
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = PageMetaFormat::genWriteData(wb, page_file, edit);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    auto write_buf = [&](WritableFilePtr & file, UInt64 offset, ByteBuffer buf) {
        PageUtil::writeFile(file, offset, buf.begin(), buf.size());
        if (sync_on_write)
            PageUtil::syncFile(file);
    };

    write_buf(data_file, page_file.data_file_pos, data_buf);
    write_buf(meta_file, page_file.meta_file_pos, meta_buf);

    page_file.data_file_pos += data_buf.size();
    page_file.meta_file_pos += meta_buf.size();
}

// =========================================================
// PageFile::Reader
// =========================================================

PageFile::Reader::Reader(PageFile & page_file)
    : data_file_path(page_file.dataPath())
    , data_file{page_file.file_provider->newRandomAccessFile(page_file.dataPath(), page_file.dataEncryptionPath())}
{
}

PageFile::Reader::~Reader()
{
    data_file->close();
}

PageMap PageFile::Reader::read(PageIdAndEntries & to_read)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndEntry & a, const PageIdAndEntry & b) {
        return a.second.offset < b.second.offset;
    });

    // allocate data_buf that can hold all pages
    size_t buf_size = 0;
    for (const auto & p : to_read)
    {
        buf_size += p.second.size;
    }
    // TODO optimization:
    // 1. Succeeding pages can be read by one call.
    // 2. Pages with small gaps between them can also read together.
    // 3. Refactor this function to support iterator mode, and then use hint to do data pre-read.

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    char * pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, page_cache] : to_read)
    {
        PageUtil::readFile(data_file, page_cache.offset, pos, page_cache.size);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(pos, page_cache.size);
            if (checksum != page_cache.checksum)
            {
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path,
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id = page_id;
        page.data = ByteBuffer(pos, pos + page_cache.size);
        page.mem_holder = mem_holder;
        page_map.emplace(page_id, page);

        pos += page_cache.size;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return page_map;
}

void PageFile::Reader::read(PageIdAndEntries & to_read, const PageHandler & handler)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndEntry & a, const PageIdAndEntry & b) {
        return a.second.offset < b.second.offset;
    });

    size_t buf_size = 0;
    for (const auto & p : to_read)
        buf_size = std::max(buf_size, p.second.size);

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });


    auto it = to_read.begin();
    while (it != to_read.end())
    {
        auto && [page_id, page_cache] = *it;

        PageUtil::readFile(data_file, page_cache.offset, data_buf, page_cache.size);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(data_buf, page_cache.size);
            if (checksum != page_cache.checksum)
            {
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path,
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id = page_id;
        page.data = ByteBuffer(data_buf, data_buf + page_cache.size);
        page.mem_holder = mem_holder;

        ++it;

        //#ifndef __APPLE__
        //        if (it != to_read.end())
        //        {
        //            auto & next_page_cache = it->second;
        //            ::posix_fadvise(data_file_fd, next_page_cache.offset, next_page_cache.size, POSIX_FADV_WILLNEED);
        //        }
        //#endif

        handler(page_id, page);
    }
}

// =========================================================
// PageFile
// =========================================================

const PageFile::Version PageFile::CURRENT_VERSION = 1;

PageFile::PageFile(PageFileId file_id_, UInt32 level_, const std::string & parent_path, const FileProviderPtr & file_provider_, PageFile::Type type_, bool is_create, Poco::Logger * log_)
    : file_id(file_id_)
    , level(level_)
    , type(type_)
    , parent_path(parent_path)
    , file_provider(file_provider_)
    , data_file_pos(0)
    , meta_file_pos(0)
    , log(log_)
{
    if (is_create)
    {
        Poco::File file(folderPath());
        if (file.exists())
        {
            deleteEncryptionInfo();
            file.remove(true);
        }
        file.createDirectories();
    }
}

std::pair<PageFile, PageFile::Type>
PageFile::recover(const String & parent_path, const FileProviderPtr & file_provider_, const String & page_file_name, Poco::Logger * log)
{
    if (!startsWith(page_file_name, folder_prefix_formal) && !startsWith(page_file_name, folder_prefix_temp)
        && !startsWith(page_file_name, folder_prefix_legacy) && !startsWith(page_file_name, folder_prefix_checkpoint))
    {
        LOG_INFO(log, "Not page file, ignored {}", page_file_name);
        return {{}, Type::Invalid};
    }
    std::vector<std::string> ss;
    boost::split(ss, page_file_name, boost::is_any_of("_"));
    if (ss.size() != 3)
    {
        LOG_INFO(log, "Unrecognized file, ignored: {}", page_file_name);
        return {{}, Type::Invalid};
    }

    PageFileId file_id = std::stoull(ss[1]);
    UInt32 level = std::stoi(ss[2]);
    PageFile pf(file_id, level, parent_path, file_provider_, Type::Formal, /* is_create */ false, log);
    if (ss[0] == folder_prefix_temp)
    {
        LOG_INFO(log, "Temporary page file, ignored: {}", page_file_name);
        pf.type = Type::Temp;
        return {pf, Type::Temp};
    }
    else if (ss[0] == folder_prefix_legacy)
    {
        pf.type = Type::Legacy;
        // ensure meta exist
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: {}", pf.metaPath());
            return {{}, Type::Invalid};
        }

        return {pf, Type::Legacy};
    }
    else if (ss[0] == folder_prefix_formal)
    {
        // ensure both meta && data exist
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: {}", pf.metaPath());
            return {{}, Type::Invalid};
        }

        if (!Poco::File(pf.dataPath()).exists())
        {
            LOG_INFO(log, "Broken page without data file, ignored: {}", pf.dataPath());
            return {{}, Type::Invalid};
        }

        return {pf, Type::Formal};
    }
    else if (ss[0] == folder_prefix_checkpoint)
    {
        pf.type = Type::Checkpoint;
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: {}", pf.metaPath());
            return {{}, Type::Invalid};
        }

        return {pf, Type::Checkpoint};
    }

    LOG_INFO(log, "Unrecognized file prefix, ignored: {}", page_file_name);
    return {{}, Type::Invalid};
}

PageFile PageFile::newPageFile(PageFileId file_id, UInt32 level, const std::string & parent_path, const FileProviderPtr & file_provider, PageFile::Type type, Poco::Logger * log)
{
    return PageFile(file_id, level, parent_path, file_provider, type, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId file_id, UInt32 level, const std::string & parent_path, const FileProviderPtr & file_provider, PageFile::Type type, Poco::Logger * log)
{
    return PageFile(file_id, level, parent_path, file_provider, type, false, log);
}

void PageFile::readAndSetPageMetas(PageEntriesEdit & edit)
{
    const auto path = metaPath();
    Poco::File file(path);
    if (unlikely(!file.exists()))
        throw Exception("Try to read meta of PageFile_" + DB::toString(file_id) + "_" + DB::toString(level)
                            + ", but not exists. Path: " + path,
                        ErrorCodes::LOGICAL_ERROR);

    const size_t file_size = file.getSize();
    auto meta_file = file_provider->newRandomAccessFile(path, EncryptionPath(path, ""));

    char * meta_data = static_cast<char *>(alloc(file_size));
    SCOPE_EXIT({ free(meta_data, file_size); });
    meta_file->pread(meta_data, file_size, 0);

    // analyze meta file and update page_entries
    std::tie(this->meta_file_pos, this->data_file_pos)
        = PageMetaFormat::analyzeMetaFile(folderPath(), file_id, level, meta_data, file_size, edit, log);
}

void PageFile::setFormal()
{
    if (type != Type::Temp)
        return;
    auto old_meta_encryption_path = metaEncryptionPath();
    auto old_data_encryption_path = dataEncryptionPath();
    Poco::File file(folderPath());
    type = Type::Formal;
    file_provider->linkEncryptionInfo(old_meta_encryption_path, metaEncryptionPath());
    file_provider->linkEncryptionInfo(old_data_encryption_path, dataEncryptionPath());
    file.renameTo(folderPath());
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
    file_provider->deleteEncryptionInfo(old_data_encryption_path);
}

void PageFile::setLegacy()
{
    if (type != Type::Formal)
        return;
    // Rename to legacy dir. Note that we can NOT remove the data part before
    // successfully rename to legacy status.
    auto old_meta_encryption_path = metaEncryptionPath();
    auto old_data_encryption_path = dataEncryptionPath();
    Poco::File formal_dir(folderPath());
    type = Type::Legacy;
    file_provider->linkEncryptionInfo(old_meta_encryption_path, metaEncryptionPath());
    formal_dir.renameTo(folderPath());
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
    file_provider->deleteEncryptionInfo(old_data_encryption_path);
    // remove the data part
    if (auto data_file = Poco::File(dataPath()); data_file.exists())
    {
        data_file.remove();
    }
}

void PageFile::setCheckpoint()
{
    if (type != Type::Temp)
        return;
    auto old_meta_encryption_path = metaEncryptionPath();
    Poco::File file(folderPath());
    type = Type::Checkpoint;
    file_provider->linkEncryptionInfo(old_meta_encryption_path, metaEncryptionPath());
    file.renameTo(folderPath());
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
}

void PageFile::removeDataIfExists() const
{
    if (auto data_file = Poco::File(dataPath()); data_file.exists())
    {
        file_provider->deleteRegularFile(dataPath(), dataEncryptionPath());
    }
}

void PageFile::destroy() const
{
    // TODO: delay remove.
    Poco::File file(folderPath());
    if (file.exists())
    {
        // remove meta first, then remove data
        file_provider->deleteRegularFile(metaPath(), metaEncryptionPath());
        file_provider->deleteRegularFile(dataPath(), dataEncryptionPath());
    }
}

bool PageFile::isExist() const
{
    Poco::File file(folderPath());
    Poco::File data_file(dataPath());
    Poco::File meta_file(metaPath());
    return (file.exists() && data_file.exists() && meta_file.exists());
}

UInt64 PageFile::getDataFileSize() const
{
    if (type == Type::Legacy)
        return 0;
    Poco::File file(dataPath());
    return file.getSize();
}

String PageFile::folderPath() const
{
    String path = parent_path + "/";
    switch (type)
    {
    case Type::Formal:
        path += folder_prefix_formal;
        break;
    case Type::Temp:
        path += folder_prefix_temp;
        break;
    case Type::Legacy:
        path += folder_prefix_legacy;
        break;
    case Type::Checkpoint:
        path += folder_prefix_checkpoint;
        break;

    case Type::Invalid:
        throw Exception("Try to access folderPath of invalid page file.", ErrorCodes::LOGICAL_ERROR);
    }
    path += "_" + DB::toString(file_id) + "_" + DB::toString(level);
    return path;
}

} // namespace DB::PS::V1
