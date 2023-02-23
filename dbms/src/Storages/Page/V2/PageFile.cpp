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
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V2/PageFile.h>
#include <Storages/Page/WriteBatch.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <ext/scope_guard.h>

#ifndef __APPLE__
#include <fcntl.h>
#endif


namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
extern const char force_formal_page_file_not_exists[];
extern const char force_legacy_or_checkpoint_page_file_exists[];
} // namespace FailPoints

static constexpr bool PAGE_CHECKSUM_ON_READ = true;

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace PS::V2
{
// =========================================================
// Page Meta format
// =========================================================

namespace PageMetaFormat
{
using WBSize = UInt32;
// TODO we should align these alias with type in PageCache
using PageTag = UInt64;
using IsPut = std::underlying_type<WriteBatchWriteType>::type;
using PageOffset = UInt64;
using Checksum = UInt64;

struct PageFlags
{
    UInt32 flags = 0x00000000;

    // Detach page means the meta and data not in the same PageFile
    void setIsDetachPage() { flags |= 0x1; }
    bool isDetachPage() const { return flags & 0x1; }
};
static_assert(std::is_trivially_copyable_v<PageFlags>);
static_assert(sizeof(PageFlags) == sizeof(UInt32));

static const size_t PAGE_META_SIZE = sizeof(PageId) + sizeof(PageFileId) + sizeof(PageFileLevel) + sizeof(PageFlags) + sizeof(PageTag)
    + sizeof(PageOffset) + sizeof(PageSize) + sizeof(Checksum);

/// Return <data to write into meta file, data to write into data file>.
std::pair<ByteBuffer, ByteBuffer> genWriteData( //
    DB::WriteBatch & wb,
    PageFile & page_file,
    PageEntriesEdit & edit)
{
    WBSize meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    meta_write_bytes += sizeof(WBSize) + sizeof(PageFormat::Version) + sizeof(WriteBatch::SequenceID);

    for (auto & write : wb.getWrites())
    {
        meta_write_bytes += sizeof(IsPut);
        // We don't serialize `PUT_EXTERNAL` for V2, just convert it to `PUT`
        if (write.type == WriteBatchWriteType::PUT_EXTERNAL)
            write.type = WriteBatchWriteType::PUT;

        switch (write.type)
        {
        case WriteBatchWriteType::PUT:
        case WriteBatchWriteType::UPSERT:
            if (write.read_buffer)
                data_write_bytes += write.size;
            meta_write_bytes += PAGE_META_SIZE;
            meta_write_bytes += sizeof(UInt64); // size of field_offsets + checksum
            meta_write_bytes += ((sizeof(UInt64) + sizeof(UInt64)) * write.offsets.size());
            break;
        case WriteBatchWriteType::DEL:
            // For delete page, store page id only. And don't need to write data file.
            meta_write_bytes += sizeof(PageId);
            break;
        case WriteBatchWriteType::REF:
            // For ref page, store RefPageId -> PageId. And don't need to write data file.
            meta_write_bytes += (sizeof(PageId) + sizeof(PageId));
            break;
        case WriteBatchWriteType::PUT_EXTERNAL:
            throw Exception("Should not serialize with `PUT_EXTERNAL`");
            break;
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }
    }

    meta_write_bytes += sizeof(Checksum);

    char * meta_buffer = static_cast<char *>(page_file.alloc(meta_write_bytes));
    char * data_buffer = static_cast<char *>(page_file.alloc(data_write_bytes));

    char * meta_pos = meta_buffer;
    char * data_pos = data_buffer;

    PageUtil::put(meta_pos, meta_write_bytes);
    PageUtil::put(meta_pos, PageFormat::V2);
    PageUtil::put(meta_pos, wb.getSequence());

    PageOffset page_data_file_off = page_file.getDataFileAppendPos();
    for (auto & write : wb.getWrites())
    {
        // We don't serialize `PUT_EXTERNAL` for V2, just convert it to `PUT`
        if (write.type == WriteBatchWriteType::PUT_EXTERNAL)
            write.type = WriteBatchWriteType::PUT;

        PageUtil::put(meta_pos, static_cast<IsPut>(write.type));
        switch (write.type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
            throw Exception("Should not serialize with `PUT_EXTERNAL`");
            break;
        case WriteBatchWriteType::PUT:
        case WriteBatchWriteType::UPSERT:
        {
            PageFlags flags;
            Checksum page_checksum = 0;
            PageOffset page_offset = 0;
            if (write.read_buffer)
            {
                write.read_buffer->readStrict(data_pos, write.size);
                page_checksum = CityHash_v1_0_2::CityHash64(data_pos, write.size);
                page_offset = page_data_file_off;
                // In this case, checksum of each fields (inside `write.offsets[i].second`)
                // is simply 0, we need to calulate the checksums of each fields
                for (size_t i = 0; i < write.offsets.size(); ++i)
                {
                    const auto field_beg = write.offsets[i].first;
                    const auto field_end = (i == write.offsets.size() - 1) ? write.size : write.offsets[i + 1].first;
                    write.offsets[i].second = CityHash_v1_0_2::CityHash64(data_pos + field_beg, field_end - field_beg);
                }

                data_pos += write.size;
                page_data_file_off += write.size;
            }
            else
            {
                // Notice: in this case, we need to copy checksum instead of calculate from buffer(which is null)
                // Do NOT use `data_pos` outside this if-else branch

                // get page_checksum from write when read_buffer is nullptr
                flags.setIsDetachPage();
                page_checksum = write.page_checksum;
                page_offset = write.page_offset;
                // `entry.field_offsets`(and checksum) just simply copy `write.offsets`
                // page_data_file_off += 0;
            }

            // UPSERT may point to another PageFile
            PageEntry entry;
            entry.file_id = (write.type == WriteBatchWriteType::PUT ? page_file.getFileId() : write.target_file_id.first);
            entry.level = (write.type == WriteBatchWriteType::PUT ? page_file.getLevel() : write.target_file_id.second);
            entry.tag = write.tag;
            entry.size = write.size;
            entry.offset = page_offset;
            entry.checksum = page_checksum;

            // entry.field_offsets = write.offsets;
            // we can swap from WriteBatch instead of copying
            entry.field_offsets.swap(write.offsets);

            PageUtil::put(meta_pos, static_cast<PageId>(write.page_id));
            PageUtil::put(meta_pos, static_cast<PageFileId>(entry.file_id));
            PageUtil::put(meta_pos, static_cast<PageFileLevel>(entry.level));
            PageUtil::put(meta_pos, static_cast<UInt32>(flags.flags));
            PageUtil::put(meta_pos, static_cast<PageTag>(write.tag));
            PageUtil::put(meta_pos, static_cast<PageOffset>(entry.offset));
            PageUtil::put(meta_pos, static_cast<PageSize>(write.size));
            PageUtil::put(meta_pos, static_cast<Checksum>(page_checksum));

            PageUtil::put(meta_pos, static_cast<UInt64>(entry.field_offsets.size()));
            for (const auto & field_offset : entry.field_offsets)
            {
                PageUtil::put(meta_pos, static_cast<UInt64>(field_offset.first));
                PageUtil::put(meta_pos, static_cast<UInt64>(field_offset.second));
            }

            if (write.type == WriteBatchWriteType::PUT)
                edit.put(write.page_id, entry);
            else if (write.type == WriteBatchWriteType::UPSERT)
                edit.upsertPage(write.page_id, entry);

            break;
        }
        case WriteBatchWriteType::DEL:
            PageUtil::put(meta_pos, static_cast<PageId>(write.page_id));

            edit.del(write.page_id);
            break;
        case WriteBatchWriteType::REF:
            PageUtil::put(meta_pos, static_cast<PageId>(write.page_id));
            PageUtil::put(meta_pos, static_cast<PageId>(write.ori_page_id));

            edit.ref(write.page_id, write.ori_page_id);
            break;
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }
    }

    const Checksum wb_checksum = CityHash_v1_0_2::CityHash64(meta_buffer, meta_write_bytes - sizeof(Checksum));
    PageUtil::put(meta_pos, wb_checksum);

    if (unlikely(meta_pos != meta_buffer + meta_write_bytes || data_pos != data_buffer + data_write_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return {{meta_buffer, meta_pos}, {data_buffer, data_pos}};
}
} // namespace PageMetaFormat

// =========================================================
// PageFile::LinkingMetaAdapter
// =========================================================

PageFile::LinkingMetaAdapter::LinkingMetaAdapter(PageFile & page_file_)
    : page_file(page_file_)
{}

PageFile::LinkingMetaAdapter::~LinkingMetaAdapter()
{
    page_file.free(meta_buffer, meta_size);
}

bool PageFile::LinkingMetaAdapter::initialize(const ReadLimiterPtr & read_limiter)
{
    const auto path = page_file.metaPath();

    Poco::File file(path);
    if (unlikely(!file.exists()))
        throw Exception("Try to read meta of " + page_file.toString() + ", but not exists. Path: " + path, ErrorCodes::LOGICAL_ERROR);

    meta_size = file.getSize();

    if (meta_size == 0) // Empty file
    {
        return false;
    }

    auto underlying_file = page_file.file_provider->newRandomAccessFile(path, page_file.metaEncryptionPath());
    // File not exists.
    if (unlikely(underlying_file->getFd() == -1))
        throw Exception("Try to read meta of " + page_file.toString() + ", but open file error. Path: " + path, ErrorCodes::LOGICAL_ERROR);

    SCOPE_EXIT({ underlying_file->close(); });

    meta_buffer = static_cast<char *>(page_file.alloc(meta_size));
    PageUtil::readFile(underlying_file, 0, meta_buffer, meta_size, read_limiter);

    return true;
}

PageFile::LinkingMetaAdapterPtr PageFile::LinkingMetaAdapter::createFrom(PageFile & page_file, const ReadLimiterPtr & read_limiter)
{
    auto reader = std::make_shared<PageFile::LinkingMetaAdapter>(page_file);
    if (!reader->initialize(read_limiter))
    {
        return nullptr;
    }
    return reader;
}

bool PageFile::LinkingMetaAdapter::hasNext() const
{
    return meta_file_offset < meta_size;
}

bool PageFile::LinkingMetaAdapter::linkToNewSequenceNext(WriteBatch::SequenceID sid, PageEntriesEdit & edit, UInt64 file_id, UInt64 level)
{
    char * meta_data_end = meta_buffer + meta_size;
    char * pos = meta_buffer + meta_file_offset;
    if (pos + sizeof(PageMetaFormat::WBSize) > meta_data_end)
    {
        LOG_WARNING(page_file.log,
                    "[batch_start_pos={}] [meta_size={}] [file={}] ignored.",
                    meta_file_offset,
                    meta_size,
                    page_file.metaPath());
        return false;
    }

    const char * wb_start_pos = pos;
    const auto wb_bytes = PageUtil::get<PageMetaFormat::WBSize>(pos);
    if (wb_start_pos + wb_bytes > meta_data_end)
    {
        LOG_WARNING(page_file.log,
                    "[expect_batch_bytes={}] [meta_size={}] [file={}] ignored.",
                    wb_bytes,
                    meta_size,
                    page_file.metaPath());
        return false;
    }

    WriteBatch::SequenceID wb_sequence = 0;
    const auto binary_version = PageUtil::get<PageFormat::Version>(pos);
    char * sequence_pos = pos;
    switch (binary_version)
    {
    case PageFormat::V1:
        wb_sequence = 0;
        break;
    case PageFormat::V2:
        wb_sequence = PageUtil::get<WriteBatch::SequenceID>(pos);
        break;
    default:
        throw Exception("[unknown_version=" + DB::toString(binary_version) + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    if (wb_sequence > sid)
    {
        throw Exception("Current sequence is invaild", ErrorCodes::LOGICAL_ERROR);
    }

    // check the checksum of WriteBatch
    const auto wb_bytes_without_checksum = wb_bytes - sizeof(PageMetaFormat::Checksum);
    const auto wb_checksum = PageUtil::get<PageMetaFormat::Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
    const auto checksum_calc = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
    if (wb_checksum != checksum_calc)
    {
        std::stringstream ss;
        ss << "[expecte_checksum=" << std::hex << wb_checksum << "] [actual_checksum" << checksum_calc << "]";
        throw Exception("[path=" + page_file.folderPath() + "] [batch_bytes=" + DB::toString(wb_bytes) + "] " + ss.str(),
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    // update the wb sequence id
    PageUtil::put<WriteBatch::SequenceID>(sequence_pos, sid);
    char * checksum_pos = meta_buffer + meta_file_offset;
    checksum_pos += wb_bytes_without_checksum;

    // recover WriteBatch
    while (pos < wb_start_pos + wb_bytes_without_checksum)
    {
        const auto write_type = static_cast<WriteBatchWriteType>(PageUtil::get<PageMetaFormat::IsPut>(pos));
        switch (write_type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
            throw Exception("Should not deserialize with PUT_EXTERNAL");
            break;
        case WriteBatchWriteType::PUT:
        case WriteBatchWriteType::UPSERT:
        {
            PageMetaFormat::PageFlags flags;
            auto page_id = PageUtil::get<PageId>(pos);
            PageEntry entry;
            switch (binary_version)
            {
            case PageFormat::V1:
            {
                entry.file_id = file_id;
                entry.level = level;
                break;
            }
            case PageFormat::V2:
            {
                PageUtil::put<PageFileId>(pos, file_id);
                PageUtil::put<PageFileLevel>(pos, level);
                entry.file_id = file_id;
                entry.level = level;
                flags = PageUtil::get<PageMetaFormat::PageFlags>(pos);
                break;
            }
            default:
                throw Exception("PageFile binary version not match [unknown_version=" + DB::toString(binary_version)
                                    + "] [file=" + page_file.metaPath() + "]",
                                ErrorCodes::LOGICAL_ERROR);
            }

            entry.tag = PageUtil::get<PageMetaFormat::PageTag>(pos);
            entry.offset = PageUtil::get<PageMetaFormat::PageOffset>(pos);
            entry.size = PageUtil::get<PageSize>(pos);
            entry.checksum = PageUtil::get<PageMetaFormat::Checksum>(pos);

            if (binary_version == PageFormat::V2)
            {
                const auto num_fields = PageUtil::get<UInt64>(pos);
                entry.field_offsets.reserve(num_fields);
                for (size_t i = 0; i < num_fields; ++i)
                {
                    auto field_offset = PageUtil::get<UInt64>(pos);
                    auto field_checksum = PageUtil::get<UInt64>(pos);
                    entry.field_offsets.emplace_back(field_offset, field_checksum);
                }
            }

            // Wheather put or upsert should change to upsert.
            edit.upsertPage(page_id, entry);
            break;
        }
        case WriteBatchWriteType::DEL:
        {
            pos += sizeof(PageId);
            break;
        }
        case WriteBatchWriteType::REF:
        {
            // ref_id
            pos += sizeof(PageId);
            // page_id
            pos += sizeof(PageId);
            break;
        }
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }
    }

    // update the checksum since sequence id && page file id, level have been changed
    const auto new_checksum = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
    PageUtil::put<PageMetaFormat::Checksum>(checksum_pos, new_checksum);

    // move `pos` over the checksum of WriteBatch
    pos += sizeof(PageMetaFormat::Checksum);

    if (unlikely(pos != wb_start_pos + wb_bytes))
        throw Exception("[batch_bytes=" + DB::toString(wb_bytes) + "] [actual_bytes=" + DB::toString(pos - wb_start_pos)
                            + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);

    meta_file_offset = pos - meta_buffer;

    return true;
}

// =========================================================
// PageFile::MetaMergingReader
// =========================================================

PageFile::MetaMergingReader::MetaMergingReader(PageFile & page_file_)
    : page_file(page_file_)
{}

PageFile::MetaMergingReader::~MetaMergingReader()
{
    page_file.free(meta_buffer, meta_size);
}

PageFile::MetaMergingReaderPtr PageFile::MetaMergingReader::createFrom(
    PageFile & page_file,
    size_t max_meta_offset,
    const ReadLimiterPtr & read_limiter,
    const bool background)
{
    auto reader = std::make_shared<PageFile::MetaMergingReader>(page_file);
    reader->initialize(max_meta_offset, read_limiter, background);
    return reader;
}

PageFile::MetaMergingReaderPtr PageFile::MetaMergingReader::createFrom(PageFile & page_file, const ReadLimiterPtr & read_limiter, const bool background)
{
    auto reader = std::make_shared<PageFile::MetaMergingReader>(page_file);
    reader->initialize(std::nullopt, read_limiter, background);
    return reader;
}

// Try to initiallize access to meta, read the whole metadata to memory.
// Status -> Finished if metadata size is zero.
//        -> Opened if metadata successfully load from disk.
void PageFile::MetaMergingReader::initialize(std::optional<size_t> max_meta_offset, const ReadLimiterPtr & read_limiter, const bool background)
{
    if (status == Status::Opened)
        return;
    else if (status == Status::Finished)
        throw Exception("Try to reopen finished merging reader: " + toString(), ErrorCodes::LOGICAL_ERROR);

    const auto path = page_file.metaPath();
    Poco::File file(path);
    if (unlikely(!file.exists()))
        throw Exception("Try to read meta of " + page_file.toString() + ", but not exists. Path: " + path, ErrorCodes::LOGICAL_ERROR);

    // If caller have not set the meta offset limit, we need to
    // initialize `meta_size` with the file size.
    if (!max_meta_offset)
    {
        meta_size = file.getSize();
    }
    else
    {
        meta_size = *max_meta_offset;
    }

    if (meta_size == 0) // Empty file
    {
        status = Status::Finished;
        return;
    }

    auto underlying_file = page_file.file_provider->newRandomAccessFile(path, page_file.metaEncryptionPath());
    // File not exists.
    if (unlikely(underlying_file->getFd() == -1))
        throw Exception("Try to read meta of " + page_file.toString() + ", but open file error. Path: " + path, ErrorCodes::LOGICAL_ERROR);
    SCOPE_EXIT({ underlying_file->close(); });
    meta_buffer = static_cast<char *>(page_file.alloc(meta_size));
    PageUtil::readFile(underlying_file, 0, meta_buffer, meta_size, read_limiter, background);
    status = Status::Opened;
}

bool PageFile::MetaMergingReader::hasNext() const
{
    return status == Status::Opened && meta_file_offset < meta_size;
}

void PageFile::MetaMergingReader::moveNext(PageFormat::Version * v)
{
    curr_edit.clear();
    curr_write_batch_sequence = 0;

    if (status == Status::Finished)
        return;

    char * meta_data_end = meta_buffer + meta_size;
    char * pos = meta_buffer + meta_file_offset;
    if (pos + sizeof(PageMetaFormat::WBSize) > meta_data_end)
    {
        LOG_WARNING(page_file.log,
                    "Incomplete write batch {{{}}} [batch_start_pos={}] [meta_size={}] [file={}] ignored.",
                    toString(),
                    meta_file_offset,
                    meta_size,
                    page_file.metaPath());
        status = Status::Finished;
        return;
    }
    const char * wb_start_pos = pos;
    const auto wb_bytes = PageUtil::get<PageMetaFormat::WBSize>(pos);
    if (wb_start_pos + wb_bytes > meta_data_end)
    {
        LOG_WARNING(page_file.log,
                    "Incomplete write batch {{{}}} [expect_batch_bytes={}] [meta_size={}] [file={}] ignored.",
                    toString(),
                    wb_bytes,
                    meta_size,
                    page_file.metaPath());
        status = Status::Finished;
        return;
    }

    WriteBatch::SequenceID wb_sequence = 0;
    const auto binary_version = PageUtil::get<PageFormat::Version>(pos);
    switch (binary_version)
    {
    case PageFormat::V1:
        wb_sequence = 0;
        break;
    case PageFormat::V2:
        wb_sequence = PageUtil::get<WriteBatch::SequenceID>(pos);
        break;
    default:
        throw Exception("PageFile binary version not match {" + toString() + "} [unknown_version=" + DB::toString(binary_version)
                            + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    // return the binary_version if `v` is not null
    if (unlikely(v != nullptr))
        *v = binary_version;

    // check the checksum of WriteBatch
    const auto wb_bytes_without_checksum = wb_bytes - sizeof(PageMetaFormat::Checksum);
    const auto wb_checksum = PageUtil::get<PageMetaFormat::Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
    const auto checksum_calc = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
    if (wb_checksum != checksum_calc)
    {
        std::stringstream ss;
        ss << "[expecte_checksum=" << std::hex << wb_checksum << "] [actual_checksum" << checksum_calc << "]";
        throw Exception("Write batch checksum not match {" + toString() + "} [path=" + page_file.folderPath()
                            + "] [batch_bytes=" + DB::toString(wb_bytes) + "] " + ss.str(),
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    // recover WriteBatch
    size_t curr_wb_data_offset = 0;
    while (pos < wb_start_pos + wb_bytes_without_checksum)
    {
        const auto write_type = static_cast<WriteBatchWriteType>(PageUtil::get<PageMetaFormat::IsPut>(pos));
        switch (write_type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
            throw Exception("Should not deserialize with PUT_EXTERNAL");
            break;
        case WriteBatchWriteType::PUT:
        case WriteBatchWriteType::UPSERT:
        {
            PageMetaFormat::PageFlags flags;

            auto page_id = PageUtil::get<PageId>(pos);
            PageEntry entry;
            switch (binary_version)
            {
            case PageFormat::V1:
            {
                entry.file_id = page_file.getFileId();
                entry.level = page_file.getLevel();
                break;
            }
            case PageFormat::V2:
            {
                entry.file_id = PageUtil::get<PageFileId>(pos);
                entry.level = PageUtil::get<PageFileLevel>(pos);
                flags = PageUtil::get<PageMetaFormat::PageFlags>(pos);
                break;
            }
            default:
                throw Exception("PageFile binary version not match {" + toString() + "} [unknown_version=" + DB::toString(binary_version)
                                    + "] [file=" + page_file.metaPath() + "]",
                                ErrorCodes::LOGICAL_ERROR);
            }

            entry.tag = PageUtil::get<PageMetaFormat::PageTag>(pos);
            entry.offset = PageUtil::get<PageMetaFormat::PageOffset>(pos);
            entry.size = PageUtil::get<PageSize>(pos);
            entry.checksum = PageUtil::get<PageMetaFormat::Checksum>(pos);

            if (binary_version == PageFormat::V2)
            {
                const auto num_fields = PageUtil::get<UInt64>(pos);
                entry.field_offsets.reserve(num_fields);
                for (size_t i = 0; i < num_fields; ++i)
                {
                    auto field_offset = PageUtil::get<UInt64>(pos);
                    auto field_checksum = PageUtil::get<UInt64>(pos);
                    entry.field_offsets.emplace_back(field_offset, field_checksum);
                }
            }

            if (write_type == WriteBatchWriteType::PUT)
            {
                curr_edit.put(page_id, entry);
                curr_wb_data_offset += entry.size;
            }
            else if (write_type == WriteBatchWriteType::UPSERT)
            {
                curr_edit.upsertPage(page_id, entry);
                // If it is a deatch page, don't move data offset.
                curr_wb_data_offset += flags.isDetachPage() ? 0 : entry.size;
            }
            break;
        }
        case WriteBatchWriteType::DEL:
        {
            auto page_id = PageUtil::get<PageId>(pos);
            curr_edit.del(page_id);
            break;
        }
        case WriteBatchWriteType::REF:
        {
            const auto ref_id = PageUtil::get<PageId>(pos);
            const auto page_id = PageUtil::get<PageId>(pos);
            curr_edit.ref(ref_id, page_id);
            break;
        }
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
        }
    }
    // move `pos` over the checksum of WriteBatch
    pos += sizeof(PageMetaFormat::Checksum);

    if (unlikely(pos != wb_start_pos + wb_bytes))
        throw Exception("pos not match {" + toString() + "} [batch_bytes=" + DB::toString(wb_bytes)
                            + "] [actual_bytes=" + DB::toString(pos - wb_start_pos) + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);

    curr_write_batch_sequence = wb_sequence;
    meta_file_offset = pos - meta_buffer;
    data_file_offset += curr_wb_data_offset;
}

// =========================================================
// PageFile::Writer
// =========================================================

PageFile::Writer::Writer(PageFile & page_file_, bool sync_on_write_, bool truncate_if_exists)
    : page_file(page_file_)
    , sync_on_write(sync_on_write_)
    , data_file{nullptr}
    , meta_file{nullptr}
    , last_write_time(Clock::now())
{
    // Create data and meta file, prevent empty page folder from being removed by GC.
    data_file = page_file.file_provider->newWritableFile(
        page_file.dataPath(),
        page_file.dataEncryptionPath(),
        truncate_if_exists,
        /*create_new_encryption_info_*/ truncate_if_exists);
    meta_file = page_file.file_provider->newWritableFile(
        page_file.metaPath(),
        page_file.metaEncryptionPath(),
        truncate_if_exists,
        /*create_new_encryption_info_*/ truncate_if_exists);
    data_file->close();
    meta_file->close();
}

PageFile::Writer::~Writer()
{
    if (data_file->isClosed())
        return;

    closeFd();
}

void PageFile::Writer::hardlinkFrom(PageFile & linked_file, WriteBatch::SequenceID sid, PageEntriesEdit & edit)
{
    auto reader = PageFile::LinkingMetaAdapter::createFrom(linked_file);

    if (!reader)
    {
        throw Exception("Can not open linked page file reader.", ErrorCodes::LOGICAL_ERROR);
    }

    if (meta_file->isClosed())
    {
        meta_file->open();
    }

    // Move to the SequenceID item
    while (reader->hasNext())
    {
        if (!reader->linkToNewSequenceNext(sid, edit, page_file.getFileId(), page_file.getLevel()))
        {
            throw Exception(fmt::format("Failed to update [sid={}] into [file_id={}] , [file_level={}]", sid, page_file.getFileId(), page_file.getLevel()),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    char * linked_meta_data;
    size_t linked_meta_size;

    std::tie(linked_meta_data, linked_meta_size) = reader->getMetaInfo();

    PageUtil::writeFile(meta_file, 0, linked_meta_data, linked_meta_size, nullptr);
    PageUtil::syncFile(meta_file);
    data_file->hardLink(linked_file.dataPath());
}


const String & PageFile::Writer::parentPath() const
{
    return page_file.parent_path;
}

size_t PageFile::Writer::write(DB::WriteBatch & wb, PageEntriesEdit & edit, const WriteLimiterPtr & write_limiter, bool background)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    if (data_file->isClosed())
    {
        data_file->open();
        meta_file->open();
    }

    // TODO: investigate if not copy data into heap, write big pages can be faster?
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = PageMetaFormat::genWriteData(wb, page_file, edit);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    auto write_buf = [&](WritableFilePtr & file, UInt64 offset, ByteBuffer buf, bool enable_failpoint) {
        PageUtil::writeFile(
            file,
            offset,
            buf.begin(),
            buf.size(),
            write_limiter,
            background,
            /*truncate_if_failed=*/true,
            /*enable_failpoint=*/enable_failpoint);
        if (sync_on_write)
            PageUtil::syncFile(file);
    };
    write_buf(data_file, page_file.data_file_pos, data_buf, false);
    write_buf(meta_file, page_file.meta_file_pos, meta_buf, true);

    fiu_do_on(FailPoints::exception_before_page_file_write_sync,
              { // Mock that writing page file meta is not completed
                  auto f = Poco::File(meta_file->getFileName());
                  auto size = f.getSize();
                  f.setSize(size - 2);
                  auto size_after = f.getSize();
                  LOG_WARNING(page_file.log,
                              "Failpoint truncate [file={}] [origin_size={}] [truncated_size={}]",
                              meta_file->getFileName(),
                              size,
                              size_after);
                  throw Exception(String("Fail point ") + FailPoints::exception_before_page_file_write_sync + " is triggered.",
                                  ErrorCodes::FAIL_POINT_ERROR);
              });

    page_file.data_file_pos += data_buf.size();
    page_file.meta_file_pos += meta_buf.size();

    // Caller should ensure there are no writing to this Writer while
    // calling `tryCloseIdleFd`, so don't need to protect `last_write_time` now.
    last_write_time = Clock::now();

    // return how may bytes written
    return data_buf.size() + meta_buf.size();
}

// Caller should ensure there are no writing to this Writer while
// calling `tryCloseIdleFd`
void PageFile::Writer::tryCloseIdleFd(const Seconds & max_idle_time)
{
    if (max_idle_time.count() == 0 || data_file->isClosed())
        return;
    if (Clock::now() - last_write_time >= max_idle_time)
        closeFd();
}

PageFileIdAndLevel PageFile::Writer::fileIdLevel() const
{
    return page_file.fileIdLevel();
}

void PageFile::Writer::closeFd()
{
    if (data_file->isClosed())
        return;

    SCOPE_EXIT({
        data_file->close();
        meta_file->close();
    });
    PageUtil::syncFile(data_file);
    PageUtil::syncFile(meta_file);
}

// =========================================================
// PageFile::Reader
// =========================================================

PageFile::Reader::Reader(PageFile & page_file)
    : data_file_path(page_file.dataPath())
    , data_file{page_file.file_provider->newRandomAccessFile(page_file.dataPath(), page_file.dataEncryptionPath())}
    , last_read_time(Clock::now())
{
}

PageFile::Reader::~Reader()
{
    data_file->close();
}

PageMap PageFile::Reader::read(PageIdAndEntries & to_read, const ReadLimiterPtr & read_limiter, bool background)
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
    for (const auto & [page_id, entry] : to_read)
    {
        PageUtil::readFile(data_file, entry.offset, pos, entry.size, read_limiter, background);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(pos, entry.size);
            if (unlikely(entry.size != 0 && checksum != entry.checksum))
            {
                std::stringstream ss;
                ss << ", expected: " << std::hex << entry.checksum << ", but: " << checksum;
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path + ss.str(),
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.data = ByteBuffer(pos, pos + entry.size);
        page.mem_holder = mem_holder;

        // Calculate the field_offsets from page entry
        for (size_t index = 0; index < entry.field_offsets.size(); index++)
        {
            const auto offset = entry.field_offsets[index].first;
            page.field_offsets.emplace(index, offset);
        }

        page_map.emplace(page_id, std::move(page));

        pos += entry.size;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    last_read_time = Clock::now();

    return page_map;
}

PageMap PageFile::Reader::read(PageFile::Reader::FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(
        to_read.begin(),
        to_read.end(),
        [](const FieldReadInfo & a, const FieldReadInfo & b) { return a.entry.offset < b.entry.offset; });

    // allocate data_buf that can hold all pages with specify fields
    size_t buf_size = 0;
    for (auto & [page_id, entry, fields] : to_read)
    {
        (void)page_id;
        // Sort fields to get better read on disk
        std::sort(fields.begin(), fields.end());
        for (const auto field_index : fields)
        {
            buf_size += entry.getFieldSize(field_index);
        }
    }
    // TODO optimization:
    // 1. Succeeding pages can be read by one call.
    // 2. Pages with small gaps between them can also read together.
    // 3. Refactor this function to support iterator mode, and then use hint to do data pre-read.

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    std::set<FieldOffsetInsidePage> fields_offset_in_page;

    char * pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, entry, fields] : to_read)
    {
        size_t read_size_this_entry = 0;
        char * write_offset = pos;

        for (const auto field_index : fields)
        {
            // TODO: Continuously fields can read by one system call.
            const auto [beg_offset, end_offset] = entry.getFieldOffsets(field_index);
            const auto size_to_read = end_offset - beg_offset;
            PageUtil::readFile(data_file, entry.offset + beg_offset, write_offset, size_to_read, read_limiter);
            fields_offset_in_page.emplace(field_index, read_size_this_entry);

            if constexpr (PAGE_CHECKSUM_ON_READ)
            {
                auto expect_checksum = entry.field_offsets[field_index].second;
                auto field_checksum = CityHash_v1_0_2::CityHash64(write_offset, size_to_read);
                if (unlikely(entry.size != 0 && field_checksum != expect_checksum))
                {
                    std::stringstream ss;
                    ss << ", expected: " << std::hex << expect_checksum << ", but: " << field_checksum;
                    throw Exception("Page[" + DB::toString(page_id) + "] field[" + DB::toString(field_index)
                                        + "] checksum not match, broken file: " + data_file_path + ss.str(),
                                    ErrorCodes::CHECKSUM_DOESNT_MATCH);
                }
            }

            read_size_this_entry += size_to_read;
            write_offset += size_to_read;
        }

        Page page;
        page.data = ByteBuffer(pos, write_offset);
        page.mem_holder = mem_holder;
        page.field_offsets.swap(fields_offset_in_page);
        fields_offset_in_page.clear();
        page_map.emplace(page_id, std::move(page));

        pos = write_offset;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception("Pos not match, expect to read " + DB::toString(buf_size) + " bytes, but only " + DB::toString(pos - data_buf),
                        ErrorCodes::LOGICAL_ERROR);

    last_read_time = Clock::now();

    return page_map;
}

Page PageFile::Reader::read(FieldReadInfo & to_read, const ReadLimiterPtr & read_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, 1);

    size_t buf_size = 0;

    std::sort(to_read.fields.begin(), to_read.fields.end());
    for (const auto field_index : to_read.fields)
    {
        buf_size += to_read.entry.getFieldSize(field_index);
    }

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    Page page_rc;
    std::set<FieldOffsetInsidePage> fields_offset_in_page;

    size_t read_size_this_entry = 0;
    char * write_offset = data_buf;

    for (const auto field_index : to_read.fields)
    {
        // TODO: Continuously fields can read by one system call.
        const auto [beg_offset, end_offset] = to_read.entry.getFieldOffsets(field_index);
        const auto size_to_read = end_offset - beg_offset;
        PageUtil::readFile(data_file, to_read.entry.offset + beg_offset, write_offset, size_to_read, read_limiter);
        fields_offset_in_page.emplace(field_index, read_size_this_entry);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto expect_checksum = to_read.entry.field_offsets[field_index].second;
            auto field_checksum = CityHash_v1_0_2::CityHash64(write_offset, size_to_read);
            if (unlikely(to_read.entry.size != 0 && field_checksum != expect_checksum))
            {
                throw Exception(fmt::format("Page [{}] field [{}], entry offset [{}], entry size[{}], checksum not match, "
                                            "broken file: {},  expected: 0x{:X}, but: 0x{:X}",
                                            to_read.page_id,
                                            field_index,
                                            to_read.entry.offset,
                                            to_read.entry.size,
                                            data_file_path,
                                            expect_checksum,
                                            field_checksum),
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }

            read_size_this_entry += size_to_read;
            write_offset += size_to_read;
        }
    }

    Page page;
    page.data = ByteBuffer(data_buf, write_offset);
    page.mem_holder = mem_holder;
    page.field_offsets.swap(fields_offset_in_page);

    if (unlikely(write_offset != data_buf + buf_size))
    {
        throw Exception(fmt::format("Pos not match, expect to read {} bytes, but only {}.", buf_size, write_offset - data_buf),
                        ErrorCodes::LOGICAL_ERROR);
    }

    last_read_time = Clock::now();

    return page;
}

bool PageFile::Reader::isIdle(const Seconds & max_idle_time)
{
    if (max_idle_time.count() == 0)
        return false;
    return (Clock::now() - last_read_time >= max_idle_time);
}

// =========================================================
// PageFile
// =========================================================

PageFile::PageFile(PageFileId file_id_,
                   UInt32 level_,
                   const std::string & parent_path,
                   const FileProviderPtr & file_provider_,
                   PageFile::Type type_,
                   bool is_create,
                   Poco::Logger * log_)
    : file_id(file_id_)
    , level(level_)
    , type(type_)
    , parent_path(parent_path)
    , file_provider{file_provider_}
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

PageFile PageFile::newPageFile(PageFileId file_id,
                               UInt32 level,
                               const std::string & parent_path,
                               const FileProviderPtr & file_provider_,
                               PageFile::Type type,
                               Poco::Logger * log)
{
#ifndef NDEBUG
    // PageStorage may create a "Formal" PageFile for writing,
    // or a "Temp" PageFile for gc data.
    if (type != PageFile::Type::Temp && type != PageFile::Type::Formal)
        throw Exception("Should not create page file with type: " + typeToString(type));
#endif
    return PageFile(file_id, level, parent_path, file_provider_, type, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId file_id,
                                       UInt32 level,
                                       const std::string & parent_path,
                                       const FileProviderPtr & file_provider_,
                                       PageFile::Type type,
                                       Poco::Logger * log)
{
    return PageFile(file_id, level, parent_path, file_provider_, type, false, log);
}

bool PageFile::isPageFileExist(
    PageFileIdAndLevel file_id,
    const String & parent_path,
    const FileProviderPtr & file_provider_,
    Type type,
    Poco::Logger * log)
{
    PageFile pf = openPageFileForRead(file_id.first, file_id.second, parent_path, file_provider_, type, log);
    return pf.isExist();
}

void PageFile::setFileAppendPos(size_t meta_pos, size_t data_pos)
{
    meta_file_pos = meta_pos;
    data_file_pos = data_pos;

    // If `meta_file_pos` is less than the file size on disk, it must
    // be some incomplete write batches meta been ignored.
    // Truncate the meta file size to throw those broken meta away.
    Poco::File meta_on_disk(metaPath());
    const auto meta_size_on_disk = meta_on_disk.getSize();
    if (unlikely(meta_size_on_disk != meta_pos))
    {
        LOG_WARNING(log,
                    "Truncate incomplete write batches [orig_size={}] [set_size={}] [file={}]",
                    meta_size_on_disk,
                    meta_file_pos,
                    metaPath());
        meta_on_disk.setSize(meta_file_pos);
    }
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
    try
    {
        file.renameTo(folderPath());
    }
    catch (Poco::Exception & e)
    {
        throw DB::Exception(e); // wrap Poco::Exception as DB::Exception for better stack backtrace
    }
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
    file_provider->deleteEncryptionInfo(old_data_encryption_path);
}

size_t PageFile::setLegacy()
{
    if (type != Type::Formal)
        return 0;
    // Rename to legacy dir. Note that we can NOT remove the data part before
    // successfully rename to legacy status.
    auto old_meta_encryption_path = metaEncryptionPath();
    auto old_data_encryption_path = dataEncryptionPath();
    Poco::File formal_dir(folderPath());
    type = Type::Legacy;
    file_provider->linkEncryptionInfo(old_meta_encryption_path, metaEncryptionPath());
    try
    {
        formal_dir.renameTo(folderPath());
    }
    catch (Poco::Exception & e)
    {
        throw DB::Exception(e); // wrap Poco::Exception as DB::Exception for better stack backtrace
    }
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
    file_provider->deleteEncryptionInfo(old_data_encryption_path);
    // remove the data part
    return removeDataIfExists();
}

size_t PageFile::setCheckpoint()
{
    if (type != Type::Temp)
        return 0;

    {
        // The data part of checkpoint file should be empty.
        const auto data_size = getDataFileSize();
        if (data_size != 0)
            throw Exception("Setting " + toString() + " to checkpoint, but data size is not zero: " + DB::toString(data_size)
                                + ", path: " + folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    auto old_meta_encryption_path = metaEncryptionPath();
    Poco::File file(folderPath());
    type = Type::Checkpoint;
    file_provider->linkEncryptionInfo(old_meta_encryption_path, metaEncryptionPath());
    try
    {
        file.renameTo(folderPath());
    }
    catch (Poco::Exception & e)
    {
        throw DB::Exception(e); // wrap Poco::Exception as DB::Exception for better stack backtrace
    }
    file_provider->deleteEncryptionInfo(old_meta_encryption_path);
    // Remove the data part, should be an emtpy file.
    return removeDataIfExists();
}

bool PageFile::linkFrom(PageFile & page_file, WriteBatch::SequenceID sid, PageEntriesEdit & edit)
{
    auto writer = this->createWriter(false, false);
    try
    {
        // FIXME : need test it with DataKeyManager
        file_provider->linkEncryptionInfo(page_file.metaEncryptionPath(), metaEncryptionPath());
        file_provider->linkEncryptionInfo(page_file.dataEncryptionPath(), dataEncryptionPath());

        writer->hardlinkFrom(page_file, sid, edit);
        setFileAppendPos(page_file.getMetaFileSize(), page_file.getDataFileSize());

        return true;
    }
    catch (DB::Exception & e)
    {
        LOG_WARNING(page_file.log, "failed to link page file. error message : {}", e.message());
    }
    return false;
}

size_t PageFile::removeDataIfExists() const
{
    size_t bytes_removed = 0;
    if (auto data_file = Poco::File(dataPath()); data_file.exists())
    {
        bytes_removed = data_file.getSize();
        file_provider->deleteRegularFile(dataPath(), dataEncryptionPath());
    }
    return bytes_removed;
}

void PageFile::destroy() const
{
    Poco::File file(folderPath());
    if (file.exists())
    {
        // remove meta first, then remove data
        file_provider->deleteRegularFile(metaPath(), metaEncryptionPath());
        file_provider->deleteRegularFile(dataPath(), dataEncryptionPath());

        // drop dir
        file.remove(true);
    }
}

bool PageFile::isExist() const
{
    Poco::File folder(folderPath());
    Poco::File data_file(dataPath());
    Poco::File meta_file(metaPath());
    if (likely(type == Type::Formal))
    {
        fiu_do_on(FailPoints::force_formal_page_file_not_exists, { return false; });
        return (folder.exists() && data_file.exists() && meta_file.exists());
    }
    else if (type == Type::Legacy || type == Type::Checkpoint)
    {
        fiu_do_on(FailPoints::force_legacy_or_checkpoint_page_file_exists, { return true; });
        return folder.exists() && meta_file.exists();
    }
    else
        throw Exception("Should not call isExist for " + toString());
}

UInt64 PageFile::getDiskSize() const
{
    return getDataFileSize() + getMetaFileSize();
}

UInt64 PageFile::getDataFileSize() const
{
    if (type != Type::Formal)
        return 0;
    Poco::File file(dataPath());
    return file.getSize();
}

UInt64 PageFile::getMetaFileSize() const
{
    Poco::File file(metaPath());
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

void removePageFilesIf(PageFileSet & page_files, const std::function<bool(const PageFile &)> & pred)
{
    for (auto itr = page_files.begin(); itr != page_files.end(); /* empty */)
    {
        if (pred(*itr))
        {
            itr = page_files.erase(itr);
        }
        else
        {
            itr++;
        }
    }
}

} // namespace PS::V2
} // namespace DB
