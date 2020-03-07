#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/PageUtil.h>

#include <ext/scope_guard.h>

namespace DB
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
using WBSize          = UInt32;
using PageFileVersion = PageFile::Version;
// TODO we should align these alias with type in PageCache
using PageTag    = UInt64;
using IsPut      = std::underlying_type<WriteBatch::WriteType>::type;
using PageOffset = UInt64;
using Checksum   = UInt64;

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
    WriteBatch &      wb,
    PageFile &        page_file,
    PageEntriesEdit & edit)
{
    WBSize meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    meta_write_bytes += sizeof(WBSize) + sizeof(PageFileVersion) + sizeof(WriteBatch::SequenceID);

    for (const auto & write : wb.getWrites())
    {
        meta_write_bytes += sizeof(IsPut);
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
            if (write.read_buffer)
                data_write_bytes += write.size;
            meta_write_bytes += PAGE_META_SIZE;
            meta_write_bytes += sizeof(UInt64); // size of field_offsets + checksum
            meta_write_bytes += ((sizeof(UInt64) + sizeof(UInt64)) * write.offsets.size());
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

    char * meta_buffer = (char *)page_file.alloc(meta_write_bytes);
    char * data_buffer = (char *)page_file.alloc(data_write_bytes);

    char * meta_pos = meta_buffer;
    char * data_pos = data_buffer;

    PageUtil::put(meta_pos, meta_write_bytes);
    PageUtil::put(meta_pos, PageFile::CURRENT_VERSION);
    PageUtil::put(meta_pos, wb.getSequence());

    PageOffset page_data_file_off = page_file.getDataFileAppendPos();
    for (auto & write : wb.getWrites())
    {
        PageUtil::put(meta_pos, static_cast<IsPut>(write.type));
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT: {
            PageFlags  flags;
            Checksum   page_checksum = 0;
            PageOffset page_offset   = 0;
            if (write.read_buffer)
            {
                write.read_buffer->readStrict(data_pos, write.size);
                page_checksum = CityHash_v1_0_2::CityHash64(data_pos, write.size);
                page_offset   = page_data_file_off;
                // In this case, checksum of each fields (inside `write.offsets[i].second`)
                // is simply 0, we need to calulate the checksums of each fields
                for (size_t i = 0; i < write.offsets.size(); ++i)
                {
                    const auto field_beg    = write.offsets[i].first;
                    const auto field_end    = (i == write.offsets.size() - 1) ? write.size : write.offsets[i + 1].first;
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
                page_offset   = write.page_offset;
                // `entry.field_offsets`(and checksum) just simply copy `write.offsets`
                // page_data_file_off += 0;
            }

            // UPSERT may point to another PageFile
            PageEntry entry;
            entry.file_id  = (write.type == WriteBatch::WriteType::PUT ? page_file.getFileId() : write.target_file_id.first);
            entry.level    = (write.type == WriteBatch::WriteType::PUT ? page_file.getLevel() : write.target_file_id.second);
            entry.tag      = write.tag;
            entry.size     = write.size;
            entry.offset   = page_offset;
            entry.checksum = page_checksum;

            // entry.field_offsets = write.offsets;
            // we can swap from WriteBatch instead of copying
            entry.field_offsets.swap(write.offsets);

            PageUtil::put(meta_pos, (PageId)write.page_id);
            PageUtil::put(meta_pos, (PageFileId)entry.file_id);
            PageUtil::put(meta_pos, (PageFileLevel)entry.level);
            PageUtil::put(meta_pos, (PageFlags)flags);
            PageUtil::put(meta_pos, (PageTag)write.tag);
            PageUtil::put(meta_pos, (PageOffset)entry.offset);
            PageUtil::put(meta_pos, (PageSize)write.size);
            PageUtil::put(meta_pos, (Checksum)page_checksum);

            PageUtil::put(meta_pos, (UInt64)entry.field_offsets.size());
            for (size_t i = 0; i < entry.field_offsets.size(); ++i)
            {
                PageUtil::put(meta_pos, (UInt64)entry.field_offsets[i].first);
                PageUtil::put(meta_pos, (UInt64)entry.field_offsets[i].second);
            }

            if (write.type == WriteBatch::WriteType::PUT)
                edit.put(write.page_id, entry);
            else if (write.type == WriteBatch::WriteType::UPSERT)
                edit.upsertPage(write.page_id, entry);

            break;
        }
        case WriteBatch::WriteType::DEL:
            PageUtil::put(meta_pos, (PageId)write.page_id);

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
/// TODO: this is somehow duplicated with `PageFile::MetaMergingReader::moveNext`, we should find a better way.
std::pair<UInt64, UInt64> analyzeMetaFile( //
    const String &           path,
    PageFileId               file_id,
    UInt32                   level,
    const char *             meta_data,
    const size_t             meta_data_size,
    PageEntriesEdit &        edit,
    WriteBatch::SequenceID & max_wb_sequence,
    Logger *                 log)
{
    const char * meta_data_end = meta_data + meta_data_size;

    UInt64 page_data_file_size = 0;
    char * pos                 = const_cast<char *>(meta_data);
    while (pos < meta_data_end)
    {
        if (pos + sizeof(WBSize) > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }
        const char * wb_start_pos = pos;
        const auto   wb_bytes     = PageUtil::get<WBSize>(pos);
        if (wb_start_pos + wb_bytes > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }

        WriteBatch::SequenceID wb_sequence    = 0;
        const auto             binary_version = PageUtil::get<PageFileVersion>(pos);
        if (binary_version == PageFile::VERSION_BASE)
        {
            wb_sequence = 0;
        }
        else if (binary_version == PageFile::VERSION_FLASH_341)
        {
            wb_sequence = PageUtil::get<WriteBatch::SequenceID>(pos);
        }
        else
        {
            throw Exception("Binary version not match, version: " + DB::toString(binary_version) + ", path: " + path,
                            ErrorCodes::LOGICAL_ERROR);
        }
        max_wb_sequence = std::max(max_wb_sequence, wb_sequence);

        // check the checksum of WriteBatch
        const auto wb_bytes_without_checksum = wb_bytes - sizeof(Checksum);
        const auto wb_checksum               = PageUtil::get<Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
        const auto checksum_calc             = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
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
            const auto is_put     = PageUtil::get<IsPut>(pos);
            const auto write_type = static_cast<WriteBatch::WriteType>(is_put);
            switch (write_type)
            {
            case WriteBatch::WriteType::PUT:
            case WriteBatch::WriteType::UPSERT: {
                PageFlags flags;
                auto      page_id = PageUtil::get<PageId>(pos);
                PageEntry entry;
                if (binary_version == PageFile::VERSION_BASE)
                {
                    entry.file_id = file_id;
                    entry.level   = level;
                }
                else if (binary_version == PageFile::VERSION_FLASH_341)
                {
                    entry.file_id = PageUtil::get<PageFileId>(pos);
                    entry.level   = PageUtil::get<PageFileLevel>(pos);
                    flags         = PageUtil::get<PageFlags>(pos);
                }
                entry.tag      = PageUtil::get<PageTag>(pos);
                entry.offset   = PageUtil::get<PageOffset>(pos);
                entry.size     = PageUtil::get<PageSize>(pos);
                entry.checksum = PageUtil::get<Checksum>(pos);

                if (binary_version == PageFile::VERSION_FLASH_341)
                {
                    const UInt64 num_fields = PageUtil::get<UInt64>(pos);
                    entry.field_offsets.reserve(num_fields);
                    for (size_t i = 0; i < num_fields; ++i)
                    {
                        auto field_offset   = PageUtil::get<UInt64>(pos);
                        auto field_checksum = PageUtil::get<UInt64>(pos);
                        entry.field_offsets.emplace_back(field_offset, field_checksum);
                    }
                }

                if (write_type == WriteBatch::WriteType::PUT)
                {
                    edit.put(page_id, entry);
                    page_data_file_size += entry.size;
                }
                else if (write_type == WriteBatch::WriteType::UPSERT)
                {
                    edit.upsertPage(page_id, entry);
                    // If it is a deatch page, don't move data offset.
                    page_data_file_size += flags.isDetachPage() ? 0 : entry.size;
                }
                break;
            }
            case WriteBatch::WriteType::DEL: {
                auto page_id = PageUtil::get<PageId>(pos);
                edit.del(page_id); // Reserve the order of removal.
                break;
            }
            case WriteBatch::WriteType::REF: {
                const auto ref_id  = PageUtil::get<PageId>(pos);
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

PageFile::MetaMergingReader::~MetaMergingReader()
{
    page_file.free(meta_buffer, meta_size);
}

void PageFile::MetaMergingReader::initialize()
{
    if (status == Status::Opened)
        return;
    else if (status == Status::Finished)
        throw Exception("Try to reopen finished merging reader: " + toString(), ErrorCodes::LOGICAL_ERROR);

    const auto path = page_file.metaPath();
    Poco::File file(path);
    if (unlikely(!file.exists()))
        throw Exception("Try to read meta of " + page_file.toString() + ", but not exists. Path: " + path, ErrorCodes::LOGICAL_ERROR);

    meta_size = file.getSize();
    if (meta_size == 0) // Empty file
    {
        status = Status::Finished;
        return;
    }

    const int fd = PageUtil::openFile<true, false>(path);
    // File not exists.
    if (unlikely(!fd))
        throw Exception("Try to read meta of " + page_file.toString() + ", but open file error. Path: " + path, ErrorCodes::LOGICAL_ERROR);
    SCOPE_EXIT({ ::close(fd); });

    meta_buffer = (char *)page_file.alloc(meta_size);
    PageUtil::readFile(fd, 0, meta_buffer, meta_size, path);
    status = Status::Opened;
}

bool PageFile::MetaMergingReader::hasNext() const
{
    return (status == Status::Uninitialized) || (status == Status::Opened && meta_file_offset < meta_size);
}

/// TODO: this is somehow duplicated with `analyzeMetaFile`, we should find a better way.
void PageFile::MetaMergingReader::moveNext()
{
    curr_edit.clear();
    curr_write_batch_sequence = 0;

    if (status == Status::Uninitialized)
        initialize();
    // Note that we need to check if status is finished after initialize.
    if (status == Status::Finished)
        return;

    char * meta_data_end = meta_buffer + meta_size;
    char * pos           = meta_buffer + meta_file_offset;
    if (pos + sizeof(PageMetaFormat::WBSize) > meta_data_end)
    {
        LOG_WARNING(page_file.log, "Incomplete write batch @" + toString() + ", ignored.");
        status = Status::Finished;
        return;
    }
    const char * wb_start_pos = pos;
    const auto   wb_bytes     = PageUtil::get<PageMetaFormat::WBSize>(pos);
    if (wb_start_pos + wb_bytes > meta_data_end)
    {
        LOG_WARNING(page_file.log, "Incomplete write batch @" + toString() + ", ignored.");
        status = Status::Finished;
        return;
    }

    WriteBatch::SequenceID wb_sequence    = 0;
    const auto             binary_version = PageUtil::get<PageMetaFormat::PageFileVersion>(pos);
    if (binary_version == 1)
    {
        wb_sequence = 0;
    }
    else if (binary_version == PageFile::VERSION_FLASH_341)
    {
        wb_sequence = PageUtil::get<WriteBatch::SequenceID>(pos);
    }
    else
    {
        throw Exception("PageFile binary version not match, unknown version: " + DB::toString(binary_version), ErrorCodes::LOGICAL_ERROR);
    }

    // check the checksum of WriteBatch
    const auto wb_bytes_without_checksum = wb_bytes - sizeof(PageMetaFormat::Checksum);
    const auto wb_checksum               = PageUtil::get<PageMetaFormat::Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
    const auto checksum_calc             = CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum);
    if (wb_checksum != checksum_calc)
    {
        std::stringstream ss;
        ss << "expected: " << std::hex << wb_checksum << ", but: " << checksum_calc;
        throw Exception("Write batch checksum not match, path: " + page_file.folderPath() + ", offset: "
                            + DB::toString(wb_start_pos - meta_buffer) + ", bytes: " + DB::toString(wb_bytes) + ", " + ss.str(),
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    // recover WriteBatch
    size_t curr_wb_data_offset = 0;
    while (pos < wb_start_pos + wb_bytes_without_checksum)
    {
        const auto write_type = static_cast<WriteBatch::WriteType>(PageUtil::get<PageMetaFormat::IsPut>(pos));
        switch (write_type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT: {
            PageMetaFormat::PageFlags flags;

            auto      page_id = PageUtil::get<PageId>(pos);
            PageEntry entry;
            if (binary_version == PageFile::VERSION_BASE)
            {
                entry.file_id = page_file.getFileId();
                entry.level   = page_file.getLevel();
            }
            else if (binary_version == PageFile::VERSION_FLASH_341)
            {
                entry.file_id = PageUtil::get<PageFileId>(pos);
                entry.level   = PageUtil::get<PageFileLevel>(pos);
                flags         = PageUtil::get<PageMetaFormat::PageFlags>(pos);
            }
            entry.tag      = PageUtil::get<PageMetaFormat::PageTag>(pos);
            entry.offset   = PageUtil::get<PageMetaFormat::PageOffset>(pos);
            entry.size     = PageUtil::get<PageSize>(pos);
            entry.checksum = PageUtil::get<PageMetaFormat::Checksum>(pos);

            if (binary_version == PageFile::VERSION_FLASH_341)
            {
                const UInt64 num_fields = PageUtil::get<UInt64>(pos);
                entry.field_offsets.reserve(num_fields);
                for (size_t i = 0; i < num_fields; ++i)
                {
                    auto field_offset   = PageUtil::get<UInt64>(pos);
                    auto field_checksum = PageUtil::get<UInt64>(pos);
                    entry.field_offsets.emplace_back(field_offset, field_checksum);
                }
            }

            if (write_type == WriteBatch::WriteType::PUT)
            {
                curr_edit.put(page_id, entry);
                curr_wb_data_offset += entry.size;
            }
            else if (write_type == WriteBatch::WriteType::UPSERT)
            {
                curr_edit.upsertPage(page_id, entry);
                // If it is a deatch page, don't move data offset.
                curr_wb_data_offset += flags.isDetachPage() ? 0 : entry.size;
            }
            break;
        }
        case WriteBatch::WriteType::DEL: {
            auto page_id = PageUtil::get<PageId>(pos);
            curr_edit.del(page_id);
            break;
        }
        case WriteBatch::WriteType::REF: {
            const auto ref_id  = PageUtil::get<PageId>(pos);
            const auto page_id = PageUtil::get<PageId>(pos);
            curr_edit.ref(ref_id, page_id);
            break;
        }
        }
    }
    // move `pos` over the checksum of WriteBatch
    pos += sizeof(PageMetaFormat::Checksum);

    if (unlikely(pos != wb_start_pos + wb_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    curr_write_batch_sequence = wb_sequence;
    meta_file_offset          = pos - meta_buffer;
    data_file_offset += curr_wb_data_offset;
}

// =========================================================
// PageFile::Writer
// =========================================================

PageFile::Writer::Writer(PageFile & page_file_, bool sync_on_write_)
    : page_file(page_file_),
      sync_on_write(sync_on_write_),
      data_file_path(page_file.dataPath()),
      meta_file_path(page_file.metaPath()),
      data_file_fd(PageUtil::openFile<false>(data_file_path)),
      meta_file_fd(PageUtil::openFile<false>(meta_file_path))
{
}

PageFile::Writer::~Writer()
{
    SCOPE_EXIT({
        ::close(data_file_fd);
        ::close(meta_file_fd);
    });
    PageUtil::syncFile(data_file_fd, data_file_path);
    PageUtil::syncFile(meta_file_fd, meta_file_path);
}

void PageFile::Writer::write(WriteBatch & wb, PageEntriesEdit & edit)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    // TODO: investigate if not copy data into heap, write big pages can be faster?
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = PageMetaFormat::genWriteData(wb, page_file, edit);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    auto write_buf = [&](int fd, UInt64 offset, const std::string & path, ByteBuffer buf) {
        PageUtil::writeFile(fd, offset, buf.begin(), buf.size(), path);
        if (sync_on_write)
            PageUtil::syncFile(fd, path);
    };

    write_buf(data_file_fd, page_file.data_file_pos, data_file_path, data_buf);
    write_buf(meta_file_fd, page_file.meta_file_pos, meta_file_path, meta_buf);

    page_file.data_file_pos += data_buf.size();
    page_file.meta_file_pos += meta_buf.size();
}

PageFileIdAndLevel PageFile::Writer::fileIdLevel() const
{
    return page_file.fileIdLevel();
}

// =========================================================
// PageFile::Reader
// =========================================================

PageFile::Reader::Reader(PageFile & page_file)
    : data_file_path(page_file.dataPath()), data_file_fd(PageUtil::openFile<true>(data_file_path))
{
}

PageFile::Reader::~Reader()
{
    ::close(data_file_fd);
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

    char *    data_buf   = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    char *  pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, entry] : to_read)
    {
        PageUtil::readFile(data_file_fd, entry.offset, pos, entry.size, data_file_path);

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
        page.page_id    = page_id;
        page.data       = ByteBuffer(pos, pos + entry.size);
        page.mem_holder = mem_holder;
        page_map.emplace(page_id, page);

        pos += entry.size;
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

    char *    data_buf   = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });


    auto it = to_read.begin();
    while (it != to_read.end())
    {
        auto && [page_id, entry] = *it;

        PageUtil::readFile(data_file_fd, entry.offset, data_buf, entry.size, data_file_path);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(data_buf, entry.size);
            if (unlikely(entry.size != 0 && checksum != entry.checksum))
            {
                std::stringstream ss;
                ss << ", expected: " << std::hex << entry.checksum << ", but: " << checksum;
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path + ss.str(),
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id    = page_id;
        page.data       = ByteBuffer(data_buf, data_buf + entry.size);
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

PageMap PageFile::Reader::read(PageFile::Reader::FieldReadInfos & to_read)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(
        to_read.begin(), to_read.end(), [](const FieldReadInfo & a, const FieldReadInfo & b) { return a.entry.offset < b.entry.offset; });

    // allocate data_buf that can hold all pages with specify fields
    size_t buf_size = 0;
    for (auto & [page_id, entry, fields] : to_read)
    {
        (void)page_id;
        (void)entry;
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

    char *    data_buf   = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    std::set<Page::FieldOffset> fields_offset_in_page;

    char *  pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, entry, fields] : to_read)
    {
        size_t read_size_this_entry = 0;
        char * write_offset         = pos;

        for (const auto field_index : fields)
        {
            // TODO: Continuously fields can read by one system call.
            const auto [beg_offset, end_offset] = entry.getFieldOffsets(field_index);
            const auto size_to_read             = end_offset - beg_offset;
            PageUtil::readFile(data_file_fd, entry.offset + beg_offset, write_offset, size_to_read, data_file_path);
            fields_offset_in_page.emplace(field_index, read_size_this_entry);

            if constexpr (PAGE_CHECKSUM_ON_READ)
            {
                auto expect_checksum = entry.field_offsets[field_index].second;
                auto field_checksum  = CityHash_v1_0_2::CityHash64(write_offset, size_to_read);
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
        page.page_id    = page_id;
        page.data       = ByteBuffer(pos, write_offset);
        page.mem_holder = mem_holder;
        page.field_offsets.swap(fields_offset_in_page);
        fields_offset_in_page.clear();
        page_map.emplace(page_id, std::move(page));

        pos = write_offset;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception("Pos not match, expect to read " + DB::toString(buf_size) + " bytes, but only " + DB::toString(pos - data_buf),
                        ErrorCodes::LOGICAL_ERROR);

    return page_map;
}


// =========================================================
// PageFile
// =========================================================

PageFile::PageFile(PageFileId file_id_, UInt32 level_, const std::string & parent_path, PageFile::Type type_, bool is_create, Logger * log_)
    : file_id(file_id_), level(level_), type(type_), parent_path(parent_path), data_file_pos(0), meta_file_pos(0), log(log_)
{
    if (is_create)
    {
        Poco::File file(folderPath());
        if (file.exists())
            file.remove(true);
        file.createDirectories();
    }
}

std::pair<PageFile, PageFile::Type> PageFile::recover(const String & parent_path, const String & page_file_name, Logger * log)
{

    if (!startsWith(page_file_name, folder_prefix_formal) && !startsWith(page_file_name, folder_prefix_temp)
        && !startsWith(page_file_name, folder_prefix_legacy) && !startsWith(page_file_name, folder_prefix_checkpoint))
    {
        LOG_INFO(log, "Not page file, ignored " + page_file_name);
        return {{}, Type::Invalid};
    }
    std::vector<std::string> ss;
    boost::split(ss, page_file_name, boost::is_any_of("_"));
    if (ss.size() != 3)
    {
        LOG_INFO(log, "Unrecognized file, ignored: " + page_file_name);
        return {{}, Type::Invalid};
    }

    PageFileId file_id = std::stoull(ss[1]);
    UInt32     level   = std::stoi(ss[2]);
    PageFile   pf(file_id, level, parent_path, Type::Formal, /* is_create */ false, log);
    if (ss[0] == folder_prefix_temp)
    {
        LOG_INFO(log, "Temporary page file, ignored: " + page_file_name);
        return {{}, Type::Temp};
    }
    else if (ss[0] == folder_prefix_legacy)
    {
        pf.type = Type::Legacy;
        // ensure meta exist
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: " + pf.metaPath());
            return {{}, Type::Invalid};
        }

        return {pf, Type::Legacy};
    }
    else if (ss[0] == folder_prefix_formal)
    {
        // ensure both meta && data exist
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: " + pf.metaPath());
            return {{}, Type::Invalid};
        }

        if (!Poco::File(pf.dataPath()).exists())
        {
            LOG_INFO(log, "Broken page without data file, ignored: " + pf.dataPath());
            return {{}, Type::Invalid};
        }
        return {pf, Type::Formal};
    }
    else if (ss[0] == folder_prefix_checkpoint)
    {
        pf.type = Type::Checkpoint;
        if (!Poco::File(pf.metaPath()).exists())
        {
            LOG_INFO(log, "Broken page without meta file, ignored: " + pf.metaPath());
            return {{}, Type::Invalid};
        }
        pf.type = Type::Checkpoint;

        return {pf, Type::Checkpoint};
    }

    LOG_INFO(log, "Unrecognized file prefix, ignored: " + page_file_name);
    return {{}, Type::Invalid};
}

PageFile PageFile::newPageFile(PageFileId file_id, UInt32 level, const std::string & parent_path, PageFile::Type type, Logger * log)
{
    return PageFile(file_id, level, parent_path, type, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId file_id, UInt32 level, const std::string & parent_path, PageFile::Type type, Logger * log)
{
    return PageFile(file_id, level, parent_path, type, false, log);
}

bool PageFile::isPageFileExist(PageFileIdAndLevel file_id, const String & parent_path, Type type, Poco::Logger * log)
{
    PageFile pf = openPageFileForRead(file_id.first, file_id.second, parent_path, type, log);
    return pf.isExist();
}

void PageFile::readAndSetPageMetas(PageEntriesEdit & edit, WriteBatch::SequenceID & max_wb_sequence)
{
    const auto path = metaPath();
    Poco::File file(path);
    if (unlikely(!file.exists()))
        throw Exception("Try to read meta of PageFile_" + DB::toString(file_id) + "_" + DB::toString(level)
                            + ", but not exists. Path: " + path,
                        ErrorCodes::LOGICAL_ERROR);

    const size_t file_size = file.getSize();
    const int    file_fd   = PageUtil::openFile<true, false>(path);
    // File not exists.
    if (unlikely(!file_fd))
        return;
    SCOPE_EXIT({ ::close(file_fd); });

    char * meta_data = (char *)alloc(file_size);
    SCOPE_EXIT({ free(meta_data, file_size); });

    PageUtil::readFile(file_fd, 0, meta_data, file_size, path);

    // analyze meta file and update page_entries
    std::tie(this->meta_file_pos, this->data_file_pos)
        = PageMetaFormat::analyzeMetaFile(folderPath(), file_id, level, meta_data, file_size, edit, max_wb_sequence, log);
}

void PageFile::setFormal()
{
    if (type != Type::Temp)
        return;
    Poco::File file(folderPath());
    type = Type::Formal;
    file.renameTo(folderPath());
}

void PageFile::setLegacy()
{
    if (type != Type::Formal)
        return;
    // Rename to legacy dir. Note that we can NOT remove the data part before
    // successfully rename to legacy status.
    Poco::File formal_dir(folderPath());
    type = Type::Legacy;
    formal_dir.renameTo(folderPath());
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

    {
        // The data part of checkpoint file should be empty.
        const auto data_size = getDataFileSize();
        if (data_size != 0)
            throw Exception("Setting " + toString() + " to checkpoint, but data size is not zero: " + DB::toString(data_size)
                                + ", path: " + folderPath(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    Poco::File file(folderPath());
    type = Type::Checkpoint;
    file.renameTo(folderPath());
    // Remove the data part, should be a emtpy file.
    if (auto data_file = Poco::File(dataPath()); data_file.exists())
    {
        data_file.remove();
    }
}

void PageFile::removeDataIfExists() const
{
    if (auto data_file = Poco::File(dataPath()); data_file.exists())
    {
        data_file.remove();
    }
}

void PageFile::destroy() const
{
    // TODO: delay remove.
    Poco::File file(folderPath());
    if (file.exists())
    {
        // remove meta first, then remove data
        Poco::File meta_file(metaPath());
        if (meta_file.exists())
        {
            meta_file.remove();
        }
        Poco::File data_file(dataPath());
        if (data_file.exists())
        {
            data_file.remove();
        }
        // drop dir
        file.remove(true);
    }
}

bool PageFile::isExist() const
{
    Poco::File file(folderPath());
    Poco::File data_file(dataPath());
    Poco::File meta_file(metaPath());
    if (likely(type == Type::Formal))
        return (file.exists() && data_file.exists() && meta_file.exists());
    else if (type == Type::Legacy || type == Type::Checkpoint)
        return file.exists() && meta_file.exists();
    else
        throw Exception("Should not call isExist for " + toString());
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

} // namespace DB
