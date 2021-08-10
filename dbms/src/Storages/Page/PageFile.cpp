#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageFileSpec.h>
#include <Storages/Page/PageFileSpecV1.h>
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

// =========================================================
// PageFile::MetaMergingReader
// =========================================================

PageFile::MetaMergingReader::MetaMergingReader(PageFile & page_file_) : page_file(page_file_) {}

PageFile::MetaMergingReader::~MetaMergingReader()
{
    page_file.free(meta_buffer, meta_size);
}

PageFile::MetaMergingReaderPtr
PageFile::MetaMergingReader::createFrom(PageFile & page_file, size_t max_meta_offset, const ReadLimiterPtr & read_limiter)
{
    auto reader = std::make_shared<PageFile::MetaMergingReader>(page_file);
    reader->initialize(max_meta_offset, read_limiter);
    return reader;
}

PageFile::MetaMergingReaderPtr PageFile::MetaMergingReader::createFrom(PageFile & page_file, const ReadLimiterPtr & read_limiter)
{
    auto reader = std::make_shared<PageFile::MetaMergingReader>(page_file);
    reader->initialize(std::nullopt, read_limiter);
    return reader;
}

// Try to initiallize access to meta, read the whole metadata to memory.
// Status -> Finished if metadata size is zero.
//        -> Opened if metadata successfully load from disk.
void PageFile::MetaMergingReader::initialize(std::optional<size_t> max_meta_offset, const ReadLimiterPtr & read_limiter)
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
    meta_buffer = (char *)page_file.alloc(meta_size);
    PageUtil::readFile(underlying_file, 0, meta_buffer, meta_size, read_limiter);
    status = Status::Opened;
}

void PageFile::FiledOffsetReader::deserialize(struct ReadContext & ctx, PageEntry & entry, size_t size)
{
    entry.field_offsets.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        auto _field_offset  = PageUtil::cast<PFWriteBatchFieldOffset>(ctx.meta_pos);
        auto field_offset   = _field_offset->bits.field_offset;
        auto field_checksum = _field_offset->bits.field_checksum;
        entry.field_offsets.emplace_back(field_offset, field_checksum);
    }
}

size_t PageFile::WriteBatchReader::readPUEdit(struct ReadContext & ctx)
{
    size_t    wb_pu_offset = 0;
    PageEntry entry;

    WriteBatch::WriteType type;
    UInt64                page_id   = 0;
    UInt8                 page_flag = 0;
    switch (ctx.version)
    {
    case PageFormat::V1: {
        auto wb_pu    = PageUtil::cast<PFWriteBatchTypePUV1>(ctx.meta_pos);
        type          = static_cast<WriteBatch::WriteType>(wb_pu->bits.type);
        page_id       = wb_pu->bits.page_id;
        entry.file_id = ctx.page_file.getFileId();
        entry.level   = ctx.page_file.getLevel();

        entry.tag      = wb_pu->bits.tag;
        entry.offset   = wb_pu->bits.page_offset;
        entry.size     = wb_pu->bits.page_size;
        entry.checksum = wb_pu->bits.page_checksum;

        break;
    }
    case PageFormat::V2: {
        auto wb_pu    = PageUtil::cast<PFWriteBatchTypePU>(ctx.meta_pos);
        type          = static_cast<WriteBatch::WriteType>(wb_pu->bits.type);
        page_id       = wb_pu->bits.page_id;
        entry.file_id = wb_pu->bits.file_id;
        entry.level   = wb_pu->bits.level;

        entry.tag      = wb_pu->bits.tag;
        entry.offset   = wb_pu->bits.page_offset;
        entry.size     = wb_pu->bits.page_size;
        entry.checksum = wb_pu->bits.page_checksum;
        page_flag      = wb_pu->bits.flag;
        fo_reader.deserialize(ctx, entry, wb_pu->bits.field_offset_len);

        break;
    }
    default:
        throw Exception("PageFile binary version not match [unknown_version=" + DB::toString(ctx.version)
                            + "] [file=" + ctx.page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    if (type == WriteBatch::WriteType::PUT)
    {
        ctx.edit.put(page_id, entry);
        wb_pu_offset += entry.size;
    }
    else if (type == WriteBatch::WriteType::UPSERT)
    {
        ctx.edit.upsertPage(page_id, entry);
        // If it is a deatch page, don't move data offset.
        wb_pu_offset += ctx.version == PageFormat::V1 ? entry.size : page_flag & PAGE_FLAG_DETACH ? 0 : entry.size;
    }
    else
    {
        // If type is not PUT/UPSERT, Do not call this method.
        throw Exception("PageFile binary type not match [unknown_type=" + DB::toString((UInt8)type) + "] [file=" + ctx.page_file.metaPath()
                            + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    return wb_pu_offset;
}

void PageFile::WriteBatchReader::readDelEdit(struct ReadContext & ctx)
{
    auto wb_del = PageUtil::cast<PFWriteBatchTypeDel>(ctx.meta_pos);
    ctx.edit.del(wb_del->bits.page_id);
}

void PageFile::WriteBatchReader::readRefEdit(struct ReadContext & ctx)
{
    auto wb_ref = PageUtil::cast<PFWriteBatchTypeRef>(ctx.meta_pos);
    ctx.edit.ref(wb_ref->bits.page_id, wb_ref->bits.og_page_id);
}

int PageFile::WriteBatchReader::deserialize(struct ReadContext & ctx)
{
    size_t     wb_data_offset = 0;
    const auto write_type     = static_cast<WriteBatch::WriteType>(PageUtil::get<PFWriteBatchType, false>(ctx.meta_pos));
    switch (write_type)
    {
    case WriteBatch::WriteType::PUT:
    case WriteBatch::WriteType::UPSERT: {
        wb_data_offset += readPUEdit(ctx);
        break;
    }
    case WriteBatch::WriteType::DEL: {
        readDelEdit(ctx);
        break;
    }
    case WriteBatch::WriteType::REF: {
        readRefEdit(ctx);
        break;
    }
    }

    return wb_data_offset;
}

size_t PageFile::MetaMergingReader::deserialize(ReadContext & ctx, WriteBatch::SequenceID * sid, PageFormat::Version * v)
{
    char *       meta_start_pos = ctx.meta_pos;
    PFMetaInfo * meta_info      = PageUtil::cast<PFMetaInfo>(ctx.meta_pos);
    size_t       meta_bytes     = meta_info->bits.meta_byte_size;

    // verify meta length
    if (meta_start_pos + meta_bytes > meta_buffer + meta_size)
    {
        LOG_WARNING(page_file.log,
                    "Incomplete write batch {" << toString() << "} [expect_batch_bytes=" << meta_bytes << "] [meta_size=" << meta_size
                                               << "] [file=" << page_file.metaPath() << "] ignored.");
        status = Status::Finished;
        return 0;
    }

    // get sequence id
    WriteBatch::SequenceID wb_sequence = 0;
    switch (meta_info->bits.meta_info_version)
    {
    case PageFormat::V1:
        wb_sequence = 0;
        break;
    case PageFormat::V2:
        wb_sequence = meta_info->bits.meta_seq_id;
        break;
    default:
        throw Exception("PageFile binary version not match {" + toString() + "} [unknown_version="
                            + DB::toString(meta_info->bits.meta_info_version) + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }
    *sid = wb_sequence;
    if (unlikely(v != nullptr))
        *v = meta_info->bits.meta_info_version;


    // check checksum
    const auto meta_bytes_without_checksum = -sizeof(PFMetaInfoChecksum);
    const auto meta_checksum               = PageUtil::get<PFMetaInfoChecksum, false>(meta_start_pos + meta_bytes_without_checksum);
    const auto checksum_calc               = CityHash_v1_0_2::CityHash64(meta_start_pos, meta_bytes_without_checksum);
    if (meta_checksum != checksum_calc)
    {
        std::stringstream ss;
        ss << "[expecte_checksum=" << std::hex << meta_checksum << "] [actual_checksum" << checksum_calc << "]";
        throw Exception("Write batch checksum not match {" + toString() + "} [path=" + page_file.folderPath()
                            + "] [batch_bytes=" + DB::toString(meta_bytes) + "] " + ss.str(),
                        ErrorCodes::CHECKSUM_DOESNT_MATCH);
    }

    size_t data_offset = 0;
    while (ctx.meta_pos < meta_start_pos + (meta_bytes - sizeof(PFMetaInfoChecksum)))
    {
        data_offset += wb_reader.deserialize(ctx);
    }

    // Skip the meta Checksum, have already verify before parse.
    ctx.meta_pos += sizeof(PFMetaInfoChecksum);

    if (unlikely(ctx.meta_pos != meta_start_pos + meta_bytes))
        throw Exception("pos not match {" + toString() + "} [batch_bytes=" + DB::toString(meta_bytes)
                            + "] [actual_bytes=" + DB::toString(ctx.meta_pos - meta_start_pos) + "] [file=" + page_file.metaPath() + "]",
                        ErrorCodes::LOGICAL_ERROR);

    return data_offset;
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
    char * pos           = meta_buffer + meta_file_offset;

    /// Verify at least remain meta info
    if (pos + sizeof(PFMetaInfo) > meta_data_end)
    {
        LOG_WARNING(page_file.log,
                    "Incomplete write batch {" << toString() << "} [batch_start_pos=" << meta_file_offset << "] [meta_size=" << meta_size
                                               << "] [file=" << page_file.metaPath() << "] ignored.");
        status = Status::Finished;
        return;
    }

    ReadContext context = {.version = 0, .page_file = page_file, .edit = curr_edit, .meta_pos = pos};

    size_t curr_wb_data_offset = deserialize(context, &curr_write_batch_sequence, v);

    meta_file_offset = pos - meta_buffer;
    data_file_offset += curr_wb_data_offset;
}

// =========================================================
// PageFile::Writer
// =========================================================

PageFile::Writer::Writer(PageFile & page_file_, bool sync_on_write_, bool truncate_if_exists)
    : page_file(page_file_), sync_on_write(sync_on_write_), data_file{nullptr}, meta_file{nullptr}, last_write_time(Clock::now())
{
    // Create data and meta file, prevent empty page folder from being removed by GC.
    data_file = page_file.file_provider->newWritableFile(
        page_file.dataPath(), page_file.dataEncryptionPath(), truncate_if_exists, /*create_new_encryption_info_*/ truncate_if_exists);
    meta_file = page_file.file_provider->newWritableFile(
        page_file.metaPath(), page_file.metaEncryptionPath(), truncate_if_exists, /*create_new_encryption_info_*/ truncate_if_exists);
    data_file->close();
    meta_file->close();
}

PageFile::Writer::~Writer()
{
    if (data_file->isClosed())
        return;

    closeFd();
}

const String & PageFile::Writer::parentPath() const
{
    return page_file.parent_path;
}

void PageFile::FiledOffsetWriter::serialize(struct WriteContext & ctx, PageEntry & entry)
{
    for (size_t i = 0; i < entry.field_offsets.size(); ++i)
    {
        PFWriteBatchFieldOffset * field_offset = PageUtil::cast<PFWriteBatchFieldOffset>(ctx.meta_pos);
        field_offset->bits.field_offset        = entry.field_offsets[i].first;
        field_offset->bits.field_checksum      = entry.field_offsets[i].second;
    }
}

void PageFile::WriteBatchWriter::applyPUEdit(struct WriteContext & ctx,
                                             WriteBatch::Write &   writer,
                                             UInt64 &              page_data_file_off,
                                             PageEntry &           entry)
{
    WriteBatchChecksum   page_checksum = 0;
    WriteBatchPageOffset page_offset   = 0;
    if (writer.read_buffer)
    {
        writer.read_buffer->readStrict(ctx.data_pos, writer.size);
        page_checksum = CityHash_v1_0_2::CityHash64(ctx.data_pos, writer.size);
        page_offset   = page_data_file_off;
        // In this case, checksum of each fields (inside `write.offsets[i].second`)
        // is simply 0, we need to calulate the checksums of each fields
        for (size_t i = 0; i < writer.offsets.size(); ++i)
        {
            const auto field_beg     = writer.offsets[i].first;
            const auto field_end     = (i == writer.offsets.size() - 1) ? writer.size : writer.offsets[i + 1].first;
            writer.offsets[i].second = CityHash_v1_0_2::CityHash64(ctx.data_pos + field_beg, field_end - field_beg);
        }

        ctx.data_pos += writer.size;
        page_data_file_off += writer.size;
    }
    else
    {
        // Notice: in this case, we need to copy checksum instead of calculate from buffer(which is null)
        // Do NOT use `data_pos` outside this if-else branch

        // get page_checksum from write when read_buffer is nullptr
        page_checksum = writer.page_checksum;
        page_offset   = writer.page_offset;
        // `entry.field_offsets`(and checksum) just simply copy `write.offsets`
        // page_data_file_off += 0;
    }

    entry.file_id  = (writer.type == WriteBatch::WriteType::PUT ? ctx.page_file->getFileId() : writer.target_file_id.first);
    entry.level    = (writer.type == WriteBatch::WriteType::PUT ? ctx.page_file->getLevel() : writer.target_file_id.second);
    entry.tag      = writer.tag;
    entry.size     = writer.size;
    entry.offset   = page_offset;
    entry.checksum = page_checksum;

    // entry.field_offsets = write.offsets;
    // we can swap from WriteBatch instead of copying
    entry.field_offsets.swap(writer.offsets);

    if (writer.type == WriteBatch::WriteType::PUT)
        ctx.edit->put(writer.page_id, entry);
    else if (writer.type == WriteBatch::WriteType::UPSERT)
        ctx.edit->upsertPage(writer.page_id, entry);
}

void PageFile::WriteBatchWriter::applyDelEdit(struct WriteContext & ctx, WriteBatch::Write & writer)
{
    ctx.edit->del(writer.page_id);
}

void PageFile::WriteBatchWriter::applyRefEdit(struct WriteContext & ctx, WriteBatch::Write & writer)
{
    ctx.edit->ref(writer.page_id, writer.ori_page_id);
}


void PageFile::WriteBatchWriter::serialize(struct WriteContext & ctx, WriteBatch::Write & writer, UInt64 & page_data_file_off)
{
    switch (writer.type)
    {
    case WriteBatch::WriteType::PUT:
    case WriteBatch::WriteType::UPSERT: {
        PageEntry entry = {};

        applyPUEdit(ctx, writer, page_data_file_off, entry);
        PFWriteBatchTypePU * wb_pu   = PageUtil::cast<PFWriteBatchTypePU>(ctx.meta_pos);
        wb_pu->bits.type             = (UInt8)writer.type;
        wb_pu->bits.page_id          = writer.page_id;
        wb_pu->bits.file_id          = entry.file_id;
        wb_pu->bits.level            = entry.level;
        wb_pu->bits.flag             = !writer.read_buffer ? PAGE_FLAG_DETACH : PAGE_FLAG_NORMAL;
        wb_pu->bits.tag              = writer.tag;
        wb_pu->bits.page_offset      = entry.offset;
        wb_pu->bits.page_size        = writer.size;
        wb_pu->bits.page_checksum    = entry.checksum;
        wb_pu->bits.field_offset_len = (UInt64)entry.field_offsets.size();
        fo_writer.serialize(ctx, entry);
        break;
    }

    case WriteBatch::WriteType::DEL: {
        PFWriteBatchTypeDel * wb_del = PageUtil::cast<PFWriteBatchTypeDel>(ctx.meta_pos);
        wb_del->bits.type            = (UInt8)writer.type;
        wb_del->bits.page_id         = writer.page_id;
        applyDelEdit(ctx, writer);
        break;
    }

    case WriteBatch::WriteType::REF: {
        PFWriteBatchTypeRef * wb_ref = PageUtil::cast<PFWriteBatchTypeRef>(ctx.meta_pos);
        wb_ref->bits.page_id         = writer.page_id;
        wb_ref->bits.og_page_id      = writer.ori_page_id;
        applyRefEdit(ctx, writer);
        break;
    }
    }
}

std::pair<size_t, size_t> PageFile::WriteBatchWriter::measureWriteBatchsSize(WriteBatch & wb)
{
    size_t meta_bytes = 0;
    size_t data_bytes = 0;

    for (const auto & write : wb.getWrites())
    {
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT: {
            if (write.read_buffer)
                data_bytes += write.size;
            meta_bytes += sizeof(PFWriteBatchTypePU);
            meta_bytes += (sizeof(PFWriteBatchFieldOffset) * write.offsets.size());
            break;
        }
        case WriteBatch::WriteType::DEL: {
            // For delete page, store page id only. And don't need to write data file.
            meta_bytes += sizeof(PFWriteBatchTypeDel);
            break;
        }
        case WriteBatch::WriteType::REF: {
            // For ref page, store RefPageId -> PageId. And don't need to write data file.
            meta_bytes += sizeof(PFWriteBatchTypeRef);
            break;
        }
        }
    }

    return {meta_bytes, data_bytes};
}

std::pair<ByteBuffer, ByteBuffer> PageFile::Writer::genWriteData(WriteBatch & wb, PageEntriesEdit & edit)
{
    UInt32 meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    // Get write batchs size
    std::tie(meta_write_bytes, data_write_bytes) = wb_writer.measureWriteBatchsSize(wb);

    // Checksum At the bottom of the meta.
    meta_write_bytes += sizeof(PFMetaInfo) + sizeof(PFMetaInfoChecksum);

    char * meta_buffer = (char *)page_file.alloc(meta_write_bytes);
    char * data_buffer = (char *)page_file.alloc(data_write_bytes);

    WriteContext write_ctx = {&wb, &page_file, &edit, data_buffer, meta_buffer};

    PFMetaInfo * meta_info            = PageUtil::cast<PFMetaInfo>(write_ctx.meta_pos);
    meta_info->bits.meta_byte_size    = meta_write_bytes;
    meta_info->bits.meta_info_version = STORAGE_FORMAT_CURRENT.page;
    meta_info->bits.meta_seq_id       = wb.getSequence();

    // Serialize the write batchs
    {
        UInt64 page_data_file_off = write_ctx.page_file->getDataFileAppendPos();
        for (auto & writer : write_ctx.wb->getWrites())
        {
            wb_writer.serialize(write_ctx, writer, page_data_file_off);
        }
    }

    // At last add Checksum.
    const PFMetaInfoChecksum wb_checksum = CityHash_v1_0_2::CityHash64(meta_buffer, meta_write_bytes - sizeof(PFMetaInfoChecksum));
    PageUtil::put(write_ctx.meta_pos, wb_checksum);

    // Check and throw
    if (unlikely(write_ctx.meta_pos != meta_buffer + meta_write_bytes || write_ctx.data_pos != data_buffer + data_write_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return {{meta_buffer, write_ctx.meta_pos}, {data_buffer, write_ctx.data_pos}};
}

void PageFile::Writer::serialize(ByteBuffer & meta_buf, ByteBuffer & data_buf, const WriteLimiterPtr & write_limiter)
{

#ifndef NDEBUG
    auto write_buf = [&](WritableFilePtr & file, UInt64 offset, ByteBuffer buf, bool enable_failpoint) {
        PageUtil::writeFile(file, offset, buf.begin(), buf.size(), write_limiter, enable_failpoint);
        if (sync_on_write)
            PageUtil::syncFile(file);
    };
    write_buf(data_file, page_file.data_file_pos, data_buf, false);
    write_buf(meta_file, page_file.meta_file_pos, meta_buf, true);
#else
    auto write_buf = [&](WritableFilePtr & file, UInt64 offset, ByteBuffer buf) {
        PageUtil::writeFile(file, offset, buf.begin(), buf.size(), write_limiter);
        if (sync_on_write)
            PageUtil::syncFile(file);
    };
    write_buf(data_file, page_file.data_file_pos, data_buf);
    write_buf(meta_file, page_file.meta_file_pos, meta_buf);
#endif
}

size_t PageFile::Writer::write(WriteBatch & wb, PageEntriesEdit & edit, const WriteLimiterPtr & write_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    if (data_file->isClosed())
    {
        data_file->open();
        meta_file->open();
    }

    // TODO: investigate if not copy data into heap, write big pages can be faster?
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = genWriteData(wb, edit);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    serialize(meta_buf, data_buf, write_limiter);

    fiu_do_on(FailPoints::exception_before_page_file_write_sync,
              { // Mock that writing page file meta is not completed
                  auto f    = Poco::File(meta_file->getFileName());
                  auto size = f.getSize();
                  f.setSize(size - 2);
                  auto size_after = f.getSize();
                  LOG_WARNING(page_file.log,
                              "Failpoint truncate [file=" << meta_file->getFileName() << "] [origin_size=" << size
                                                          << "] [truncated_size=" << size_after << "]");
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
    : data_file_path(page_file.dataPath()),
      data_file{page_file.file_provider->newRandomAccessFile(page_file.dataPath(), page_file.dataEncryptionPath())},
      last_read_time(Clock::now())
{
}

PageFile::Reader::~Reader()
{
    data_file->close();
}

PageMap PageFile::Reader::read(PageIdAndEntries & to_read, const ReadLimiterPtr & read_limiter)
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
        PageUtil::readFile(data_file, entry.offset, pos, entry.size, read_limiter);

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

    last_read_time = Clock::now();

    return page_map;
}

void PageFile::Reader::read(PageIdAndEntries & to_read, const PageHandler & handler, const ReadLimiterPtr & read_limiter)
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

        PageUtil::readFile(data_file, entry.offset, data_buf, entry.size, read_limiter);

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

    last_read_time = Clock::now();
}

PageMap PageFile::Reader::read(PageFile::Reader::FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter)
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
            PageUtil::readFile(data_file, entry.offset + beg_offset, write_offset, size_to_read, read_limiter);
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

    last_read_time = Clock::now();

    return page_map;
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

PageFile::PageFile(PageFileId              file_id_,
                   UInt32                  level_,
                   const std::string &     parent_path,
                   const FileProviderPtr & file_provider_,
                   PageFile::Type          type_,
                   bool                    is_create,
                   Logger *                log_)
    : file_id(file_id_),
      level(level_),
      type(type_),
      parent_path(parent_path),
      file_provider{file_provider_},
      data_file_pos(0),
      meta_file_pos(0),
      log(log_)
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
PageFile::recover(const String & parent_path, const FileProviderPtr & file_provider_, const String & page_file_name, Logger * log)
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
    PageFile   pf(file_id, level, parent_path, file_provider_, Type::Formal, /* is_create */ false, log);
    if (ss[0] == folder_prefix_temp)
    {
        LOG_INFO(log, "Temporary page file, ignored: " + page_file_name);
        pf.type = Type::Temp;
        return {pf, Type::Temp};
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

PageFile PageFile::newPageFile(PageFileId              file_id,
                               UInt32                  level,
                               const std::string &     parent_path,
                               const FileProviderPtr & file_provider_,
                               PageFile::Type          type,
                               Logger *                log)
{
#ifndef NDEBUG
    // PageStorage may create a "Formal" PageFile for writing,
    // or a "Temp" PageFile for gc data.
    if (type != PageFile::Type::Temp && type != PageFile::Type::Formal)
        throw Exception("Should not create page file with type: " + typeToString(type));
#endif
    return PageFile(file_id, level, parent_path, file_provider_, type, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId              file_id,
                                       UInt32                  level,
                                       const std::string &     parent_path,
                                       const FileProviderPtr & file_provider_,
                                       PageFile::Type          type,
                                       Logger *                log)
{
    return PageFile(file_id, level, parent_path, file_provider_, type, false, log);
}

bool PageFile::isPageFileExist(
    PageFileIdAndLevel file_id, const String & parent_path, const FileProviderPtr & file_provider_, Type type, Poco::Logger * log)
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
                    "Truncate incomplete write batches"
                    " [orig_size="
                        << meta_size_on_disk << "] [set_size=" << meta_file_pos << "] [file=" << metaPath() << "]");
        meta_on_disk.setSize(meta_file_pos);
    }
}

void PageFile::setFormal()
{
    if (type != Type::Temp)
        return;
    auto       old_meta_encryption_path = metaEncryptionPath();
    auto       old_data_encryption_path = dataEncryptionPath();
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
    auto       old_meta_encryption_path = metaEncryptionPath();
    auto       old_data_encryption_path = dataEncryptionPath();
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

    auto       old_meta_encryption_path = metaEncryptionPath();
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

} // namespace DB
