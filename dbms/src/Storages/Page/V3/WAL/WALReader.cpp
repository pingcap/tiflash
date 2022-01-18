#include <Common/RedactHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Encryption/FileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

namespace DB::PS::V3
{
namespace ser
{
inline void serializeVersionTo(const PageVersionType & version, WriteBuffer & buf)
{
    writeIntBinary(version.sequence, buf);
    writeIntBinary(version.epoch, buf);
}

inline void deserializeVersionFrom(ReadBuffer & buf, PageVersionType & version)
{
    readIntBinary(version.sequence, buf);
    readIntBinary(version.epoch, buf);
}

void serializePutTo(const PageEntriesEdit::EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == WriteBatch::WriteType::PUT || record.type == WriteBatch::WriteType::UPSERT || record.type == WriteBatch::WriteType::REF);

    writeIntBinary(record.type, buf);

    UInt32 flags = 0;
    writeIntBinary(flags, buf);
    writeIntBinary(record.page_id, buf);
    serializeVersionTo(record.version, buf);
    writeIntBinary(record.entry.file_id, buf);
    writeIntBinary(record.entry.offset, buf);
    writeIntBinary(record.entry.size, buf);
    writeIntBinary(record.entry.checksum, buf);
    // fieldsOffset TODO: compression on `fieldsOffset`
    writeIntBinary(record.entry.field_offsets.size(), buf);
    for (const auto & [off, checksum] : record.entry.field_offsets)
    {
        writeIntBinary(off, buf);
        writeIntBinary(checksum, buf);
    }
}

void deserializePutFrom([[maybe_unused]] const WriteBatch::WriteType record_type, ReadBuffer & buf, PageEntriesEdit & edit)
{
    assert(record_type == WriteBatch::WriteType::PUT || record_type == WriteBatch::WriteType::UPSERT || record_type == WriteBatch::WriteType::REF);

    UInt32 flags = 0;
    readIntBinary(flags, buf);
    PageId page_id;
    readIntBinary(page_id, buf);
    PageVersionType version;
    deserializeVersionFrom(buf, version);
    PageEntryV3 entry;
    readIntBinary(entry.file_id, buf);
    readIntBinary(entry.offset, buf);
    readIntBinary(entry.size, buf);
    readIntBinary(entry.checksum, buf);
    // fieldsOffset
    PageFieldOffsetChecksums field_offsets;
    UInt64 size_field_offsets = 0;
    readIntBinary(size_field_offsets, buf);
    if (size_field_offsets != 0)
    {
        entry.field_offsets.reserve(size_field_offsets);
        PageFieldOffset field_offset;
        UInt64 field_checksum;
        for (size_t i = 0; i < size_field_offsets; ++i)
        {
            readIntBinary(field_offset, buf);
            readIntBinary(field_checksum, buf);
            entry.field_offsets.emplace_back(field_offset, field_checksum);
        }
    }

    // All consider as put
    PageEntriesEdit::EditRecord rec;
    rec.type = WriteBatch::WriteType::PUT;
    rec.page_id = page_id;
    rec.version = version;
    rec.entry = entry;
    edit.appendRecord(rec);
}

void serializeDelTo(const PageEntriesEdit::EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == WriteBatch::WriteType::DEL);

    writeIntBinary(record.type, buf);

    writeIntBinary(record.page_id, buf);
    serializeVersionTo(record.version, buf);
}

void deserializeDelFrom([[maybe_unused]] const WriteBatch::WriteType record_type, ReadBuffer & buf, PageEntriesEdit & edit)
{
    assert(record_type == WriteBatch::WriteType::DEL);

    PageId page_id;
    readIntBinary(page_id, buf);
    PageVersionType version;
    deserializeVersionFrom(buf, version);

    PageEntriesEdit::EditRecord rec;
    rec.type = WriteBatch::WriteType::DEL;
    rec.page_id = page_id;
    rec.version = version;
    edit.appendRecord(rec);
}

void deserializeFrom(ReadBuffer & buf, PageEntriesEdit & edit)
{
    WriteBatch::WriteType record_type;
    while (!buf.eof())
    {
        readIntBinary(record_type, buf);
        switch (record_type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
        case WriteBatch::WriteType::REF:
        {
            deserializePutFrom(record_type, buf, edit);
            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            deserializeDelFrom(record_type, buf, edit);
            break;
        }
        default:
            throw Exception(fmt::format("Unkonwn record type: {}", record_type));
        }
    }
}

String serializeTo(const PageEntriesEdit & edit)
{
    WriteBufferFromOwnString buf;
    UInt32 version = 1;
    writeIntBinary(version, buf);
    for (const auto & record : edit.getRecords())
    {
        switch (record.type)
        {
        case DB::WriteBatch::WriteType::PUT:
        case DB::WriteBatch::WriteType::UPSERT:
        case DB::WriteBatch::WriteType::REF:
            serializePutTo(record, buf);
            break;
        case DB::WriteBatch::WriteType::DEL:
            serializeDelTo(record, buf);
            break;
        default:
            throw Exception(fmt::format("Unknown record type: {}", record.type), ErrorCodes::LOGICAL_ERROR);
        }
    }
    return buf.releaseStr();
}

PageEntriesEdit deserializeFrom(std::string_view record)
{
    PageEntriesEdit edit;
    ReadBufferFromMemory buf(record.data(), record.size());
    UInt32 version = 0;
    readIntBinary(version, buf);
    if (version != 1)
        throw Exception("");

    deserializeFrom(buf, edit);
    return edit;
}
} // namespace ser


WALStoreReaderPtr WALStoreReader::create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator)
{
    Poco::Logger * logger = &Poco::Logger::get("WALStore");
    LogFilenameSet log_files;
    {
        std::vector<std::pair<String, Strings>> all_filenames;
        Strings filenames;
        for (const auto & p : delegator->listPaths())
        {
            Poco::File directory(p);
            if (!directory.exists())
                directory.createDirectories();
            filenames.clear();
            directory.list(filenames);
            all_filenames.emplace_back(std::make_pair(p, std::move(filenames)));
            filenames.clear();
        }
        ASSERT(all_filenames.size() == 1); // TODO: multi-path
        for (const auto & [parent_path, filenames] : all_filenames)
        {
            for (const auto & filename : filenames)
            {
                auto name = LogFileName::parseFrom(parent_path, filename, logger);
                if (name.stage == Format::LogFileStage::Normal)
                {
                    log_files.insert(name);
                }
            }
        }
    }

    auto reader = std::make_shared<WALStoreReader>(provider, std::move(log_files));
    reader->openNextFile();
    return reader;
}

WALStoreReader::WALStoreReader(FileProviderPtr & provider_, LogFilenameSet && all_filenames_)
    : provider(provider_)
    , all_filenames(std::move(all_filenames_))
    , next_reading_file(all_filenames.begin())
    , logger(&Poco::Logger::get("LogReader"))
{}

bool WALStoreReader::remained() const
{
    if (reader == nullptr)
        return false;

    if (!reader->isEOF())
        return true;
    if (next_reading_file != all_filenames.end())
        return true;
    return false;
}

std::tuple<bool, PageEntriesEdit> WALStoreReader::next()
{
    bool ok = false;
    String record;
    do
    {
        std::tie(ok, record) = reader->readRecord();
        if (ok)
        {
            LOG_FMT_TRACE(logger, "deserialize [size={}] [deser={}]", record.size(), Redact::keyToHexString(record.data(), record.size()));
            return {true, ser::deserializeFrom(record)};
        }
        // Roll to read the next file
        if (bool next_file = openNextFile(); !next_file)
        {
            // No more file to be read.
            return {false, PageEntriesEdit{}};
        }
    } while (true);
}

bool WALStoreReader::openNextFile()
{
    if (next_reading_file == all_filenames.end())
    {
        return false;
    }

    {
        const auto & parent_path = next_reading_file->parent_path;
        const auto log_num = next_reading_file->log_num;
        const auto level_num = next_reading_file->level_num;
        const auto filename = fmt::format("log_{}_{}", log_num, level_num);

        auto read_buf = createReadBufferFromFileBaseByFileProvider(
            provider,
            fmt::format("{}/{}", parent_path, filename),
            EncryptionPath{parent_path, filename},
            /*estimated_size*/ Format::BLOCK_SIZE,
            /*aio_threshold*/ 0,
            /*read_limiter*/ nullptr,
            /*buffer_size*/ Format::BLOCK_SIZE // Must be `Format::BLOCK_SIZE`
        );
        reader = std::make_unique<LogReader>(
            std::move(read_buf),
            &reporter,
            /*verify_checksum*/ true,
            log_num,
            WALRecoveryMode::TolerateCorruptedTailRecords,
            logger);
    }
    ++next_reading_file; // Note this will invalid `parent_path`
    return true;
}

} // namespace DB::PS::V3
