#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/PathPool.h>

namespace DB::PS::V3
{
namespace ser
{
void serializePutTo(const PageEntriesEdit::EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == WriteBatch::WriteType::PUT || record.type == WriteBatch::WriteType::UPSERT || record.type == WriteBatch::WriteType::REF);

    writeIntBinary(record.type, buf);
    UInt32 flags = 0;
    writeIntBinary(flags, buf);
    writeIntBinary(record.page_id, buf);
    writeIntBinary(record.version.sequence, buf);
    writeIntBinary(record.version.epoch, buf);
    writeIntBinary(record.entry.file_id, buf);
    writeIntBinary(record.entry.offset, buf);
    writeIntBinary(record.entry.size, buf);
    writeIntBinary(record.entry.checksum, buf);
    // fieldsOffset
    writeIntBinary(record.entry.field_offsets.size(), buf);
    for (const auto & [off, checksum] : record.entry.field_offsets)
    {
        writeIntBinary(off, buf);
        writeIntBinary(checksum, buf);
    }
}

void deserializePutFrom([[maybe_unused]] const WriteBatch::WriteType record_type, ReadBuffer & buf, PageEntriesEdit & edit)
{
    assert(record_type == WriteBatch::WriteType::DEL);

    UInt32 flags = 0;
    readIntBinary(flags, buf);
    PageId page_id;
    readIntBinary(page_id, buf);
    PageVersionType version;
    readIntBinary(version.sequence, buf);
    readIntBinary(version.epoch, buf);
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

    writeIntBinary(record.page_id, buf);
    writeIntBinary(record.version.sequence, buf);
    writeIntBinary(record.version.epoch, buf);
}

void deserializeDelFrom(const WriteBatch::WriteType /*record_type*/, ReadBuffer & buf, PageEntriesEdit & edit)
{
    PageId page_id;
    readIntBinary(page_id, buf);
    PageVersionType version;
    readIntBinary(version.sequence, buf);
    readIntBinary(version.epoch, buf);

    PageEntriesEdit::EditRecord rec;
    rec.type = WriteBatch::WriteType::DEL;
    rec.page_id = page_id;
    rec.version = version;
    edit.appendRecord(rec);
}

void deserializeFrom(ReadBuffer & buf, PageEntriesEdit & edit)
{
    WriteBatch::WriteType record_type;
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
    std::vector<std::pair<String, Strings>> all_filenames;
    {
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
    }

    auto reader = std::make_shared<WALStoreReader>(provider, std::move(all_filenames));
    reader->openNextFile();
    return reader;
}

WALStoreReader::WALStoreReader(FileProviderPtr & provider_, std::vector<std::pair<String, Strings>> && all_filenames_)
    : provider(provider_)
    , all_filenames(std::move(all_filenames_))
    , logger(&Poco::Logger::get("LogReader"))
{}

bool WALStoreReader::remained() const
{
    if (reader == nullptr)
        return false;

    if (!reader->isEOF())
        return true;
    if (all_files_read_index < all_filenames[0].second.size()) // FIXME: multi-path
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
            return {true, ser::deserializeFrom(record)};
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
    if (all_files_read_index >= all_filenames[0].second.size()) // FIXME: multi-path
    {
        return false;
    }

    auto parent_path = all_filenames[0].first;
    auto filename = all_filenames[0].second[all_files_read_index];
    // parse `log_num` from `filename`
    Format::LogNumberType log_num = 0;

    auto read_buf = createReadBufferFromFileBaseByFileProvider(
        provider,
        fmt::format("{}/log_{}", parent_path, log_num),
        EncryptionPath{parent_path, fmt::format("log_{}", log_num)},
        Format::BLOCK_SIZE, // Must be `Format::BLOCK_SIZE`
        0,
        nullptr);
    reader = std::make_unique<LogReader>(
        std::move(read_buf),
        &reporter,
        /*verify_checksum*/ true,
        log_num,
        WALRecoveryMode::TolerateCorruptedTailRecords,
        logger);
    return true;
}

} // namespace DB::PS::V3
