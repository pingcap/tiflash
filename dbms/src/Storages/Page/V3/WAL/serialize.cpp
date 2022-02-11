#include <IO/ReadHelpers.h>
#include <Storages/Page/V3/WAL/serialize.h>

namespace DB::PS::V3::ser
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
} // namespace DB::PS::V3::ser
