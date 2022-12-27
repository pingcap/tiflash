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

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/WriteBatch.h>

#include <magic_enum.hpp>
#include <type_traits>

namespace DB::PS::V3
{
inline void serializeVersionTo(const PageVersion & version, WriteBuffer & buf)
{
    writeIntBinary(version.sequence, buf);
    writeIntBinary(version.epoch, buf);
}

inline void deserializeVersionFrom(ReadBuffer & buf, PageVersion & version)
{
    readIntBinary(version.sequence, buf);
    readIntBinary(version.epoch, buf);
}

inline void serializeEntryTo(const PageEntryV3 & entry, WriteBuffer & buf)
{
    writeIntBinary(entry.file_id, buf);
    writeIntBinary(entry.offset, buf);
    writeIntBinary(entry.size, buf);
    writeIntBinary(entry.padded_size, buf);
    writeIntBinary(entry.checksum, buf);
    writeIntBinary(entry.tag, buf);
    // fieldsOffset TODO: compression on `fieldsOffset`
    writeIntBinary(entry.field_offsets.size(), buf);
    for (const auto & [off, checksum] : entry.field_offsets)
    {
        writeIntBinary(off, buf);
        writeIntBinary(checksum, buf);
    }
}

inline void deserializeEntryFrom(ReadBuffer & buf, PageEntryV3 & entry)
{
    readIntBinary(entry.file_id, buf);
    readIntBinary(entry.offset, buf);
    readIntBinary(entry.size, buf);
    readIntBinary(entry.padded_size, buf);
    readIntBinary(entry.checksum, buf);
    readIntBinary(entry.tag, buf);
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
}

inline void deserializeUniversalPageIDFrom(ReadBuffer & buf, UniversalPageId & page_id)
{
    String s_id;
    readStringBinary(s_id, buf);
    page_id = std::move(s_id);
}

inline void deserializeUInt128PageIDFrom(ReadBuffer & buf, PageIdV3Internal & page_id)
{
    readIntBinary(page_id, buf);
}

template <typename EditRecord>
void serializePutTo(const EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == EditRecordType::PUT || record.type == EditRecordType::UPSERT || record.type == EditRecordType::VAR_ENTRY);

    writeIntBinary(record.type, buf);

    UInt32 flags = 0;
    writeIntBinary(flags, buf);
    if constexpr (std::is_same_v<EditRecord, u128::PageEntriesEdit::EditRecord>)
    {
        writeIntBinary(record.page_id, buf);
    }
    else if constexpr (std::is_same_v<EditRecord, universal::PageEntriesEdit::EditRecord>)
    {
        writeStringBinary(record.page_id.asStr(), buf);
    }
    serializeVersionTo(record.version, buf);
    writeIntBinary(record.being_ref_count, buf);

    serializeEntryTo(record.entry, buf);
}

template <typename EditType>
void deserializePutFrom([[maybe_unused]] const EditRecordType record_type, ReadBuffer & buf, EditType & edit)
{
    assert(record_type == EditRecordType::PUT || record_type == EditRecordType::UPSERT || record_type == EditRecordType::VAR_ENTRY);

    UInt32 flags = 0;
    readIntBinary(flags, buf);

    typename EditType::EditRecord rec;
    rec.type = record_type;
    if constexpr (std::is_same_v<typename EditType::PageId, PageIdV3Internal>)
    {
        deserializeUInt128PageIDFrom(buf, rec.page_id);
    }
    else if constexpr (std::is_same_v<typename EditType::PageId, UniversalPageId>)
    {
        deserializeUniversalPageIDFrom(buf, rec.page_id);
    }
    deserializeVersionFrom(buf, rec.version);
    readIntBinary(rec.being_ref_count, buf);

    deserializeEntryFrom(buf, rec.entry);
    edit.appendRecord(rec);
}

template <typename EditRecord>
void serializeRefTo(const EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == EditRecordType::REF || record.type == EditRecordType::VAR_REF);

    writeIntBinary(record.type, buf);

    if constexpr (std::is_same_v<EditRecord, u128::PageEntriesEdit::EditRecord>)
    {
        writeIntBinary(record.page_id, buf);
        writeIntBinary(record.ori_page_id, buf);
    }
    else if constexpr (std::is_same_v<EditRecord, universal::PageEntriesEdit::EditRecord>)
    {
        writeStringBinary(record.page_id.asStr(), buf);
        writeStringBinary(record.ori_page_id.asStr(), buf);
    }
    serializeVersionTo(record.version, buf);
    assert(record.entry.file_id == INVALID_BLOBFILE_ID);
}

template <typename EditType>
void deserializeRefFrom([[maybe_unused]] const EditRecordType record_type, ReadBuffer & buf, EditType & edit)
{
    assert(record_type == EditRecordType::REF || record_type == EditRecordType::VAR_REF);

    typename EditType::EditRecord rec;
    rec.type = record_type;
    if constexpr (std::is_same_v<typename EditType::PageId, PageIdV3Internal>)
    {
        deserializeUInt128PageIDFrom(buf, rec.page_id);
        deserializeUInt128PageIDFrom(buf, rec.ori_page_id);
    }
    else if constexpr (std::is_same_v<typename EditType::PageId, UniversalPageId>)
    {
        deserializeUniversalPageIDFrom(buf, rec.page_id);
        deserializeUniversalPageIDFrom(buf, rec.ori_page_id);
    }
    deserializeVersionFrom(buf, rec.version);
    edit.appendRecord(rec);
}

template <typename EditRecord>
void serializePutExternalTo(const EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == EditRecordType::PUT_EXTERNAL || record.type == EditRecordType::VAR_EXTERNAL);

    writeIntBinary(record.type, buf);

    if constexpr (std::is_same_v<EditRecord, u128::PageEntriesEdit::EditRecord>)
    {
        writeIntBinary(record.page_id, buf);
    }
    else if constexpr (std::is_same_v<EditRecord, universal::PageEntriesEdit::EditRecord>)
    {
        writeStringBinary(record.page_id.asStr(), buf);
    }
    serializeVersionTo(record.version, buf);
    writeIntBinary(record.being_ref_count, buf);
}

template <typename EditType>
void deserializePutExternalFrom([[maybe_unused]] const EditRecordType record_type, ReadBuffer & buf, EditType & edit)
{
    assert(record_type == EditRecordType::PUT_EXTERNAL || record_type == EditRecordType::VAR_EXTERNAL);

    typename EditType::EditRecord rec;
    rec.type = record_type;
    if constexpr (std::is_same_v<typename EditType::PageId, PageIdV3Internal>)
    {
        deserializeUInt128PageIDFrom(buf, rec.page_id);
    }
    else if constexpr (std::is_same_v<typename EditType::PageId, UniversalPageId>)
    {
        deserializeUniversalPageIDFrom(buf, rec.page_id);
    }
    deserializeVersionFrom(buf, rec.version);
    readIntBinary(rec.being_ref_count, buf);
    edit.appendRecord(rec);
}

template <typename EditRecord>
void serializeDelTo(const EditRecord & record, WriteBuffer & buf)
{
    assert(record.type == EditRecordType::DEL || record.type == EditRecordType::VAR_DELETE);

    writeIntBinary(record.type, buf);

    if constexpr (std::is_same_v<EditRecord, u128::PageEntriesEdit::EditRecord>)
    {
        writeIntBinary(record.page_id, buf);
    }
    else if constexpr (std::is_same_v<EditRecord, universal::PageEntriesEdit::EditRecord>)
    {
        writeStringBinary(record.page_id.asStr(), buf);
    }
    serializeVersionTo(record.version, buf);
}

template <typename EditType>
void deserializeDelFrom([[maybe_unused]] const EditRecordType record_type, ReadBuffer & buf, EditType & edit)
{
    assert(record_type == EditRecordType::DEL || record_type == EditRecordType::VAR_DELETE);

    typename EditType::EditRecord rec;
    rec.type = record_type;
    if constexpr (std::is_same_v<typename EditType::PageId, PageIdV3Internal>)
    {
        deserializeUInt128PageIDFrom(buf, rec.page_id);
    }
    else if constexpr (std::is_same_v<typename EditType::PageId, UniversalPageId>)
    {
        deserializeUniversalPageIDFrom(buf, rec.page_id);
    }
    deserializeVersionFrom(buf, rec.version);
    edit.appendRecord(rec);
}

template <typename EditType>
void deserializeFrom(ReadBuffer & buf, EditType & edit)
{
    EditRecordType record_type;
    while (!buf.eof())
    {
        readIntBinary(record_type, buf);
        switch (record_type)
        {
        case EditRecordType::PUT:
        case EditRecordType::UPSERT:
        case EditRecordType::VAR_ENTRY:
        {
            deserializePutFrom(record_type, buf, edit);
            break;
        }
        case EditRecordType::REF:
        case EditRecordType::VAR_REF:
        {
            deserializeRefFrom(record_type, buf, edit);
            break;
        }
        case EditRecordType::DEL:
        case EditRecordType::VAR_DELETE:
        {
            deserializeDelFrom(record_type, buf, edit);
            break;
        }
        case EditRecordType::PUT_EXTERNAL:
        case EditRecordType::VAR_EXTERNAL:
        {
            deserializePutExternalFrom(record_type, buf, edit);
            break;
        }
        default:
            throw Exception(fmt::format("Unknown record type: {}", static_cast<Int32>(record_type)), ErrorCodes::LOGICAL_ERROR);
        }
    }
}

namespace u128
{
String Serializer::serializeTo(const PageEntriesEdit & edit)
{
    WriteBufferFromOwnString buf;
    UInt32 version = 1;
    writeIntBinary(version, buf);
    for (const auto & record : edit.getRecords())
    {
        switch (record.type)
        {
        case EditRecordType::PUT:
        case EditRecordType::UPSERT:
        case EditRecordType::VAR_ENTRY:
            serializePutTo(record, buf);
            break;
        case EditRecordType::REF:
        case EditRecordType::VAR_REF:
            serializeRefTo(record, buf);
            break;
        case EditRecordType::VAR_DELETE:
        case EditRecordType::DEL:
            serializeDelTo(record, buf);
            break;
        case EditRecordType::PUT_EXTERNAL:
        case EditRecordType::VAR_EXTERNAL:
            serializePutExternalTo(record, buf);
            break;
        }
    }
    return buf.releaseStr();
}
u128::PageEntriesEdit Serializer::deserializeFrom(std::string_view record)
{
    u128::PageEntriesEdit edit;
    ReadBufferFromMemory buf(record.data(), record.size());
    UInt32 version = 0;
    readIntBinary(version, buf);
    if (version != 1)
        throw Exception(fmt::format("Unknown version for PageEntriesEdit deser [version={}]", version), ErrorCodes::LOGICAL_ERROR);

    DB::PS::V3::deserializeFrom(buf, edit);
    return edit;
}
} // namespace u128
namespace universal
{
String Serializer::serializeTo(const PageEntriesEdit & edit)
{
    WriteBufferFromOwnString buf;
    UInt32 version = 1;
    writeIntBinary(version, buf);
    for (const auto & record : edit.getRecords())
    {
        switch (record.type)
        {
        case EditRecordType::PUT:
        case EditRecordType::UPSERT:
        case EditRecordType::VAR_ENTRY:
            serializePutTo(record, buf);
            break;
        case EditRecordType::REF:
        case EditRecordType::VAR_REF:
            serializeRefTo(record, buf);
            break;
        case EditRecordType::VAR_DELETE:
        case EditRecordType::DEL:
            serializeDelTo(record, buf);
            break;
        case EditRecordType::PUT_EXTERNAL:
        case EditRecordType::VAR_EXTERNAL:
            serializePutExternalTo(record, buf);
            break;
        }
    }
    return buf.releaseStr();
}
universal::PageEntriesEdit Serializer::deserializeFrom(std::string_view record)
{
    universal::PageEntriesEdit edit;
    ReadBufferFromMemory buf(record.data(), record.size());
    UInt32 version = 0;
    readIntBinary(version, buf);
    if (version != 1)
        throw Exception(fmt::format("Unknown version for PageEntriesEdit deser [version={}]", version), ErrorCodes::LOGICAL_ERROR);

    DB::PS::V3::deserializeFrom(buf, edit);
    return edit;
}
} // namespace universal

} // namespace DB::PS::V3
