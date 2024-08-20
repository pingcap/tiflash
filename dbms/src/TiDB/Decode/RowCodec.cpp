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

#include <Columns/IColumn.h>
#include <IO/Endian.h>
#include <IO/Operators.h>
#include <TiDB/Decode/Datum.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDB.h>


namespace DB
{
using TiDB::ColumnInfo;
using TiDB::ColumnInfos;
using TiDB::TableInfo;

template <typename T>
static T decodeUInt(size_t & cursor, const TiKVValue::Base & raw_value)
{
    T res = readLittleEndian<T>(&raw_value[cursor]);
    cursor += sizeof(T);
    return res;
}

template <typename T>
static void encodeUInt(T u, WriteBuffer & ss)
{
    u = toLittleEndian(u);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

template <typename Target, typename Source>
static void decodeUInts(size_t & cursor, const TiKVValue::Base & raw_value, size_t n, std::vector<Target> & target)
{
    target.reserve(n);
    for (size_t i = 0; i < n; i++)
        target.emplace_back(decodeUInt<Source>(cursor, raw_value));
}

template <typename Target, typename Sign, typename = std::enable_if_t<std::is_signed_v<Sign>>>
static std::make_signed_t<Target> castIntWithLength(Sign i)
{
    return static_cast<std::make_signed_t<Target>>(i);
}

template <typename Target, typename Sign, typename = std::enable_if_t<std::is_unsigned_v<Sign>>>
static std::make_unsigned_t<Target> castIntWithLength(Sign i)
{
    return static_cast<std::make_unsigned_t<Target>>(i);
}

template <typename T>
static void encodeIntWithLength(T i, WriteBuffer & ss)
{
    if (castIntWithLength<UInt64>(castIntWithLength<UInt8>(i)) == i)
        encodeUInt(static_cast<UInt8>(castIntWithLength<UInt8>(i)), ss);
    else if (castIntWithLength<UInt64>(castIntWithLength<UInt16>(i)) == i)
        encodeUInt(static_cast<UInt16>(castIntWithLength<UInt16>(i)), ss);
    else if (castIntWithLength<UInt64>(castIntWithLength<UInt32>(i)) == i)
        encodeUInt(static_cast<UInt32>(castIntWithLength<UInt32>(i)), ss);
    else
        encodeUInt(static_cast<UInt64>(i), ss);
}

enum struct RowCodecVer : UInt8
{
    ROW_V2 = 128,
};

namespace RowV2
{
static constexpr UInt8 BigRowMask = 0x1;

template <bool is_big = false>
struct Types
{
    using ColumnIDType = UInt8;
    using ValueOffsetType = UInt16;
};

template <>
struct Types<true>
{
    using ColumnIDType = UInt32;
    using ValueOffsetType = UInt32;
};

TiKVValue::Base encodeNotNullColumn(const Field & field, const ColumnInfo & column_info)
{
    WriteBufferFromOwnString ss;

    switch (column_info.tp)
    {
    case TiDB::TypeLongLong:
    case TiDB::TypeLong:
    case TiDB::TypeInt24:
    case TiDB::TypeShort:
    case TiDB::TypeTiny:
        column_info.hasUnsignedFlag() ? encodeIntWithLength(field.safeGet<UInt64>(), ss)
                                      : encodeIntWithLength(field.safeGet<Int64>(), ss);
        break;
    case TiDB::TypeYear:
        encodeIntWithLength(field.safeGet<Int64>(), ss);
        break;
    case TiDB::TypeFloat:
    case TiDB::TypeDouble:
        EncodeFloat64(field.safeGet<Float64>(), ss);
        break;
    case TiDB::TypeVarString:
    case TiDB::TypeVarchar:
    case TiDB::TypeString:
    case TiDB::TypeBlob:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeJSON:
        return field.safeGet<String>();
    case TiDB::TypeNewDecimal:
        EncodeDecimalForRow(field, ss, column_info);
        break;
    case TiDB::TypeTimestamp:
    case TiDB::TypeDate:
    case TiDB::TypeDatetime:
    case TiDB::TypeBit:
    case TiDB::TypeSet:
    case TiDB::TypeEnum:
        encodeIntWithLength(field.safeGet<UInt64>(), ss);
        break;
    case TiDB::TypeTime:
        encodeIntWithLength(field.safeGet<Int64>(), ss);
        break;
    default:
        throw Exception(std::string("Invalid TP ") + std::to_string(column_info.tp) + " of column " + column_info.name);
    }

    return ss.str();
}
} // namespace RowV2

void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss)
{
    size_t column_in_key = 0;
    if (table_info.pk_is_handle)
        column_in_key = 1;
    else if (table_info.is_common_handle)
        column_in_key = table_info.getPrimaryIndexInfo().idx_cols.size();
    if (table_info.columns.size() < fields.size() + column_in_key)
        throw Exception(
            std::string("Encoding row has ") + std::to_string(table_info.columns.size()) + " columns but "
                + std::to_string(fields.size() + table_info.pk_is_handle) + " values: ",
            ErrorCodes::LOGICAL_ERROR);

    size_t encoded_fields_idx = 0;
    for (const auto & column_info : table_info.columns)
    {
        if ((table_info.pk_is_handle || table_info.is_common_handle) && column_info.hasPriKeyFlag())
            continue;
        EncodeDatum(Field(column_info.id), TiDB::CodecFlagInt, ss);
        EncodeDatumForRow(fields[encoded_fields_idx++], column_info.getCodecFlag(), ss, column_info);
        if (encoded_fields_idx == fields.size())
            break;
    }
}

struct RowEncoderV2
{
    RowEncoderV2(const TableInfo & table_info_, const std::vector<Field> & fields_)
        : table_info(table_info_)
        , fields(fields_)
    {}

    void encode(WriteBuffer & ss) &&
    {
        size_t column_in_key = 0;
        if (table_info.pk_is_handle)
            column_in_key = 1;
        else if (table_info.is_common_handle)
            column_in_key = table_info.getPrimaryIndexInfo().idx_cols.size();
        if (table_info.columns.size() < fields.size() + column_in_key)
            throw Exception(
                std::string("Encoding row has ") + std::to_string(table_info.columns.size()) + " columns but "
                    + std::to_string(fields.size() + table_info.pk_is_handle) + " values: ",
                ErrorCodes::LOGICAL_ERROR);

        bool is_big = false;
        size_t value_length = 0;

        /// Cache encoded individual columns.
        for (size_t i_col = 0, i_val = 0; i_col < table_info.columns.size(); i_col++)
        {
            if (i_val == fields.size())
                break;

            const auto & column_info = table_info.columns[i_col];
            const auto & field = fields[i_val];
            if ((table_info.pk_is_handle || table_info.is_common_handle) && column_info.hasPriKeyFlag())
            {
                // for common handle/pk is handle table,
                // the field with primary key flag is usually encoded to key instead of value
                continue;
            }

            if (column_info.id > std::numeric_limits<typename RowV2::Types<false>::ColumnIDType>::max())
                is_big = true;
            if (!field.isNull())
            {
                num_not_null_columns++;
                auto value = RowV2::encodeNotNullColumn(field, column_info);
                value_length += value.length();
                not_null_column_id_values.emplace(column_info.id, std::move(value));
            }
            else
            {
                num_null_columns++;
                null_column_ids.emplace(column_info.id);
            }
            i_val++;
        }
        is_big = is_big || value_length > std::numeric_limits<RowV2::Types<false>::ValueOffsetType>::max();

        /// Encode header.
        encodeUInt(static_cast<UInt8>(RowCodecVer::ROW_V2), ss);
        UInt8 row_flag = 0;
        row_flag |= is_big ? RowV2::BigRowMask : 0;
        encodeUInt(row_flag, ss);

        /// Encode column numbers and IDs.
        encodeUInt(static_cast<UInt16>(num_not_null_columns), ss);
        encodeUInt(static_cast<UInt16>(num_null_columns), ss);
        is_big ? encodeColumnIDs<RowV2::Types<true>::ColumnIDType>(ss)
               : encodeColumnIDs<RowV2::Types<false>::ColumnIDType>(ss);

        /// Encode value offsets.
        is_big ? encodeValueOffsets<RowV2::Types<true>::ValueOffsetType>(ss)
               : encodeValueOffsets<RowV2::Types<false>::ValueOffsetType>(ss);

        /// Encode values.
        encodeValues(ss);
    }

private:
    template <typename T>
    void encodeColumnIDs(WriteBuffer & ss)
    {
        for (const auto & not_null_id_val : not_null_column_id_values)
        {
            encodeUInt(static_cast<T>(not_null_id_val.first), ss);
        }
        for (const auto & null_id : null_column_ids)
        {
            encodeUInt(static_cast<T>(null_id), ss);
        }
    }

    template <typename T>
    void encodeValueOffsets(WriteBuffer & ss)
    {
        T offset = 0;
        for (const auto & not_null_id_val : not_null_column_id_values)
        {
            offset += static_cast<T>(not_null_id_val.second.length());
            encodeUInt(offset, ss);
        }
    }

    void encodeValues(WriteBuffer & ss)
    {
        for (const auto & not_null_id_val : not_null_column_id_values)
        {
            ss << not_null_id_val.second;
        }
    }

private:
    const TableInfo & table_info;
    const std::vector<Field> & fields;

    size_t num_not_null_columns = 0;
    size_t num_null_columns = 0;
    std::map<ColumnID, TiKVValue::Base> not_null_column_id_values;
    std::set<ColumnID> null_column_ids;
};

void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss)
{
    RowEncoderV2(table_info, fields).encode(ss);
}

// pre-declar block
template <bool is_big>
bool appendRowV2ToBlockImpl(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool ignore_pk_if_absent,
    bool force_decode);

bool appendRowV1ToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool ignore_pk_if_absent,
    bool force_decode);
// pre-declar block end

bool appendRowToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const DecodingStorageSchemaSnapshotConstPtr & schema_snapshot,
    bool force_decode)
{
    const ColumnInfos & column_infos = schema_snapshot->column_infos;
    // when pk is handle, we need skip pk column when decoding value
    ColumnID pk_handle_id = InvalidColumnID;
    if (schema_snapshot->pk_is_handle)
    {
        pk_handle_id = schema_snapshot->pk_column_ids[0];
    }

    // For pk_is_handle table, the column with primary key flag is decoded from encoded key instead of encoded value.
    // For common handle table, the column with primary key flag is (usually) decoded from encoded key. We skip
    // filling the columns with primary key flags inside this method.
    // For other table (non-clustered, use hidden _tidb_rowid as handle), the column with primary key flags could be
    // changed, we need to fill missing column with default value.
    const bool ignore_pk_if_absent = schema_snapshot->is_common_handle || schema_snapshot->pk_is_handle;

    switch (static_cast<UInt8>(raw_value[0]))
    {
    case static_cast<UInt8>(RowCodecVer::ROW_V2):
    {
        auto row_flag = readLittleEndian<UInt8>(&raw_value[1]);
        bool is_big = row_flag & RowV2::BigRowMask;
        return is_big ? appendRowV2ToBlockImpl<true>(
                   raw_value,
                   column_ids_iter,
                   column_ids_iter_end,
                   block,
                   block_column_pos,
                   column_infos,
                   pk_handle_id,
                   ignore_pk_if_absent,
                   force_decode)
                      : appendRowV2ToBlockImpl<false>(
                          raw_value,
                          column_ids_iter,
                          column_ids_iter_end,
                          block,
                          block_column_pos,
                          column_infos,
                          pk_handle_id,
                          ignore_pk_if_absent,
                          force_decode);
    }
    default:
        return appendRowV1ToBlock(
            raw_value,
            column_ids_iter,
            column_ids_iter_end,
            block,
            block_column_pos,
            column_infos,
            pk_handle_id,
            ignore_pk_if_absent,
            force_decode);
    }
}

inline bool addDefaultValueToColumnIfPossible(
    const ColumnInfo & column_info,
    Block & block,
    size_t block_column_pos,
    bool ignore_pk_if_absent,
    bool force_decode)
{
    // We consider a missing column could be safely filled with NULL, unless it has not default value and is NOT NULL.
    // This could saves lots of unnecessary schema syncs for old data with a newer schema that has newly added columns.

    if (column_info.hasPriKeyFlag())
    {
        // For clustered index or pk_is_handle, if the pk column does not exists, it can still be decoded from the key
        if (ignore_pk_if_absent)
            return true;

        assert(!ignore_pk_if_absent);
        if (!force_decode)
            return false;
        // Else non-clustered index, and not pk_is_handle, it could be a row encoded by older schema,
        // we need to fill the column wich has primary key flag with default value.
        // fallthrough to fill default value when force_decode
    }

    if (column_info.hasNoDefaultValueFlag() && column_info.hasNotNullFlag())
    {
        if (!force_decode)
            return false;
        // Else the row does not contain this "not null" / "no default value" column,
        // it could be a row encoded by older schema.
        // fallthrough to fill default value when force_decode
    }
    // not null or has no default value, tidb will fill with specific value.
    auto * raw_column = const_cast<IColumn *>((block.getByPosition(block_column_pos)).column.get());
    raw_column->insert(column_info.defaultValueToField());
    return true;
}

template <bool is_big>
bool appendRowV2ToBlockImpl(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool ignore_pk_if_absent,
    bool force_decode)
{
    size_t cursor = 2; // Skip the initial codec ver and row flag.
    size_t num_not_null_columns = decodeUInt<UInt16>(cursor, raw_value);
    size_t num_null_columns = decodeUInt<UInt16>(cursor, raw_value);
    std::vector<ColumnID> not_null_column_ids;
    std::vector<ColumnID> null_column_ids;
    std::vector<size_t> value_offsets;
    decodeUInts<ColumnID, typename RowV2::Types<is_big>::ColumnIDType>(
        cursor,
        raw_value,
        num_not_null_columns,
        not_null_column_ids);
    decodeUInts<ColumnID, typename RowV2::Types<is_big>::ColumnIDType>(
        cursor,
        raw_value,
        num_null_columns,
        null_column_ids);
    decodeUInts<size_t, typename RowV2::Types<is_big>::ValueOffsetType>(
        cursor,
        raw_value,
        num_not_null_columns,
        value_offsets);
    size_t values_start_pos = cursor;
    size_t idx_not_null = 0;
    size_t idx_null = 0;
    // Merge ordered not null/null columns to keep order.
    while (idx_not_null < not_null_column_ids.size() || idx_null < null_column_ids.size())
    {
        if (column_ids_iter == column_ids_iter_end)
        {
            // extra column
            return force_decode;
        }

        bool is_null;
        if (idx_not_null < not_null_column_ids.size() && idx_null < null_column_ids.size())
            is_null = not_null_column_ids[idx_not_null] > null_column_ids[idx_null];
        else
            is_null = idx_null < null_column_ids.size();

        auto next_datum_column_id = is_null ? null_column_ids[idx_null] : not_null_column_ids[idx_not_null];
        const auto next_column_id = column_ids_iter->first;
        if (next_column_id > next_datum_column_id)
        {
            // The next column id to read is bigger than the column id of next datum in encoded row.
            // It means this is the datum of extra column. May happen when reading after dropping
            // a column.
            if (!force_decode)
                return false;
            // Ignore the extra column and continue to parse other datum
            if (is_null)
                idx_null++;
            else
                idx_not_null++;
        }
        else if (next_column_id < next_datum_column_id)
        {
            // The next column id to read is less than the column id of next datum in encoded row.
            // It means this is the datum of missing column. May happen when reading after adding
            // a column.
            // Fill with default value and continue to read data for next column id.
            const auto & column_info = column_infos[column_ids_iter->second];
            if (!addDefaultValueToColumnIfPossible(
                    column_info,
                    block,
                    block_column_pos,
                    ignore_pk_if_absent,
                    force_decode))
                return false;
            column_ids_iter++;
            block_column_pos++;
        }
        else
        {
            // If pk_handle_id is a valid column id, then it means the table's pk_is_handle is true
            // we can just ignore the pk value encoded in value part
            if (unlikely(next_column_id == pk_handle_id))
            {
                column_ids_iter++;
                block_column_pos++;
                if (is_null)
                {
                    idx_null++;
                }
                else
                {
                    idx_not_null++;
                }
                continue;
            }

            // Parse the datum.
            auto * raw_column = const_cast<IColumn *>((block.getByPosition(block_column_pos)).column.get());
            const auto & column_info = column_infos[column_ids_iter->second];
            if (is_null)
            {
                if (!raw_column->isColumnNullable())
                {
                    if (!force_decode)
                    {
                        // Detect `NULL` column value in a non-nullable column in the schema, let upper level try to sync the schema.
                        return false;
                    }
                    else
                    {
                        // If user turn a nullable column (with default value) to a non-nullable column. There could exists some rows
                        // with `NULL` values inside the SST files. Or some key-values encoded with old schema come after the schema
                        // change is applied in TiFlash because of network issue.
                        // If the column_id exists in null_column_ids, it means the column has default value but filled with NULL.
                        // Just filled with default value for these old schema rows.
                        raw_column->insert(column_info.defaultValueToField());
                        idx_null++;
                    }
                }
                else
                {
                    // ColumnNullable::insertDefault just insert a null value
                    raw_column->insertDefault();
                    idx_null++;
                }
            }
            else
            {
                size_t start = idx_not_null ? value_offsets[idx_not_null - 1] : 0;
                size_t length = value_offsets[idx_not_null] - start;
                if (!raw_column->decodeTiDBRowV2Datum(values_start_pos + start, raw_value, length, force_decode))
                    return false;
                idx_not_null++;
            }
            column_ids_iter++;
            block_column_pos++;
        }
    }
    while (column_ids_iter != column_ids_iter_end)
    {
        if (column_ids_iter->first != pk_handle_id)
        {
            const auto & column_info = column_infos[column_ids_iter->second];
            if (!addDefaultValueToColumnIfPossible(
                    column_info,
                    block,
                    block_column_pos,
                    ignore_pk_if_absent,
                    force_decode))
                return false;
        }
        column_ids_iter++;
        block_column_pos++;
    }
    return true;
}

using TiDB::DatumFlat;
bool appendRowV1ToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const ColumnInfos & column_infos,
    ColumnID pk_handle_id,
    bool ignore_pk_if_absent,
    bool force_decode)
{
    size_t cursor = 0;
    std::map<ColumnID, Field> decoded_fields;
    while (cursor < raw_value.size())
    {
        Field f = DecodeDatum(cursor, raw_value);
        if (f.isNull())
            break;
        ColumnID col_id = f.get<ColumnID>();
        decoded_fields.emplace(col_id, DecodeDatum(cursor, raw_value));
    }
    if (cursor != raw_value.size())
        throw Exception(
            std::string(__PRETTY_FUNCTION__) + ": cursor is not end, remaining: " + raw_value.substr(cursor),
            ErrorCodes::LOGICAL_ERROR);

    auto decoded_field_iter = decoded_fields.begin();
    while (decoded_field_iter != decoded_fields.end())
    {
        if (column_ids_iter == column_ids_iter_end)
        {
            // extra column
            return force_decode;
        }

        auto next_field_column_id = decoded_field_iter->first;
        if (column_ids_iter->first > next_field_column_id)
        {
            // extra column
            if (!force_decode)
                return false;
            decoded_field_iter++;
        }
        else if (column_ids_iter->first < next_field_column_id)
        {
            const auto & column_info = column_infos[column_ids_iter->second];
            if (!addDefaultValueToColumnIfPossible(
                    column_info,
                    block,
                    block_column_pos,
                    ignore_pk_if_absent,
                    force_decode))
                return false;
            column_ids_iter++;
            block_column_pos++;
        }
        else
        {
            // if pk_handle_id is a valid column id, then it means the table's pk_is_handle is true
            // we can just ignore the pk value encoded in value part
            if (unlikely(column_ids_iter->first == pk_handle_id))
            {
                decoded_field_iter++;
                column_ids_iter++;
                block_column_pos++;
                continue;
            }

            auto * raw_column = const_cast<IColumn *>((block.getByPosition(block_column_pos)).column.get());
            const auto & column_info = column_infos[column_ids_iter->second];
            DatumFlat datum(decoded_field_iter->second, column_info.tp);
            const Field & unflattened = datum.field();
            if (datum.overflow(column_info))
            {
                // Overflow detected, fatal if force_decode is true,
                // as schema being newer and narrow shouldn't happen.
                // Otherwise return false to outer, outer should sync schema and try again.
                if (force_decode)
                {
                    throw Exception(
                        "Detected overflow when decoding data " + std::to_string(unflattened.get<UInt64>())
                            + " of column " + column_info.name + " with column " + raw_column->getName(),
                        ErrorCodes::LOGICAL_ERROR);
                }

                return false;
            }
            if (datum.invalidNull(column_info))
            {
                if (!force_decode)
                {
                    // Detect `NULL` column value in a non-nullable column in the schema, let upper level try to sync the schema.
                    return false;
                }
                else
                {
                    // If user turn a nullable column (with default value) to a non-nullable column. There could exists some rows
                    // with `NULL` values inside the SST files. Or some key-values encoded with old schema come after the schema
                    // change is applied in TiFlash because of network issue.
                    // If the column_id exists in null_column_ids, it means the column has default value but filled with NULL.
                    // Just filled with default value for these old schema rows.
                    raw_column->insert(column_info.defaultValueToField());
                }
            }
            else
            {
                raw_column->insert(unflattened);
            }
            decoded_field_iter++;
            column_ids_iter++;
            block_column_pos++;
        }
    }
    while (column_ids_iter != column_ids_iter_end)
    {
        if (column_ids_iter->first != pk_handle_id)
        {
            const auto & column_info = column_infos[column_ids_iter->second];
            if (!addDefaultValueToColumnIfPossible(
                    column_info,
                    block,
                    block_column_pos,
                    ignore_pk_if_absent,
                    force_decode))
                return false;
        }
        column_ids_iter++;
        block_column_pos++;
    }
    return true;
}

} // namespace DB
