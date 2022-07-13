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

#pragma once
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB::tests
{
using DM::ColumnDefine;
using DM::ColumnDefines;
using TiDB::ColumnInfo;
using TiDB::TableInfo;
using ColumnIDs = std::vector<ColumnID>;

template <typename T>
struct ColumnTP
{
};
template <>
struct ColumnTP<Int8>
{
    static const auto tp = TiDB::TypeTiny;
};
template <>
struct ColumnTP<Int16>
{
    static const auto tp = TiDB::TypeShort;
};
template <>
struct ColumnTP<Int32>
{
    static const auto tp = TiDB::TypeLong;
};
template <>
struct ColumnTP<Int64>
{
    static const auto tp = TiDB::TypeLongLong;
};
template <>
struct ColumnTP<UInt8>
{
    static const auto tp = TiDB::TypeTiny;
};
template <>
struct ColumnTP<UInt16>
{
    static const auto tp = TiDB::TypeShort;
};
template <>
struct ColumnTP<UInt32>
{
    static const auto tp = TiDB::TypeLong;
};
template <>
struct ColumnTP<UInt64>
{
    static const auto tp = TiDB::TypeLongLong;
};
template <>
struct ColumnTP<Float32>
{
    static const auto tp = TiDB::TypeFloat;
};
template <>
struct ColumnTP<Float64>
{
    static const auto tp = TiDB::TypeDouble;
};
template <>
struct ColumnTP<String>
{
    static const auto tp = TiDB::TypeString;
};
template <>
struct ColumnTP<DecimalField<Decimal32>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTP<DecimalField<Decimal64>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTP<DecimalField<Decimal128>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTP<DecimalField<Decimal256>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};

inline String getTestColumnName(ColumnID id)
{
    return "column" + std::to_string(id);
}

template <typename T, bool nullable = false>
ColumnInfo getColumnInfo(ColumnID id)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.tp = ColumnTP<T>::tp;
    column_info.name = getTestColumnName(id);
    if constexpr (std::is_unsigned_v<T>)
        column_info.setUnsignedFlag();
    if constexpr (!nullable)
        column_info.setNotNullFlag();
    return column_info;
}

template <typename T, bool is_null = false>
struct ColumnIDValue
{
    static constexpr bool value_is_null = is_null;
    using ValueType = std::decay_t<T>;
    ColumnIDValue(ColumnID id_, const T & value_)
        : id(id_)
        , value(value_)
    {}
    ColumnIDValue(ColumnID id_, T && value_)
        : id(id_)
        , value(std::move(value_))
    {}
    ColumnID id;
    ValueType value;
};

template <typename T>
struct ColumnIDValue<T, true>
{
    static constexpr bool value_is_null = true;
    using ValueType = std::decay_t<T>;
    ColumnIDValue(ColumnID id_)
        : id(id_)
    {}
    ColumnID id;
};

template <typename T>
using ColumnIDValueNull = ColumnIDValue<T, true>;

using OrderedColumnInfoFields = std::map<ColumnID, std::tuple<ColumnInfo, Field>>;

template <typename DataType>
constexpr bool IsDecimalFieldType = false;
template <>
inline constexpr bool IsDecimalFieldType<DecimalField<Decimal32>> = true;
template <>
inline constexpr bool IsDecimalFieldType<DecimalField<Decimal64>> = true;
template <>
inline constexpr bool IsDecimalFieldType<DecimalField<Decimal128>> = true;
template <>
inline constexpr bool IsDecimalFieldType<DecimalField<Decimal256>> = true;

template <typename Type>
void getTableInfoFieldsInternal(OrderedColumnInfoFields & column_info_fields, Type && column_id_value)
{
    using DecayType = std::decay_t<Type>;
    using ValueType = typename DecayType::ValueType;
    using NearestType = typename NearestFieldType<ValueType>::Type;
    if constexpr (DecayType::value_is_null)
    {
        ColumnInfo column_info = getColumnInfo<ValueType, true>(column_id_value.id);
        // create non zero flen and decimal to avoid error when creating decimal type
        if constexpr (IsDecimalFieldType<NearestType>)
        {
            column_info.flen = 1;
            column_info.decimal = 1;
        }
        column_info_fields.emplace(column_id_value.id, std::make_tuple(column_info, Field()));
    }
    else
    {
        if constexpr (IsDecimalFieldType<NearestType>)
        {
            ColumnInfo column_info = getColumnInfo<ValueType>(column_id_value.id);
            auto field = static_cast<NearestType>(std::move(column_id_value.value));
            column_info.flen = field.getPrec();
            column_info.decimal = field.getScale();
            column_info_fields.emplace(column_id_value.id, std::make_tuple(column_info, field));
        }
        else
        {
            column_info_fields.emplace(column_id_value.id,
                                       std::make_tuple(getColumnInfo<ValueType>(column_id_value.id), static_cast<NearestType>(std::move(column_id_value.value))));
        }
    }
}

template <typename Type, typename... Rest>
void getTableInfoFieldsInternal(OrderedColumnInfoFields & column_info_fields, Type && first, Rest &&... rest)
{
    getTableInfoFieldsInternal(column_info_fields, first);
    getTableInfoFieldsInternal(column_info_fields, std::forward<Rest>(rest)...);
}

template <typename... Types>
std::pair<TableInfo, std::vector<Field>> getTableInfoAndFields(ColumnIDs handle_ids, bool is_common_handle, Types &&... column_value_ids)
{
    OrderedColumnInfoFields column_info_fields;
    getTableInfoFieldsInternal(column_info_fields, std::forward<Types>(column_value_ids)...);
    TableInfo table_info;
    std::vector<Field> fields;
    for (auto & column_info_field : column_info_fields)
    {
        auto & column = std::get<0>(column_info_field.second);
        auto & field = std::get<1>(column_info_field.second);
        if (std::find(handle_ids.begin(), handle_ids.end(), column.id) != handle_ids.end())
        {
            column.setPriKeyFlag();
        }
        table_info.columns.emplace_back(std::move(column));
        fields.emplace_back(std::move(field));
    }
    if (!is_common_handle)
    {
        if (handle_ids[0] != EXTRA_HANDLE_COLUMN_ID)
            table_info.pk_is_handle = true;
    }
    else
    {
        table_info.is_common_handle = true;
        TiDB::IndexInfo index_info;
        for (auto handle_id : handle_ids)
        {
            TiDB::IndexColumnInfo index_column_info;
            for (auto & column : table_info.columns)
            {
                if (column.id == handle_id)
                {
                    index_column_info.name = column.name;
                    break;
                }
            }
            index_info.idx_cols.emplace_back(index_column_info);
        }
        table_info.index_infos.emplace_back(index_info);
    }

    return std::make_pair(std::move(table_info), std::move(fields));
}

inline DecodingStorageSchemaSnapshotConstPtr getDecodingStorageSchemaSnapshot(const TableInfo & table_info)
{
    ColumnDefines store_columns;
    if (table_info.is_common_handle)
    {
        DM::ColumnDefine extra_handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_STRING_TYPE};
        store_columns.emplace_back(extra_handle_column);
    }
    else
    {
        DM::ColumnDefine extra_handle_column{EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE};
        store_columns.emplace_back(extra_handle_column);
    }
    store_columns.emplace_back(VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
    store_columns.emplace_back(TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE);
    ColumnID handle_id = EXTRA_HANDLE_COLUMN_ID;
    for (auto & column_info : table_info.columns)
    {
        if (table_info.pk_is_handle)
        {
            if (column_info.hasPriKeyFlag())
                handle_id = column_info.id;
        }
        store_columns.emplace_back(column_info.id, column_info.name, DB::getDataTypeByColumnInfo(column_info));
    }

    if (handle_id != EXTRA_HANDLE_COLUMN_ID)
    {
        auto iter = std::find_if(store_columns.begin(), store_columns.end(), [&](const ColumnDefine & cd) { return cd.id == handle_id; });
        return std::make_shared<DecodingStorageSchemaSnapshot>(std::make_shared<ColumnDefines>(store_columns), table_info, *iter, /* decoding_schema_version_ */ 1);
    }
    else
    {
        return std::make_shared<DecodingStorageSchemaSnapshot>(std::make_shared<ColumnDefines>(store_columns), table_info, store_columns[0], /* decoding_schema_version_ */ 1);
    }
}

template <bool is_big>
size_t valueStartPos(const TableInfo & table_info)
{
    return 1 + 1 + 2 + 2 + (is_big ? 8 : 3) * table_info.columns.size();
}

inline Block decodeRowToBlock(const String & row_value, DecodingStorageSchemaSnapshotConstPtr decoding_schema)
{
    auto & sorted_column_id_with_pos = decoding_schema->sorted_column_id_with_pos;
    auto iter = sorted_column_id_with_pos.begin();
    const size_t value_column_num = 3;
    // skip first three column which is EXTRA_HANDLE_COLUMN, VERSION_COLUMN, TAG_COLUMN
    for (size_t i = 0; i < value_column_num; i++)
        iter++;

    Block block = createBlockSortByColumnID(decoding_schema);
    if (decoding_schema->pk_is_handle)
        appendRowToBlock(row_value, iter, sorted_column_id_with_pos.end(), block, value_column_num, decoding_schema->column_infos, decoding_schema->pk_column_ids[0], true);
    else
        appendRowToBlock(row_value, iter, sorted_column_id_with_pos.end(), block, value_column_num, decoding_schema->column_infos, InvalidColumnID, true);

    // remove first three column
    for (size_t i = 0; i < value_column_num; i++)
        block.erase(0);
    return block;
}

template <bool is_big, typename T>
std::tuple<T, size_t> getValueLengthByRowV2(const T & v)
{
    using NearestType = typename NearestFieldType<T>::Type;
    auto [table_info, fields] = getTableInfoAndFields({EXTRA_HANDLE_COLUMN_ID}, false, ColumnIDValue(1, v));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    WriteBufferFromOwnString ss;
    encodeRowV2(table_info, fields, ss);
    auto encoded = ss.str();
    Block block = decodeRowToBlock(encoded, decoding_schema);
    return std::make_tuple(static_cast<T>(std::move((*block.getByPosition(0).column)[0].template safeGet<NearestType>())),
                           encoded.size() - valueStartPos<is_big>(table_info));
}

template <typename T>
T getValueByRowV1(const T & v)
{
    using NearestType = typename NearestFieldType<T>::Type;
    auto [table_info, fields] = getTableInfoAndFields({EXTRA_HANDLE_COLUMN_ID}, false, ColumnIDValue(1, v));
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    WriteBufferFromOwnString ss;
    encodeRowV1(table_info, fields, ss);
    auto encoded = ss.str();
    Block block = decodeRowToBlock(encoded, decoding_schema);
    return static_cast<T>(std::move((*block.getByPosition(0).column)[0].template safeGet<NearestType>()));
}

} // namespace DB::tests