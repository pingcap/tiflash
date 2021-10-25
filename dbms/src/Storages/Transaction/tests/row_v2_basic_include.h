#pragma once

namespace DB::tests
{
using TiDB::ColumnInfo;
using TiDB::TableInfo;

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

template <typename T, bool nullable = false>
ColumnInfo getColumnInfo(ColumnID id)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.tp = ColumnTP<T>::tp;
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
    ColumnIDValue(ColumnID id_, const T & value_) : id(id_), value(value_) {}
    ColumnIDValue(ColumnID id_, T && value_) : id(id_), value(std::move(value_)) {}
    ColumnID id;
    ValueType value;
};

template <typename T>
struct ColumnIDValue<T, true>
{
    static constexpr bool value_is_null = true;
    using ValueType = std::decay_t<T>;
    ColumnIDValue(ColumnID id_) : id(id_) {}
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
    if constexpr (DecayType::value_is_null)
        column_info_fields.emplace(column_id_value.id, std::make_tuple(getColumnInfo<ValueType, true>(column_id_value.id), Field()));
    else
    {
        using NearestType = typename NearestFieldType<ValueType>::Type;
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
std::tuple<TableInfo, ColumnIdToIndex, std::vector<Field>> getTableInfoLutFields(Types &&... column_value_ids)
{
    OrderedColumnInfoFields column_info_fields;
    getTableInfoFieldsInternal(column_info_fields, std::forward<Types>(column_value_ids)...);
    TableInfo table_info;
    ColumnIdToIndex column_lut;
    column_lut.set_empty_key(EmptyColumnID);
    std::vector<Field> fields;
    for (auto & column_info_field : column_info_fields)
    {
        table_info.columns.emplace_back(std::move(std::get<0>(column_info_field.second)));
        column_lut.insert({table_info.columns.back().id, table_info.columns.size() - 1});
        fields.emplace_back(std::move(std::get<1>(column_info_field.second)));
    }
    return std::make_tuple(std::move(table_info), std::move(column_lut), std::move(fields));
}

template <bool is_big>
size_t valueStartPos(const TableInfo & table_info)
{
    return 1 + 1 + 2 + 2 + (is_big ? 8 : 3) * table_info.columns.size();
}

bool isBig(const TiKVValue::Base & encoded)
{
    static constexpr UInt8 BigRowMask = 0x1;
    return encoded[1] & BigRowMask;
}

template <bool is_big, typename T>
std::tuple<T, size_t> getValueLength(const T & v)
{
    using NearestType = typename NearestFieldType<T>::Type;
    auto [table_info, column_lut, fields] = getTableInfoLutFields(ColumnIDValue(1, v));

    WriteBufferFromOwnString ss;
    encodeRowV2(table_info, fields, ss);
    auto encoded = ss.str();
    auto * decoded = decodeRow(encoded, table_info, column_lut);
    auto ret = std::make_tuple(static_cast<T>(std::move(decoded->decoded_fields[0].field.template safeGet<NearestType>())),
                               encoded.size() - valueStartPos<is_big>(table_info));
    delete decoded;
    return ret;
}
}