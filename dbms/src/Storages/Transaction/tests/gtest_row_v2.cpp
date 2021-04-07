#include <Storages/Transaction/RowCodec.h>
#include <gtest/gtest.h>

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

    std::stringstream ss;
    encodeRowV2(table_info, fields, ss);
    auto encoded = ss.str();
    auto * decoded = decodeRow(encoded, table_info, column_lut);
    auto ret = std::make_tuple(static_cast<T>(std::move(decoded->decoded_fields[0].field.template safeGet<NearestType>())),
        encoded.size() - valueStartPos<is_big>(table_info));
    delete decoded;
    return ret;
}

#define ASSERT_INT_VALUE_LENGTH(v, l) ASSERT_EQ(getValueLength<false>(v), std::make_tuple(v, l))

TEST(RowV2Suite, IntValueLength)
{
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int8>::max(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int8>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt8>::max(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt8>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::max()) + Int16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::min()) - Int16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::max()) + UInt16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::min()) - UInt16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int16>::max(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int16>::min(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt16>::max(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt16>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::max()) + Int32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::min()) - Int32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::max()) + UInt32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::min()) - UInt32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int32>::max(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int32>::min(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt32>::max(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt32>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::max()) + Int64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::min()) - Int64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + UInt64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::min()) - UInt64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int64>::max(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int64>::min(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt64>::max(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt64>::min(), 1UL);
}

#define ASSERT_FLOAT_VALUE(f) ASSERT_DOUBLE_EQ(std::get<0>(getValueLength<false>(f)), f)

TEST(RowV2Suite, FloatValue)
{
    ASSERT_FLOAT_VALUE(Float32(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::max());
    ASSERT_FLOAT_VALUE(Float64(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::max());
}

#define ASSERT_STRING_VALUE_LENGTH(b, s) ASSERT_EQ(getValueLength<b>(s), std::make_tuple(s, s.length()))

TEST(RowV2Suite, StringValueLength)
{
    ASSERT_STRING_VALUE_LENGTH(false, String(""));
    ASSERT_STRING_VALUE_LENGTH(false, String("aaa"));
    ASSERT_STRING_VALUE_LENGTH(false, String(std::numeric_limits<UInt16>::max(), 'a'));
    ASSERT_STRING_VALUE_LENGTH(true, String(std::numeric_limits<UInt16>::max() + 1, 'a'));
}

#define ASSERT_DECIMAL_VALUE(d) ASSERT_EQ(std::get<0>(getValueLength<false>(d)), d)

TEST(RowV2Suite, DecimalValueLength)
{
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 2), 2));
}

#define ASSERT_ROW_VALUE(is_big, ...)                                               \
    {                                                                               \
        auto [table_info, column_lut, fields] = getTableInfoLutFields(__VA_ARGS__); \
        std::stringstream ss;                                                       \
        encodeRowV2(table_info, fields, ss);                                        \
        auto encoded = ss.str();                                                    \
        ASSERT_EQ(is_big, isBig(encoded));                                          \
        auto * decoded = decodeRow(encoded, table_info, column_lut);                \
        ASSERT_EQ(fields.size(), decoded->decoded_fields.size());                   \
        for (size_t i = 0; i < fields.size(); i++)                                  \
        {                                                                           \
            ASSERT_EQ(fields[i], decoded->decoded_fields[i].field);                 \
        }                                                                           \
        delete decoded;                                                             \
    }

TEST(RowV2Suite, SmallRow)
{
    // Small row of nulls.
    ASSERT_ROW_VALUE(
        false, ColumnIDValueNull<UInt8>(1), ColumnIDValueNull<Int16>(2), ColumnIDValueNull<UInt32>(4), ColumnIDValueNull<Int64>(3));
    // Small row of integers.
    ASSERT_ROW_VALUE(false,
        ColumnIDValue(1, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt16>::max()),
        ColumnIDValue(2, std::numeric_limits<Int32>::max()),
        ColumnIDValueNull<UInt8>(5),
        ColumnIDValue(4, std::numeric_limits<UInt64>::min()),
        ColumnIDValueNull<Int64>(6));
    // Small row of string, float, decimal.
    ASSERT_ROW_VALUE(false,
        ColumnIDValue(3, String(std::numeric_limits<UInt16>::max() - 128, 'a')),
        ColumnIDValueNull<DecimalField<Decimal128>>(128),
        ColumnIDValue(2, std::numeric_limits<Float64>::min()),
        ColumnIDValueNull<DecimalField<Decimal256>>(4),
        ColumnIDValue(5, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)),
        ColumnIDValueNull<String>(255),
        ColumnIDValueNull<Float32>(1));
}

TEST(RowV2Suite, BigRow)
{
    // Big row elevated by large null column ID.
    ASSERT_ROW_VALUE(true,
        ColumnIDValue(1, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt16>::max()),
        ColumnIDValueNull<UInt32>(std::numeric_limits<UInt8>::max() + 1),
        ColumnIDValue(5, std::numeric_limits<Int32>::max()),
        ColumnIDValue(4, std::numeric_limits<UInt64>::min()));
    // Big row elevated by large not null column ID.
    ASSERT_ROW_VALUE(true,
        ColumnIDValueNull<Int8>(1),
        ColumnIDValueNull<UInt16>(3),
        ColumnIDValue(std::numeric_limits<UInt32>::max(), std::numeric_limits<UInt32>::max()),
        ColumnIDValueNull<Int32>(5),
        ColumnIDValueNull<UInt64>(4));
    // Big row elevated by a single large column value.
    ASSERT_ROW_VALUE(true,
        ColumnIDValue(3, String(std::numeric_limits<UInt16>::max() + 1, 'a')),
        ColumnIDValue(2, std::numeric_limits<Float64>::min()),
        ColumnIDValue(1, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)));
    // Big row elevated by overall large column values.
    ASSERT_ROW_VALUE(true,
        ColumnIDValue(3, String(std::numeric_limits<UInt16>::max() - 8, 'a')),
        ColumnIDValue(2, std::numeric_limits<Float64>::min()),
        ColumnIDValue(1, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)));
}

} // namespace DB::tests
