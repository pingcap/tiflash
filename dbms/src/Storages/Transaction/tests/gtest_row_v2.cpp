#include <Storages/Transaction/RowCodec.h>
#include <gtest/gtest.h>

namespace DB::tests
{

using TiDB::ColumnInfo;
using TiDB::TableInfo;

template <typename T>
struct ColumnTp
{
};
template <>
struct ColumnTp<Int8>
{
    static const auto tp = TiDB::TypeTiny;
};
template <>
struct ColumnTp<Int16>
{
    static const auto tp = TiDB::TypeShort;
};
template <>
struct ColumnTp<Int32>
{
    static const auto tp = TiDB::TypeLong;
};
template <>
struct ColumnTp<Int64>
{
    static const auto tp = TiDB::TypeLongLong;
};
template <>
struct ColumnTp<UInt8>
{
    static const auto tp = TiDB::TypeTiny;
};
template <>
struct ColumnTp<UInt16>
{
    static const auto tp = TiDB::TypeShort;
};
template <>
struct ColumnTp<UInt32>
{
    static const auto tp = TiDB::TypeLong;
};
template <>
struct ColumnTp<UInt64>
{
    static const auto tp = TiDB::TypeLongLong;
};
template <>
struct ColumnTp<Float32>
{
    static const auto tp = TiDB::TypeFloat;
};
template <>
struct ColumnTp<Float64>
{
    static const auto tp = TiDB::TypeDouble;
};
template <>
struct ColumnTp<String>
{
    static const auto tp = TiDB::TypeString;
};
template <>
struct ColumnTp<DecimalField<Decimal32>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTp<DecimalField<Decimal64>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTp<DecimalField<Decimal128>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};
template <>
struct ColumnTp<DecimalField<Decimal256>>
{
    static const auto tp = TiDB::TypeNewDecimal;
};

template <typename T>
ColumnInfo getColumnInfo(ColumnID id)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.tp = ColumnTp<T>::tp;
    if constexpr (std::is_unsigned_v<T>)
        column_info.setUnsignedFlag();
    return column_info;
}

template <bool is_big>
size_t valueStartPos(const TableInfo & table_info)
{
    return 1 + 1 + 2 + 2 + (is_big ? 8 : 3) * table_info.columns.size();
}

template <bool is_big, typename T>
std::tuple<T, size_t> getValueAndLength(T v)
{
    using NearestType = typename NearestFieldType<T>::Type;

    TableInfo table_info;
    table_info.columns.emplace_back(getColumnInfo<T>(1));
    ColumnIdToIndex column_lut;
    column_lut.set_empty_key(EmptyColumnID);
    column_lut.insert({1, 0});

    std::stringstream ss;
    encodeRowV2(table_info, std::vector<Field>{Field(static_cast<NearestType>(v))}, ss);
    auto encoded = ss.str();
    auto * decoded = decodeRow(encoded, table_info, column_lut);

    return std::make_tuple(
        static_cast<T>(decoded->decoded_fields[0].field.safeGet<NearestType>()), encoded.size() - valueStartPos<is_big>(table_info));
}

#define ASSERT_INT_VALUE_AND_LENGTH(v, l) ASSERT_EQ(getValueAndLength<false>(v), std::make_tuple(v, l))

TEST(RowV2Suite, IntValueAndLength)
{
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int8>::max(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int8>::min(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt8>::max(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt8>::min(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::max()) + Int16(1)), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::min()) - Int16(1)), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::max()) + UInt16(1)), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::min()) - UInt16(1)), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int16>::max(), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int16>::min(), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt16>::max(), 2UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt16>::min(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::max()) + Int32(1)), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::min()) - Int32(1)), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::max()) + UInt32(1)), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::min()) - UInt32(1)), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int32>::max(), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int32>::min(), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt32>::max(), 4UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt32>::min(), 1UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::max()) + Int64(1)), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::min()) - Int64(1)), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + UInt64(1)), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::min()) - UInt64(1)), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int64>::max(), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<Int64>::min(), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt64>::max(), 8UL);
    ASSERT_INT_VALUE_AND_LENGTH(std::numeric_limits<UInt64>::min(), 1UL);
}

#define ASSERT_FLOAT_VALUE(f) ASSERT_FLOAT_EQ(std::get<0>(getValueAndLength<false>(f)), f)

TEST(RowV2Suite, FloatValue)
{
    ASSERT_FLOAT_VALUE(Float32(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::max());
    ASSERT_FLOAT_VALUE(Float64(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::max());
}

#define ASSERT_STRING_VALUE_AND_LENGTH(b, s) ASSERT_EQ(getValueAndLength<b>(s), std::make_tuple(s, s.length()))

TEST(RowV2Suite, StringValueAndLength)
{
    ASSERT_STRING_VALUE_AND_LENGTH(false, String(""));
    ASSERT_STRING_VALUE_AND_LENGTH(false, String("aaa"));
    ASSERT_STRING_VALUE_AND_LENGTH(false, String(std::numeric_limits<UInt16>::max(), 'a'));
    ASSERT_STRING_VALUE_AND_LENGTH(true, String(std::numeric_limits<UInt16>::max() + 1, 'a'));
}

#define ASSERT_DECIMAL_VALUE(d) ASSERT_EQ(std::get<0>(getValueAndLength<false>(d)), d)

TEST(RowV2Suite, DecimalValueAndLength)
{
    ASSERT_DECIMAL_VALUE(DecimalField((ToDecimal<UInt64, Decimal64>(12345678910ULL, 4)), 4));
    ASSERT_DECIMAL_VALUE(DecimalField((ToDecimal<Float64, Decimal32>(1234.56789, 5)), 5));
    ASSERT_DECIMAL_VALUE(DecimalField((ToDecimal<Float64, Decimal32>(1234.56789, 2)), 2));
}

} // namespace DB::tests
