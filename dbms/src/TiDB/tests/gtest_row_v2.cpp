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

#include <TiDB/Decode/RowCodec.h>
#include <TiDB/tests/RowCodecTestUtils.h>
#include <gtest/gtest.h>

namespace DB::tests
{
bool isBig(const TiKVValue::Base & encoded)
{
    static constexpr UInt8 BigRowMask = 0x1;
    return encoded[1] & BigRowMask;
}

#define ASSERT_INT_VALUE_LENGTH(v, l) ASSERT_EQ(getValueLengthByRowV2<false>(v), std::make_tuple(v, l))

TEST(RowV2Suite, IntValueLength)
{
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int8>::max(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int8>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt8>::max(), 1UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt8>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::max()) + Int16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int16>(static_cast<Int16>(std::numeric_limits<Int8>::min()) - Int16(1)), 2UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::max()) + UInt16(1)),
        2UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt16>(static_cast<UInt16>(std::numeric_limits<UInt8>::min()) - UInt16(1)),
        2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int16>::max(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int16>::min(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt16>::max(), 2UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt16>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::max()) + Int32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int32>(static_cast<Int32>(std::numeric_limits<Int16>::min()) - Int32(1)), 4UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::max()) + UInt32(1)),
        4UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt32>(static_cast<UInt32>(std::numeric_limits<UInt16>::min()) - UInt32(1)),
        4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int32>::max(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int32>::min(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt32>::max(), 4UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt32>::min(), 1UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::max()) + Int64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(static_cast<Int64>(static_cast<Int64>(std::numeric_limits<Int32>::min()) - Int64(1)), 8UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + UInt64(1)),
        8UL);
    ASSERT_INT_VALUE_LENGTH(
        static_cast<UInt64>(static_cast<UInt64>(std::numeric_limits<UInt32>::min()) - UInt64(1)),
        8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int64>::max(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<Int64>::min(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt64>::max(), 8UL);
    ASSERT_INT_VALUE_LENGTH(std::numeric_limits<UInt64>::min(), 1UL);
}

#define ASSERT_FLOAT_VALUE(f) ASSERT_DOUBLE_EQ(std::get<0>(getValueLengthByRowV2<false>(f)), f)

TEST(RowV2Suite, FloatValue)
{
    ASSERT_FLOAT_VALUE(Float32(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float32>::max());
    ASSERT_FLOAT_VALUE(Float64(0));
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::min());
    ASSERT_FLOAT_VALUE(std::numeric_limits<Float64>::max());
}

#define ASSERT_STRING_VALUE_LENGTH(b, s) ASSERT_EQ(getValueLengthByRowV2<b>(s), std::make_tuple(s, s.length()))

TEST(RowV2Suite, StringValueLength)
{
    ASSERT_STRING_VALUE_LENGTH(false, String(""));
    ASSERT_STRING_VALUE_LENGTH(false, String("aaa"));
    ASSERT_STRING_VALUE_LENGTH(false, String(std::numeric_limits<UInt16>::max(), 'a'));
    ASSERT_STRING_VALUE_LENGTH(true, String(std::numeric_limits<UInt16>::max() + 1, 'a'));
}

#define ASSERT_DECIMAL_VALUE(d) ASSERT_EQ(std::get<0>(getValueLengthByRowV2<false>(d)), d)

TEST(RowV2Suite, DecimalValueLength)
{
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5));
    ASSERT_DECIMAL_VALUE(DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 2), 2));
}

#define ASSERT_ROW_VALUE(is_big, ...)                                                                     \
    {                                                                                                     \
        auto [table_info, fields] = getTableInfoAndFields({MutSup::extra_handle_id}, false, __VA_ARGS__); \
        auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);                              \
        WriteBufferFromOwnString ss;                                                                      \
        encodeRowV2(table_info, fields, ss);                                                              \
        auto encoded = ss.str();                                                                          \
        ASSERT_EQ(is_big, isBig(encoded));                                                                \
        auto block = decodeRowToBlock(encoded, decoding_schema);                                          \
        ASSERT_EQ(fields.size(), block.columns());                                                        \
        for (size_t i = 0; i < fields.size(); i++)                                                        \
        {                                                                                                 \
            ASSERT_EQ(fields[i], ((*block.getByPosition(i).column)[0]));                                  \
        }                                                                                                 \
    }

TEST(RowV2Suite, SmallRow)
{
    // Small row of nulls.
    ASSERT_ROW_VALUE(
        false,
        ColumnIDValueNull<UInt8>(1),
        ColumnIDValueNull<Int16>(2),
        ColumnIDValueNull<UInt32>(4),
        ColumnIDValueNull<Int64>(3));
    // Small row of integers.
    ASSERT_ROW_VALUE(
        false,
        ColumnIDValue(1, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt16>::max()),
        ColumnIDValue(2, std::numeric_limits<Int32>::max()),
        ColumnIDValueNull<UInt8>(5),
        ColumnIDValue(4, std::numeric_limits<UInt64>::min()),
        ColumnIDValueNull<Int64>(6));
    // Small row of string, float, decimal.
    ASSERT_ROW_VALUE(
        false,
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
    ASSERT_ROW_VALUE(
        true,
        ColumnIDValue(1, std::numeric_limits<Int8>::min()),
        ColumnIDValue(3, std::numeric_limits<UInt16>::max()),
        ColumnIDValueNull<UInt32>(std::numeric_limits<UInt8>::max() + 1),
        ColumnIDValue(5, std::numeric_limits<Int32>::max()),
        ColumnIDValue(4, std::numeric_limits<UInt64>::min()));
    // Big row elevated by large not null column ID.
    ASSERT_ROW_VALUE(
        true,
        ColumnIDValueNull<Int8>(1),
        ColumnIDValueNull<UInt16>(3),
        ColumnIDValue(std::numeric_limits<UInt32>::max(), std::numeric_limits<UInt32>::max()),
        ColumnIDValueNull<Int32>(5),
        ColumnIDValueNull<UInt64>(4));
    // Big row elevated by a single large column value.
    ASSERT_ROW_VALUE(
        true,
        ColumnIDValue(3, String(std::numeric_limits<UInt16>::max() + 1, 'a')),
        ColumnIDValue(2, std::numeric_limits<Float64>::min()),
        ColumnIDValue(1, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)));
    // Big row elevated by overall large column values.
    ASSERT_ROW_VALUE(
        true,
        ColumnIDValue(3, String(std::numeric_limits<UInt16>::max() - 8, 'a')),
        ColumnIDValue(2, std::numeric_limits<Float64>::min()),
        ColumnIDValue(1, DecimalField(ToDecimal<Float64, Decimal32>(1234.56789, 5), 5)));
}

} // namespace DB::tests
