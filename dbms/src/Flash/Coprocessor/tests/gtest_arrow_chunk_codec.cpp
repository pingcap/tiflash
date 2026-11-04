// Copyright 2026 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <IO/Endian.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <limits>

namespace DB::tests
{
namespace
{
template <typename T>
void appendValue(String & data, T value)
{
    const auto little_endian = toLittleEndian(value);
    data.append(reinterpret_cast<const char *>(&little_endian), sizeof(little_endian));
}

String makeHeader(UInt32 rows, UInt32 nulls = 0)
{
    String data;
    appendValue(data, rows);
    appendValue(data, nulls);
    return data;
}

DAGSchema makeSchema(std::initializer_list<TiDB::TP> types, bool nullable = false)
{
    DAGSchema schema;
    for (const auto type : types)
    {
        TiDB::ColumnInfo info;
        info.tp = type;
        if (!nullable)
            info.setNotNullFlag();
        if (type == TiDB::TypeEnum)
            info.elems = {{"a", 1}};
        if (type == TiDB::TypeNewDecimal)
        {
            info.flen = 9;
            info.decimal = 0;
        }
        schema.emplace_back(fmt::format("col{}", schema.size()), std::move(info));
    }
    return schema;
}

String makeIntColumn(std::initializer_list<Int64> values)
{
    String data = makeHeader(values.size());
    for (const auto value : values)
        appendValue(data, value);
    return data;
}

String makeStringColumn(UInt32 rows, std::initializer_list<UInt64> offsets, const String & values)
{
    String data = makeHeader(rows);
    for (const auto offset : offsets)
        appendValue(data, offset);
    data += values;
    return data;
}
} // namespace

TEST(ArrowChunkCodecTest, DecodesValidColumns)
{
    const auto schema = makeSchema({TiDB::TypeLongLong, TiDB::TypeString});
    const auto data = makeIntColumn({42, -7}) + makeStringColumn(2, {0, 3, 6}, "foobar");
    const auto block = ArrowChunkCodec().decode(data, schema);
    ASSERT_EQ(block.columns(), 2);
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(block.getByPosition(0).column->getInt(0), 42);
    EXPECT_EQ(block.getByPosition(0).column->getInt(1), -7);
    EXPECT_EQ(block.getByPosition(1).column->getDataAt(0).toString(), "foo");
    EXPECT_EQ(block.getByPosition(1).column->getDataAt(1).toString(), "bar");
}

TEST(ArrowChunkCodecTest, DecodesNullsAndEmptyStrings)
{
    String data = makeHeader(2, 1);
    data.push_back(1);
    appendValue(data, Int64{42});
    appendValue(data, Int64{0});
    data += makeHeader(2, 1);
    data.push_back(1);
    for (const UInt64 offset : {0, 3, 3})
        appendValue(data, offset);
    data += "foo";

    const auto block = ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeLongLong, TiDB::TypeString}, true));
    ASSERT_EQ(block.rows(), 2);
    EXPECT_FALSE(block.getByPosition(0).column->isNullAt(0));
    EXPECT_TRUE(block.getByPosition(0).column->isNullAt(1));
    EXPECT_EQ(block.getByPosition(1).column->getDataAt(0).toString(), "foo");
    EXPECT_TRUE(block.getByPosition(1).column->isNullAt(1));

    const auto empty_strings
        = ArrowChunkCodec().decode(makeStringColumn(2, {0, 0, 0}, ""), makeSchema({TiDB::TypeString}));
    ASSERT_EQ(empty_strings.rows(), 2);
    EXPECT_EQ(empty_strings.getByPosition(0).column->getDataAt(0).size, 0);
    EXPECT_EQ(empty_strings.getByPosition(0).column->getDataAt(1).size, 0);
}

TEST(ArrowChunkCodecTest, DecodesEmptyChunks)
{
    EXPECT_EQ(ArrowChunkCodec().decode("", makeSchema({TiDB::TypeLongLong})).columns(), 0);
    const auto block = ArrowChunkCodec().decode(
        makeHeader(0) + makeStringColumn(0, {0}, ""),
        makeSchema({TiDB::TypeLongLong, TiDB::TypeString}));
    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(block.rows(), 0);
}

TEST(ArrowChunkCodecTest, DecodesValidEnumAndDecimal)
{
    String enum_value;
    appendValue(enum_value, UInt64{1});
    enum_value += "a";
    String data = makeStringColumn(1, {0, 9}, enum_value);
    data += makeHeader(1);
    data.push_back(3); // integer digits
    data.append(3, '\0'); // fractional digits, result fraction, sign
    appendValue(data, Int32{123});
    data.append(8 * sizeof(Int32), '\0');

    const auto block = ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeEnum, TiDB::TypeNewDecimal}));
    ASSERT_EQ(block.columns(), 2);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.getByPosition(0).column->getInt(0), 1);
    EXPECT_EQ(block.getByPosition(1).column->getDataAt(0).size, sizeof(Int32));
}

TEST(ArrowChunkCodecTest, RejectsEveryTruncatedPrefix)
{
    const std::vector<std::pair<DAGSchema, String>> cases{
        {makeSchema({TiDB::TypeLongLong}), makeIntColumn({42, -7})},
        {makeSchema({TiDB::TypeString}), makeStringColumn(2, {0, 3, 6}, "foobar")},
    };
    for (const auto & [schema, data] : cases)
    {
        for (size_t size = 1; size < data.size(); ++size)
            EXPECT_THROW(ArrowChunkCodec().decode(data.substr(0, size), schema), Exception) << "size=" << size;
    }
}

TEST(ArrowChunkCodecTest, RejectsInvalidColumnCountsAndRowCounts)
{
    const auto data = makeIntColumn({42});
    EXPECT_THROW(ArrowChunkCodec().decode(data, {}), Exception);
    EXPECT_THROW(ArrowChunkCodec().decode(data + data, makeSchema({TiDB::TypeLongLong})), Exception);
    EXPECT_THROW(ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeLongLong, TiDB::TypeLongLong})), Exception);
    EXPECT_THROW(
        ArrowChunkCodec().decode(data + makeIntColumn({1, 2}), makeSchema({TiDB::TypeLongLong, TiDB::TypeLongLong})),
        Exception);
}

TEST(ArrowChunkCodecTest, RejectsInvalidNullCountsAndTruncatedBitmaps)
{
    const auto schema = makeSchema({TiDB::TypeLongLong}, true);
    EXPECT_THROW(ArrowChunkCodec().decode(makeHeader(1, 2), schema), Exception);
    EXPECT_THROW(ArrowChunkCodec().decode(makeHeader(9, 1) + String(1, '\xff'), schema), Exception);
    EXPECT_THROW(ArrowChunkCodec().decode(makeHeader(std::numeric_limits<UInt32>::max(), 1), schema), Exception);
}

TEST(ArrowChunkCodecTest, RejectsOversizedRowCountsBeforeAllocating)
{
    const auto data = makeHeader(std::numeric_limits<UInt32>::max());
    EXPECT_THROW(ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeLongLong})), Exception);
    EXPECT_THROW(ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeString})), Exception);
}

TEST(ArrowChunkCodecTest, RejectsInvalidOffsets)
{
    const auto schema = makeSchema({TiDB::TypeString});
    for (const auto & data : {
             makeStringColumn(1, {1, 1}, "x"),
             makeStringColumn(1, {2, 1}, "xx"),
             makeStringColumn(2, {0, 2, 1}, "xx"),
             makeStringColumn(1, {0, 4}, "abc"),
             makeStringColumn(1, {0, std::numeric_limits<UInt64>::max()}, ""),
         })
        EXPECT_THROW(ArrowChunkCodec().decode(data, schema), Exception);
}

TEST(ArrowChunkCodecTest, DecodesNullEnumValues)
{
    String data = makeHeader(3, 2);
    data.push_back(2);
    for (const UInt64 offset : {0, 0, 9, 9})
        appendValue(data, offset);
    appendValue(data, UInt64{1});
    data += "a";

    const auto block = ArrowChunkCodec().decode(data, makeSchema({TiDB::TypeEnum}, true));
    ASSERT_EQ(block.rows(), 3);
    const auto & column = block.getByPosition(0).column;
    EXPECT_TRUE(column->isNullAt(0));
    EXPECT_FALSE(column->isNullAt(1));
    EXPECT_EQ((*column)[1].get<Int64>(), 1);
    EXPECT_TRUE(column->isNullAt(2));
}

TEST(ArrowChunkCodecTest, RejectsTruncatedEnumValues)
{
    const auto schema = makeSchema({TiDB::TypeEnum});
    for (UInt64 size = 0; size < sizeof(UInt64); ++size)
        EXPECT_THROW(ArrowChunkCodec().decode(makeStringColumn(1, {0, size}, String(size, '\0')), schema), Exception);
}

TEST(ArrowChunkCodecTest, RejectsUnexpectedDecodedRowCounts)
{
    EXPECT_THROW(ArrowChunkCodec().decode(makeStringColumn(1, {0, 0}, ""), makeSchema({TiDB::TypeBit})), Exception);
}

TEST(ArrowChunkCodecTest, RejectsDecimalWordBufferOverflow)
{
    const auto schema = makeSchema({TiDB::TypeNewDecimal});
    for (const auto & [integer_digits, fractional_digits] :
         std::vector<std::pair<UInt8, UInt8>>{{255, 0}, {0, 255}, {45, 45}})
    {
        String data = makeHeader(1);
        data.push_back(static_cast<char>(integer_digits));
        data.push_back(static_cast<char>(fractional_digits));
        data.append(38, '\0');
        EXPECT_THROW(ArrowChunkCodec().decode(data, schema), Exception);
    }
}
} // namespace DB::tests
