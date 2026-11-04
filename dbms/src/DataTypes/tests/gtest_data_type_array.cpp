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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/Vector.h>

#include <cstring>
#include <limits>
#include <vector>

namespace DB::tests
{
namespace
{
template <typename T>
String podBytes(const std::vector<T> & values)
{
    if (values.empty())
        return {};
    return String(reinterpret_cast<const char *>(values.data()), values.size() * sizeof(T));
}

MutableColumnPtr makeFloat32Array(const std::vector<std::vector<Float32>> & rows)
{
    auto nested = ColumnFloat32::create();
    auto offsets = ColumnUInt64::create();
    for (const auto & row : rows)
    {
        for (Float32 value : row)
            nested->insert(value);
        offsets->insert(static_cast<UInt64>(nested->size()));
    }
    return ColumnArray::create(std::move(nested), std::move(offsets));
}

const ColumnArray & asArray(const IColumn & column)
{
    return typeid_cast<const ColumnArray &>(column);
}

void assertArrayEqual(const IColumn & actual_column, const IColumn & expected_column)
{
    const auto & actual = asArray(actual_column);
    const auto & expected = asArray(expected_column);
    ASSERT_EQ(actual.size(), expected.size());

    const auto & actual_nested = typeid_cast<const ColumnFloat32 &>(actual.getData()).getData();
    const auto & expected_nested = typeid_cast<const ColumnFloat32 &>(expected.getData()).getData();
    ASSERT_EQ(actual_nested.size(), expected_nested.size());
    ASSERT_TRUE(actual.getOffsets().empty() || actual.getOffsets().back() == actual_nested.size());

    ColumnArray::Offset previous = 0;
    for (size_t i = 0; i < actual.size(); ++i)
    {
        ASSERT_EQ(actual.getOffsets()[i], expected.getOffsets()[i]);
        ASSERT_GE(actual.getOffsets()[i], previous);
        ASSERT_LE(actual.getOffsets()[i], actual_nested.size());

        const auto length = actual.getOffsets()[i] - previous;
        const auto ref = actual.getDataAt(i);
        ASSERT_EQ(ref.size, length * sizeof(Float32));
        if (length != 0)
        {
            ASSERT_EQ(0, std::memcmp(ref.data, expected_nested.data() + previous, ref.size));
            ASSERT_EQ(
                VectorFloat32Ref(ref).toString(),
                VectorFloat32Ref(expected_nested.data() + previous, length).toString());
        }
        previous = actual.getOffsets()[i];
    }
}

void serializeSplit(
    const IDataType & type,
    const IColumn & column,
    String & sizes,
    String & elements,
    size_t offset,
    size_t limit,
    bool position_independent_encoding)
{
    WriteBufferFromOwnString sizes_out;
    WriteBufferFromOwnString elements_out;
    type.serializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath & path) -> WriteBuffer * {
            if (IDataType::isArraySizes(path))
                return &sizes_out;
            return &elements_out;
        },
        offset,
        limit,
        position_independent_encoding,
        {});
    const auto size_ref = sizes_out.stringRef();
    const auto element_ref = elements_out.stringRef();
    sizes.assign(size_ref.data, size_ref.size);
    elements.assign(element_ref.data, element_ref.size);
}

void deserializeSplit(
    const IDataType & type,
    IColumn & column,
    const String & sizes,
    const String & elements,
    size_t limit,
    bool position_independent_encoding)
{
    ReadBufferFromString sizes_in(sizes);
    ReadBufferFromString elements_in(elements);
    type.deserializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath & path) -> ReadBuffer * {
            if (IDataType::isArraySizes(path))
                return &sizes_in;
            return &elements_in;
        },
        limit,
        0,
        position_independent_encoding,
        {});
}

void deserializeSingleStream(
    const IDataType & type,
    IColumn & column,
    const String & bytes,
    size_t limit,
    bool position_independent_encoding)
{
    ReadBufferFromString in(bytes);
    type.deserializeBinaryBulkWithMultipleStreams(
        column,
        [&](const IDataType::SubstreamPath &) -> ReadBuffer * { return &in; },
        limit,
        0,
        position_independent_encoding,
        {});
}
} // namespace

TEST(DataTypeArrayTest, RoundTripSeparateAndSingleStream)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    const auto expected = makeFloat32Array({{1.0F, 2.0F}, {}, {3.5F}});

    for (const bool position_independent : {true, false})
    {
        String sizes;
        String elements;
        serializeSplit(type, *expected, sizes, elements, 0, 0, position_independent);
        auto actual = type.createColumn();
        deserializeSplit(type, *actual, sizes, elements, expected->size(), position_independent);
        assertArrayEqual(*actual, *expected);
    }

    WriteBufferFromOwnString combined;
    IDataType::SubstreamPath path;
    type.serializeBinaryBulkWithMultipleStreams(
        *expected,
        [&](const IDataType::SubstreamPath &) -> WriteBuffer * { return &combined; },
        0,
        0,
        true,
        path);
    const auto ref = combined.stringRef();
    auto actual = type.createColumn();
    deserializeSingleStream(type, *actual, String(ref.data, ref.size), expected->size(), true);
    assertArrayEqual(*actual, *expected);
}
CATCH

TEST(DataTypeArrayTest, AppendPositionIndependentBatches)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    const auto expected = makeFloat32Array({{1.0F, 2.0F}, {}, {3.5F}, {4.0F}});
    auto actual = type.createColumn();

    String first_sizes;
    String first_elements;
    serializeSplit(type, *expected, first_sizes, first_elements, 0, 2, true);
    deserializeSplit(type, *actual, first_sizes, first_elements, 2, true);

    String second_sizes;
    String second_elements;
    serializeSplit(type, *expected, second_sizes, second_elements, 2, 2, true);
    deserializeSplit(type, *actual, second_sizes, second_elements, 2, true);
    assertArrayEqual(*actual, *expected);

    // The exchange receiver reads sizes and elements from one byte stream, one block at a time.
    WriteBufferFromOwnString combined;
    for (size_t offset : {0, 2})
    {
        IDataType::SubstreamPath path;
        type.serializeBinaryBulkWithMultipleStreams(
            *expected,
            [&](const IDataType::SubstreamPath &) -> WriteBuffer * { return &combined; },
            offset,
            2,
            true,
            path);
    }
    const auto ref = combined.stringRef();
    const String bytes(ref.data, ref.size);
    ReadBufferFromString in(bytes);
    auto from_exchange = type.createColumn();
    for (size_t rows : {2, 2})
    {
        IDataType::SubstreamPath path;
        type.deserializeBinaryBulkWithMultipleStreams(
            *from_exchange,
            [&](const IDataType::SubstreamPath &) -> ReadBuffer * { return &in; },
            rows,
            0,
            true,
            path);
    }
    assertArrayEqual(*from_exchange, *expected);
}
CATCH

TEST(DataTypeArrayTest, RejectsWrappedSizesBeforeNestedColumnIsIndexed)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    deserializeSplit(type, *column, podBytes<UInt64>({1}), podBytes<Float32>({1.0F}), 1, true);

    const std::vector<UInt64> wrapped_sizes{1ULL << 63, (1ULL << 63) + 4};
    const auto exchange = podBytes(wrapped_sizes) + podBytes<Float32>({1.0F, 2.0F, 3.0F, 4.0F});
    try
    {
        deserializeSingleStream(type, *column, exchange, wrapped_sizes.size(), true);
        ADD_FAILURE() << "wrapped array sizes were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("Corrupt array data"), std::string::npos) << e.displayText();
    }

    assertArrayEqual(*column, *makeFloat32Array({{1.0F}}));
    ASSERT_EQ(asArray(*column).getOffsets().back(), 1);
}
CATCH

TEST(DataTypeArrayTest, RejectsOffsetOverflowWhenAppending)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    deserializeSplit(type, *column, podBytes<UInt64>({1}), podBytes<Float32>({7.0F}), 1, true);

    try
    {
        deserializeSplit(type, *column, podBytes<UInt64>({std::numeric_limits<UInt64>::max()}), {}, 1, true);
        ADD_FAILURE() << "overflowing appended array size was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("overflows offset"), std::string::npos) << e.displayText();
    }

    assertArrayEqual(*column, *makeFloat32Array({{7.0F}}));
}
CATCH

TEST(DataTypeArrayTest, RejectsDecreasingAbsoluteOffsets)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    try
    {
        deserializeSplit(
            type,
            *column,
            podBytes<UInt64>({1ULL << 63, 4}),
            podBytes<Float32>({1.0F, 2.0F, 3.0F, 4.0F}),
            2,
            false);
        ADD_FAILURE() << "decreasing absolute array offsets were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("smaller than previous offset"), std::string::npos) << e.displayText();
    }
    ASSERT_TRUE(column->empty());
    ASSERT_TRUE(asArray(*column).getData().empty());

    auto decreasing = type.createColumn();
    try
    {
        deserializeSplit(type, *decreasing, podBytes<UInt64>({5, 3}), podBytes<Float32>({1.0F, 2.0F, 3.0F}), 2, false);
        ADD_FAILURE() << "decreasing absolute array offsets were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("smaller than previous offset"), std::string::npos) << e.displayText();
    }
    ASSERT_TRUE(decreasing->empty());
}
CATCH

TEST(DataTypeArrayTest, RejectsEmptyElementsWithNonZeroOffset)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    deserializeSplit(type, *column, podBytes<UInt64>({1}), podBytes<Float32>({1.0F}), 1, true);

    try
    {
        // Appending a size of 4 with no elements leaves the previous float in place, so the
        // nested column is non-empty and the length check reports a short read.
        deserializeSplit(type, *column, podBytes<UInt64>({4}), {}, 1, true);
        ADD_FAILURE() << "array offsets without elements were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("last offset is"), std::string::npos) << e.displayText();
    }
    assertArrayEqual(*column, *makeFloat32Array({{1.0F}}));

    auto fresh = type.createColumn();
    try
    {
        deserializeSingleStream(type, *fresh, podBytes<UInt64>({4}), 1, true);
        ADD_FAILURE() << "array offsets without elements were accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("last offset is"), std::string::npos) << e.displayText();
    }
    ASSERT_TRUE(fresh->empty());
    ASSERT_TRUE(asArray(*fresh).getData().empty());
}
CATCH

TEST(DataTypeArrayTest, RejectsShortElementStreamAndKeepsPreviousRows)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    deserializeSplit(type, *column, podBytes<UInt64>({1}), podBytes<Float32>({1.0F}), 1, true);

    try
    {
        deserializeSplit(type, *column, podBytes<UInt64>({2}), podBytes<Float32>({2.0F}), 1, true);
        ADD_FAILURE() << "short array element stream was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
        EXPECT_NE(e.displayText().find("last offset is"), std::string::npos) << e.displayText();
    }
    assertArrayEqual(*column, *makeFloat32Array({{1.0F}}));
}
CATCH

TEST(DataTypeArrayTest, RollbackPreservesNullableElementsOnShortRead)
try
{
    const DataTypeArray type(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>()));
    auto column = type.createColumn();
    deserializeSingleStream(
        type,
        *column,
        podBytes<UInt64>({1}) + String(1, '\0') + podBytes<Float32>({7.0F}),
        1,
        true);

    const String sizes = podBytes<UInt64>({2});
    const String null_map(1, '\0');
    const String elements;
    ReadBufferFromString sizes_in(sizes);
    ReadBufferFromString null_map_in(null_map);
    ReadBufferFromString elements_in(elements);
    IDataType::SubstreamPath path;
    EXPECT_THROW(
        type.deserializeBinaryBulkWithMultipleStreams(
            *column,
            [&](const IDataType::SubstreamPath & stream_path) -> ReadBuffer * {
                if (IDataType::isArraySizes(stream_path))
                    return &sizes_in;
                if (IDataType::isNullMap(stream_path))
                    return &null_map_in;
                return &elements_in;
            },
            1,
            0,
            true,
            path),
        Exception);

    const auto & array = asArray(*column);
    const auto & nullable = typeid_cast<const ColumnNullable &>(array.getData());
    ASSERT_EQ(array.size(), 1);
    EXPECT_EQ(array.getOffsets().back(), 1);
    ASSERT_EQ(nullable.getNullMapColumn().size(), 1);
    ASSERT_EQ(nullable.getNestedColumn().size(), 1);
    EXPECT_FALSE(nullable.isNullAt(0));
    EXPECT_EQ(typeid_cast<const ColumnFloat32 &>(nullable.getNestedColumn()).getData()[0], 7.0F);
}
CATCH

TEST(DataTypeArrayTest, RollbackPreservesTupleElementsOnNestedArrayFailure)
try
{
    const DataTypeArray type(std::make_shared<DataTypeTuple>(DataTypes{
        std::make_shared<DataTypeFloat32>(),
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>())}));
    auto column = type.createColumn();
    const String valid
        = podBytes<UInt64>({1}) + podBytes<Float32>({7.0F}) + podBytes<UInt64>({2}) + podBytes<Float32>({11.0F, 13.0F});
    deserializeSingleStream(type, *column, valid, 1, true);

    const String truncated
        = podBytes<UInt64>({1}) + podBytes<Float32>({8.0F}) + podBytes<UInt64>({2}) + podBytes<Float32>({17.0F});
    EXPECT_THROW(deserializeSingleStream(type, *column, truncated, 1, true), Exception);

    const auto & array = asArray(*column);
    const auto & tuple = typeid_cast<const ColumnTuple &>(array.getData());
    ASSERT_EQ(array.size(), 1);
    EXPECT_EQ(array.getOffsets().back(), 1);
    const auto & first = typeid_cast<const ColumnFloat32 &>(*tuple.getColumns()[0]).getData();
    ASSERT_EQ(first.size(), 1);
    EXPECT_EQ(first[0], 7.0F);
    assertArrayEqual(*tuple.getColumns()[1], *makeFloat32Array({{11.0F, 13.0F}}));
}
CATCH

TEST(DataTypeArrayTest, RejectsShortTupleComponent)
try
{
    const DataTypeArray type(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat32>()}));
    auto column = type.createColumn();
    const String valid = podBytes<UInt64>({1}) + podBytes<Float32>({7.0F, 11.0F});
    deserializeSingleStream(type, *column, valid, 1, true);

    const String truncated = podBytes<UInt64>({1}) + podBytes<Float32>({8.0F});
    EXPECT_THROW(deserializeSingleStream(type, *column, truncated, 1, true), Exception);

    const auto & array = asArray(*column);
    const auto & tuple = typeid_cast<const ColumnTuple &>(array.getData());
    ASSERT_EQ(array.size(), 1);
    EXPECT_EQ(array.getOffsets().back(), 1);
    const auto & first = typeid_cast<const ColumnFloat32 &>(*tuple.getColumns()[0]).getData();
    const auto & second = typeid_cast<const ColumnFloat32 &>(*tuple.getColumns()[1]).getData();
    ASSERT_EQ(first.size(), 1);
    ASSERT_EQ(second.size(), 1);
    EXPECT_EQ(first[0], 7.0F);
    EXPECT_EQ(second[0], 11.0F);
}
CATCH

TEST(DataTypeArrayTest, RollbackPreservesStringElementsOnShortRead)
try
{
    const DataTypeArray type(std::make_shared<DataTypeString>(DataTypeString::SerdesFormat::SeparateSizeAndChars));
    auto column = type.createColumn();
    const String original = "keep";
    const auto original_bytes = original.size() + 1;
    const String valid = podBytes<UInt64>({1}) + podBytes<ColumnString::Offset>({original_bytes}) + original + '\0';
    deserializeSingleStream(type, *column, valid, 1, true);

    // The decoder appends the string offset and grows the chars buffer before the short read throws.
    const String truncated = podBytes<UInt64>({1}) + podBytes<ColumnString::Offset>({4}) + "new";
    EXPECT_THROW(deserializeSingleStream(type, *column, truncated, 1, true), Exception);

    const auto & array = asArray(*column);
    const auto & strings = typeid_cast<const ColumnString &>(array.getData());
    ASSERT_EQ(array.size(), 1);
    EXPECT_EQ(array.getOffsets().back(), 1);
    ASSERT_EQ(strings.getOffsets().size(), 1);
    ASSERT_EQ(strings.getOffsets().back(), original_bytes);
    ASSERT_EQ(strings.getChars().size(), original_bytes);
    EXPECT_EQ(strings.getDataAt(0).toStringView(), original);
}
CATCH

TEST(DataTypeArrayTest, GetDataAtRejectsOffsetsPastNestedColumn)
try
{
    auto nested = ColumnFloat32::create();
    nested->insert(1.0F);
    nested->insert(2.0F);
    nested->insert(3.0F);
    nested->insert(4.0F);
    auto offsets = ColumnUInt64::create();
    offsets->insert(1ULL << 63);
    offsets->insert(4);
    auto column = ColumnArray::create(std::move(nested), std::move(offsets));

    for (size_t row = 0; row < column->size(); ++row)
    {
        try
        {
            const auto ref = column->getDataAt(row);
            ADD_FAILURE() << "out-of-bounds array offsets were indexed at row " << row << " size " << ref.size;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
            EXPECT_NE(e.displayText().find("out of bounds"), std::string::npos) << e.displayText();
        }
    }
}
CATCH

} // namespace DB::tests
