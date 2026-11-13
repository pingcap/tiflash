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

namespace DB::ErrorCodes
{
extern const int TOO_LARGE_ARRAY_SIZE;
}

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

/// Records a bulk read instead of copying. An overflowing element count must be rejected before any read.
class RejectingReadBuffer : public ReadBuffer
{
public:
    RejectingReadBuffer()
        : ReadBuffer(&storage, 1, 0)
    {}

    size_t readBig(char * /*to*/, size_t n) override
    {
        requested = n;
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "element bytes were requested before the element count was validated, bytes={}",
            n);
    }

    size_t requested = 0;

private:
    inline static char storage = 0;
};

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

TEST(DataTypeArrayTest, ReadsPositionIndependentSizesUntilEof)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    auto column = type.createColumn();
    deserializeSplit(type, *column, podBytes<UInt64>({2, 0}), podBytes<Float32>({1.0F, 2.0F}), 5, true);
    assertArrayEqual(*column, *makeFloat32Array({{1.0F, 2.0F}, {}}));

    // A short batch appended to existing rows must also shrink to the number of sizes read.
    deserializeSplit(type, *column, podBytes<UInt64>({1}), podBytes<Float32>({3.0F}), 5, true);
    const auto expected = makeFloat32Array({{1.0F, 2.0F}, {}, {3.0F}});
    assertArrayEqual(*column, *expected);

    deserializeSplit(type, *column, {}, {}, 5, true);
    assertArrayEqual(*column, *expected);
}
CATCH

TEST(DataTypeArrayTest, RejectsOffsetCountOverflowWhenAppending)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    for (const size_t limit : {std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max() - 1})
    {
        auto column = type.createColumn();
        deserializeSplit(type, *column, podBytes<UInt64>({1, 0}), podBytes<Float32>({7.0F}), 2, true);

        try
        {
            deserializeSplit(type, *column, podBytes<UInt64>({0}), {}, limit, true);
            ADD_FAILURE() << "overflowing array offset count was accepted, limit=" << limit;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
            EXPECT_NE(e.displayText().find("overflows offset count"), std::string::npos) << e.displayText();
        }
    }
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
}
CATCH

TEST(DataTypeArrayTest, AllowsEmptyArrays)
try
{
    const DataTypes nested_types{
        std::make_shared<DataTypeFloat32>(),
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>()),
        std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat32>()}),
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>())};
    for (const auto & nested_type : nested_types)
    {
        SCOPED_TRACE(nested_type->getName());
        const DataTypeArray type(nested_type);
        for (const size_t rows : {0, 2})
        {
            const String sizes = podBytes(std::vector<UInt64>(rows, 0));
            for (const bool position_independent : {true, false})
            {
                for (const bool single_stream : {true, false})
                {
                    auto column = type.createColumn();
                    if (single_stream)
                        deserializeSingleStream(type, *column, sizes, rows, position_independent);
                    else
                        deserializeSplit(type, *column, sizes, {}, rows, position_independent);

                    const auto & array = asArray(*column);
                    ASSERT_EQ(array.size(), rows);
                    EXPECT_TRUE(array.getData().empty());
                    for (size_t row = 0; row < rows; ++row)
                    {
                        EXPECT_EQ(array.getOffsets()[row], 0);
                        EXPECT_EQ(array.getDataAt(row).size, 0);
                    }
                }
            }
        }
    }
}
CATCH

TEST(DataTypeArrayTest, RejectsEmptyElementsForNonZeroOffsets)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    for (const bool position_independent : {true, false})
    {
        const String sizes = position_independent ? podBytes<UInt64>({1, 2}) : podBytes<UInt64>({1, 3});
        for (const bool single_stream : {true, false})
        {
            auto column = type.createColumn();
            try
            {
                if (single_stream)
                    deserializeSingleStream(type, *column, sizes, 2, position_independent);
                else
                    deserializeSplit(type, *column, sizes, {}, 2, position_independent);
                ADD_FAILURE() << "non-zero array offsets without elements were accepted";
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR) << e.displayText();
                EXPECT_NE(e.displayText().find("last offset is"), std::string::npos) << e.displayText();
            }
        }
    }
}
CATCH

TEST(DataTypeArrayTest, RejectsMissingNullableAndTupleComponents)
try
{
    const DataTypes nested_types{
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat32>()),
        std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat32>()})};
    for (const auto & nested_type : nested_types)
    {
        SCOPED_TRACE(nested_type->getName());
        const DataTypeArray type(nested_type);
        const String first_value = nested_type->isNullable() ? String(1, '\0') : podBytes<Float32>({7.0F});
        for (const bool position_independent : {true, false})
        {
            for (const bool missing_first_component : {true, false})
            {
                auto column = type.createColumn();
                const String sizes = podBytes<UInt64>({1});
                const String first = missing_first_component ? String{} : first_value;
                const String second = missing_first_component ? podBytes<Float32>({11.0F}) : String{};
                ReadBufferFromString sizes_in(sizes);
                ReadBufferFromString first_in(first);
                ReadBufferFromString second_in(second);
                IDataType::SubstreamPath path;
                EXPECT_THROW(
                    type.deserializeBinaryBulkWithMultipleStreams(
                        *column,
                        [&](const IDataType::SubstreamPath & stream_path) -> ReadBuffer * {
                            if (IDataType::isArraySizes(stream_path))
                                return &sizes_in;
                            if (IDataType::isNullMap(stream_path) || stream_path.back().tuple_element_name == "1")
                                return &first_in;
                            return &second_in;
                        },
                        1,
                        0,
                        position_independent,
                        path),
                    Exception);
            }
        }
    }
}
CATCH

TEST(DataTypeArrayTest, RejectsMissingElementsInNonEmptyNestedColumn)
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
}
CATCH

TEST(DataTypeArrayTest, RejectsShortElementStream)
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
}
CATCH

TEST(DataTypeArrayTest, RejectsShortNullableComponent)
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
    const String null_map(2, '\0');
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
}
CATCH

TEST(DataTypeArrayTest, RejectsShortNestedArrayInTuple)
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
}
CATCH

TEST(DataTypeArrayTest, RejectsShortTupleComponentAfterNestedArray)
try
{
    const DataTypeArray type(std::make_shared<DataTypeTuple>(DataTypes{
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()),
        std::make_shared<DataTypeFloat32>()}));
    auto column = type.createColumn();
    const String valid = podBytes<UInt64>({1, 2}) + podBytes<Float32>({11.0F, 13.0F, 7.0F});
    deserializeSingleStream(type, *column, valid, 1, true);

    // The Array component completes, but the following Float32 component has no data.
    const String truncated = podBytes<UInt64>({1, 1}) + podBytes<Float32>({17.0F});
    EXPECT_THROW(deserializeSingleStream(type, *column, truncated, 1, true), Exception);
}
CATCH

TEST(DataTypeArrayTest, RejectsShortStringElementStream)
try
{
    const DataTypeArray type(std::make_shared<DataTypeString>(DataTypeString::SerdesFormat::SeparateSizeAndChars));
    auto column = type.createColumn();
    const String original = "keep";
    const auto original_bytes = original.size() + 1;
    const String valid = podBytes<UInt64>({1, original_bytes}) + original + '\0';
    deserializeSingleStream(type, *column, valid, 1, true);

    const String truncated = podBytes<UInt64>({1, 4}) + "new";
    EXPECT_THROW(deserializeSingleStream(type, *column, truncated, 1, true), Exception);
}
CATCH

TEST(DataTypeArrayTest, RejectsElementCountThatOverflowsAllocation)
try
{
    const DataTypeArray type(std::make_shared<DataTypeFloat32>());
    // Float32 is 4 bytes and PaddedPODArray adds 80 bytes of alignment padding.
    // 2^62 makes the element-byte multiplication wrap to 0.
    // 2^62 - 1 occupies 2^64 - 4 bytes, so the padding wraps the allocation to 128.
    // 2^62 - 20 makes that padded size wrap to 0.
    // 2^61 - 19 keeps the padded size above 2^63, so power-of-two rounding wraps to 0.
    const UInt64 counts[] = {1ULL << 62, (1ULL << 62) - 1, (1ULL << 62) - 20, (1ULL << 61) - 19};

    for (const bool position_independent : {true, false})
    {
        for (const UInt64 count : counts)
        {
            auto column = type.createColumn();
            const String count_bytes = podBytes<UInt64>({count});
            ReadBufferFromString sizes_in(count_bytes);
            RejectingReadBuffer elements_in;
            try
            {
                IDataType::SubstreamPath path;
                type.deserializeBinaryBulkWithMultipleStreams(
                    *column,
                    [&](const IDataType::SubstreamPath & stream_path) -> ReadBuffer * {
                        if (IDataType::isArraySizes(stream_path))
                            return &sizes_in;
                        return &elements_in;
                    },
                    1,
                    0,
                    position_independent,
                    path);
                ADD_FAILURE() << "element count " << count
                              << " was accepted, position_independent=" << position_independent;
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::TOO_LARGE_ARRAY_SIZE) << e.displayText();
                EXPECT_EQ(elements_in.requested, 0) << e.displayText();
            }
        }
    }
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
