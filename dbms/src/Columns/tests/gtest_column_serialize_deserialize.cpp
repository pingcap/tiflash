// Copyright 2024 PingCAP, Inc.
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
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnTuple.h>
#include <Common/COWPtr.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class TestColumnSerializeDeserialize : public ::testing::Test
{
public:
    static void testCountSerializeByteSize(
        const ColumnPtr & column_ptr,
        const PaddedPODArray<size_t> & result_byte_size)
    {
        PaddedPODArray<size_t> byte_size(column_ptr->size());
        for (size_t i = 0; i < column_ptr->size(); ++i)
            byte_size[i] = i;
        column_ptr->countSerializeByteSize(byte_size);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
            ASSERT_EQ(byte_size[i], i + result_byte_size[i]);
    }

    static void testCountSerialByteSizeForColumnArray(
        const ColumnPtr & column_ptr,
        const ColumnPtr & offsets,
        const PaddedPODArray<size_t> & result_byte_size)
    {
        auto column_array = ColumnArray::create(column_ptr->cloneFullColumn(), offsets->cloneFullColumn());
        PaddedPODArray<size_t> byte_size(column_array->size());
        for (size_t i = 0; i < column_array->size(); ++i)
            byte_size[i] = i;
        column_array->countSerializeByteSize(byte_size);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
            ASSERT_EQ(byte_size[i], 8 + i + result_byte_size[i]);
    }

    static void testSerializeAndDeserialize(const ColumnPtr & column_ptr)
    {
        doTestSerializeAndDeserialize(column_ptr, false);
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
        doTestSerializeAndDeserialize(column_ptr, true);
#endif
    }

    static void doTestSerializeAndDeserialize(const ColumnPtr & column_ptr, bool is_aligned [[maybe_unused]])
    {
        PaddedPODArray<size_t> byte_size(column_ptr->size());
        column_ptr->countSerializeByteSize(byte_size);
        size_t total_size = 0;
        for (auto i : byte_size)
            total_size += i;
        PaddedPODArray<char> memory(total_size);
        PaddedPODArray<char *> pos;
        size_t current_size = 0;
        for (size_t i = 0; i < byte_size.size() / 2; ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        column_ptr->serializeToPos(pos, 0, byte_size.size() / 2, false);
        for (size_t i = 0; i < byte_size.size() / 2; ++i)
            pos[i] -= byte_size[i];

        auto new_col_ptr = column_ptr->cloneEmpty();
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
        if (is_aligned)
            new_col_ptr->reserveAlign(byte_size.size(), AlignBufferAVX2::buffer_size);
#endif
        ColumnsAlignBufferAVX2 align_buffer;
        new_col_ptr->deserializeAndInsertFromPos(pos, align_buffer);

        current_size = 0;
        pos.clear();
        for (size_t i = byte_size.size() / 2; i < byte_size.size() - 1; ++i)
        {
            pos.push_back(memory.data() + current_size);
            current_size += byte_size[i];
        }
        pos.push_back(nullptr);
        column_ptr->serializeToPos(pos, byte_size.size() / 2, byte_size.size() - byte_size.size() / 2, true);
        for (size_t i = byte_size.size() / 2; i < byte_size.size() - 1; ++i)
            pos[i - byte_size.size() / 2] -= byte_size[i];
        pos.resize(pos.size() - 1);
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
        align_buffer.reset(true);
#endif
        new_col_ptr->deserializeAndInsertFromPos(pos, align_buffer);
        new_col_ptr->insertFrom(*column_ptr, byte_size.size() - 1);
        ASSERT_COLUMN_EQ(std::move(new_col_ptr), column_ptr);
    }
};

TEST_F(TestColumnSerializeDeserialize, TestColumnVector)
try
{
    auto col_vector = createColumn<UInt64>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}).column;
    testCountSerializeByteSize(col_vector, {8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8});
    //auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 10, 16}).column;
    //testCountSerialByteSizeForColumnArray(col_vector, col_offsets, {8, 16, 24, 32, 48});

    testSerializeAndDeserialize(col_vector);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnDecimal)
try
{
    auto col_decimal = createColumn<Decimal32>(
                           std::make_tuple(8, 2),
                           {"1.0", "2.2",  "3.33", "4",    "5",    "6",    "7.7",  "8.8",  "9.9",  "10",   "11",
                            "12",  "13.3", "14.4", "15.5", "16.2", "17",   "18.8", "19.9", "20.0", "21",   "22",
                            "23",  "24",   "25.5", "26.6", "27.7", "28.8", "29.9", "30.1", "31",   "32.5", "33.9"})
                           .column;
    testCountSerializeByteSize(col_decimal, {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                             4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 12, 33}).column;
    testCountSerialByteSizeForColumnArray(col_decimal, col_offsets, {4, 8, 12, 6 * 4, 21 * 4});

    testSerializeAndDeserialize(col_decimal);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnString)
try
{
    auto col_string = createColumn<String>({"123",
                                            "1234567890",
                                            "4567",
                                            "1234567890",
                                            "8901",
                                            "1234567890",
                                            "123456789012",
                                            "234567",
                                            "12345678",
                                            "123456",
                                            "123456789012",
                                            "12345678901234567",
                                            "12345678901234",
                                            "123456789012",
                                            "123456789",
                                            "12345678901234567890"})
                          .column;
    testCountSerializeByteSize(
        col_string,
        {8 + 4,
         8 + 11,
         8 + 5,
         8 + 11,
         8 + 5,
         8 + 11,
         8 + 13,
         8 + 7,
         8 + 9,
         8 + 7,
         8 + 13,
         8 + 18,
         8 + 15,
         8 + 13,
         8 + 10,
         8 + 21});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6, 10, 16}).column;
    testCountSerialByteSizeForColumnArray(col_string, col_offsets, {8 + 4, 16 + 16, 24 + 27, 32 + 36, 48 + 90});

    testSerializeAndDeserialize(col_string);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnFixedString)
try
{
    auto col_fixed_string_mut = ColumnFixedString::create(2);
    col_fixed_string_mut->insertData("a", 1);
    col_fixed_string_mut->insertData("b", 1);
    col_fixed_string_mut->insertData("c", 1);
    col_fixed_string_mut->insertData("d", 1);
    col_fixed_string_mut->insertData("e", 1);
    col_fixed_string_mut->insertData("ff", 2);
    ColumnPtr col_fixed_string = std::move(col_fixed_string_mut);
    testCountSerializeByteSize(col_fixed_string, {2, 2, 2, 2, 2, 2});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerialByteSizeForColumnArray(col_fixed_string, col_offsets, {2, 4, 6});

    testSerializeAndDeserialize(col_fixed_string);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnNullable)
try
{
    auto col_nullable_vec = createNullableColumn<UInt64>({1, 2, 3, 4, 5, 6}, {0, 1, 0, 1, 0, 1}).column;
    testCountSerializeByteSize(col_nullable_vec, {9, 9, 9, 9, 9, 9});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerialByteSizeForColumnArray(col_nullable_vec, col_offsets, {9, 18, 27});

    testSerializeAndDeserialize(col_nullable_vec);

    auto col_nullable_string
        = createNullableColumn<String>({"123", "2", "34", "456", "5678", "6"}, {0, 1, 0, 1, 0, 1}).column;
    testCountSerializeByteSize(col_nullable_string, {9 + 4, 9 + 1, 9 + 3, 9 + 1, 9 + 5, 9 + 1});
    testCountSerialByteSizeForColumnArray(col_nullable_string, col_offsets, {9 + 4, 18 + 4, 27 + 7});

    testSerializeAndDeserialize(col_nullable_string);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnArray)
try
{
    auto col_vector = createColumn<Float32>({1.0, 2.2, 3.3, 4.4, 5.5, 6.1}).column;
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    auto col_array_vec = ColumnArray::create(col_vector, col_offsets);
    testCountSerializeByteSize(col_array_vec, {8 + 4, 8 + 8, 8 + 12});
    testSerializeAndDeserialize(col_array_vec);

    auto col_string = createColumn<String>({"123", "2", "34", "456", "5678", "6"}).column;
    auto col_array_string = ColumnArray::create(col_string, col_offsets);
    testCountSerializeByteSize(col_array_string, {8 + 8 + 4, 8 + 16 + 5, 8 + 24 + 11});
    testSerializeAndDeserialize(col_array_string);

    auto col_nullable_string
        = createNullableColumn<String>({"123", "2", "34", "456", "5678", "6"}, {0, 1, 0, 1, 0, 1}).column;
    auto col_array_nullable_string = ColumnArray::create(col_nullable_string, col_offsets);
    testCountSerializeByteSize(col_array_nullable_string, {8 + 9 + 4, 8 + 18 + 4, 8 + 27 + 7});
    testSerializeAndDeserialize(col_array_nullable_string);
}
CATCH

TEST_F(TestColumnSerializeDeserialize, TestColumnTuple)
try
{
    auto col_tuple = ColumnTuple::create(
        {createColumn<UInt64>({1, 2, 3, 4, 5, 6}).column,
         createColumn<String>({"123", "2", "34", "456", "5678", "6"}).column});
    testCountSerializeByteSize(col_tuple, {8 + 8 + 4, 8 + 8 + 2, 8 + 8 + 3, 8 + 8 + 4, 8 + 8 + 5, 8 + 8 + 2});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerialByteSizeForColumnArray(col_tuple, col_offsets, {8 + 8 + 4, 16 + 16 + 5, 24 + 24 + 11});

    testSerializeAndDeserialize(col_tuple);
}
CATCH

} // namespace tests
} // namespace DB
