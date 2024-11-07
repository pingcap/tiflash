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
    void testCountSerializeByteSize(const ColumnPtr & column_ptr, const PaddedPODArray<size_t> & result_byte_size)
    {
        PaddedPODArray<size_t> byte_size(column_ptr->size());
        for (size_t i = 0; i < column_ptr->size(); ++i)
            byte_size[i] = i;
        column_ptr->countSerializeByteSize(byte_size);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
        {
            ASSERT_EQ(byte_size[i], i + result_byte_size[i]);
        }
    }

    void testCountSerialByteSizeForColumnArray(const ColumnPtr & column_ptr, const ColumnPtr & offsets, const PaddedPODArray<size_t> & result_byte_size)
    {
        auto column_array = ColumnArray::create(column_ptr, std::move(offsets));
        PaddedPODArray<size_t> byte_size(column_array->size());
        for (size_t i = 0; i < column_array->size(); ++i)
            byte_size[i] = i;
        column_array->countSerializeByteSize(byte_size);
        ASSERT_EQ(byte_size.size(), result_byte_size.size());
        for (size_t i = 0; i < byte_size.size(); ++i)
        {
            ASSERT_EQ(byte_size[i], 8 + i + result_byte_size[i]);
        }
    }
};

TEST_F(TestColumnSerializeDeserialize, TestCountSerializeByteSize)
try
{
    auto col_vector = createColumn<UInt64>({1, 2, 3, 4, 5, 6}).column;
    testCountSerializeByteSize(col_vector, {8, 8, 8, 8, 8, 8});
    auto col_offsets = createColumn<IColumn::Offset>({1, 3, 6}).column;
    testCountSerialByteSizeForColumnArray(col_vector, col_offsets, {8, 16, 24});

    auto col_decimal = createColumn<Decimal32>(std::make_tuple(8, 2), {"1.0", "2.2", "3.33", "4", "5", "6"}).column;
    testCountSerializeByteSize(col_decimal, {4, 4, 4, 4, 4, 4});
    testCountSerialByteSizeForColumnArray(col_decimal, col_offsets, {4, 8, 12});

    auto col_string = createColumn<String>({"123", "2", "34", "456", "5678", "6"}).column;
    testCountSerializeByteSize(col_string, {8 + 4, 8 + 2, 8 + 3, 8 + 4, 8 + 5, 8 + 2});
    testCountSerialByteSizeForColumnArray(col_string, col_offsets, {8 + 4, 16 + 5, 24 + 11});

    auto col_fixed_string_mut = ColumnFixedString::create(2);
    col_fixed_string_mut->insertData("a", 1);
    col_fixed_string_mut->insertData("b", 1);
    col_fixed_string_mut->insertData("c", 1);
    col_fixed_string_mut->insertData("d", 1);
    col_fixed_string_mut->insertData("e", 1);
    col_fixed_string_mut->insertData("ff", 2);
    ColumnPtr col_fixed_string = std::move(col_fixed_string_mut);
    testCountSerializeByteSize(col_fixed_string, {2, 2, 2, 2, 2, 2});
    testCountSerialByteSizeForColumnArray(col_fixed_string, col_offsets, {2, 4, 6});

}
CATCH

} // namespace tests
} // namespace DB
