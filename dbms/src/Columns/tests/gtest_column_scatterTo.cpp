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
class TestColumnScatterTo : public ::testing::Test
{
public:
    void compareColumn(
        const ColumnWithTypeAndName & expected_col_with_type_name,
        const ColumnWithTypeAndName & actual_col_with_type_name)
    {
        const ColumnPtr expected = expected_col_with_type_name.column;
        const ColumnPtr actual = actual_col_with_type_name.column;
        if unlikely (typeid_cast<const ColumnSet *>(expected.get()) || typeid_cast<const ColumnSet *>(actual.get()))
        {
            /// ColumnSet compares size only now, since the test ensures data is equal
            const ColumnSet * expected_set = typeid_cast<const ColumnSet *>(expected.get());
            const ColumnSet * actual_set = typeid_cast<const ColumnSet *>(actual.get());
            ASSERT_TRUE(expected_set && actual_set);
            ASSERT_TRUE(expected_set->size() == actual_set->size());
            return;
        }
        ASSERT_COLUMN_EQ(expected, actual);
    }

    void doTestWork(ColumnWithTypeAndName & col_with_type_and_name)
    {
        auto column_ptr = col_with_type_and_name.column;
        ASSERT_TRUE(rows == column_ptr->size());
        MutableColumns scatterTo_res(scattered_num_column);
        for (size_t i = 0; i < scattered_num_column; ++i)
        {
            scatterTo_res[i] = column_ptr->cloneEmpty();
        }
        IColumn::Selector selector;
        for (size_t i = 0; i < rows; ++i)
            selector.push_back(i % scattered_num_column);

        /// Apply two scatter function from zero to ensure they produce the same results
        column_ptr->scatterTo(scatterTo_res, selector);
        auto scatter_res = column_ptr->scatter(scattered_num_column, selector);
        for (size_t i = 0; i < scattered_num_column; ++i)
        {
            ColumnWithTypeAndName scatter_col(std::move(scatter_res[i]), col_with_type_and_name.type, "");
            ColumnWithTypeAndName scatterTo_col(std::move(scatterTo_res[i]), col_with_type_and_name.type, "");
            compareColumn(scatter_col, scatterTo_col);
        }

        /// Apply scatterTo twice sequentially
        scatterTo_res.clear();
        for (size_t i = 0; i < scattered_num_column; i++)
        {
            scatterTo_res.push_back(column_ptr->cloneEmpty());
        }
        column_ptr->scatterTo(scatterTo_res, selector);
        column_ptr->scatterTo(scatterTo_res, selector);
        scatter_res = column_ptr->scatter(scattered_num_column, selector);
        for (size_t i = 0; i < scattered_num_column; ++i)
        {
            scatter_res[i]->insertRangeFrom(*scatter_res[i], 0, scatter_res[i]->size());
            ColumnWithTypeAndName scatter_col(std::move(scatter_res[i]), col_with_type_and_name.type, "");
            ColumnWithTypeAndName scatterTo_col(std::move(scatterTo_res[i]), col_with_type_and_name.type, "");
            compareColumn(scatter_col, scatterTo_col);
        }
    }
    const size_t scattered_num_column = 3;
    const size_t rows = 6;
};

TEST_F(TestColumnScatterTo, TestColumnDecimal)
try
{
    using Native = typename Decimal32::NativeType;
    using FieldType = DecimalField<Decimal32>;

    auto col_with_type_and_name = createColumn<Decimal32>(
        std::make_tuple(9, 4),
        {FieldType(static_cast<Native>(1), 4),
         FieldType(static_cast<Native>(2), 4),
         FieldType(static_cast<Native>(3), 4),
         FieldType(static_cast<Native>(4), 4),
         FieldType(static_cast<Native>(5), 4),
         FieldType(static_cast<Native>(6), 4)});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnNumber)
try
{
    auto col_with_type_and_name = createColumn<UInt64>({1, 2, 3, 4, 5, 6});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnString)
try
{
    auto col_with_type_and_name = createColumn<String>({"1", "2", "3", "4", "5", "6"});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnFixedString)
try
{
    auto col = ColumnFixedString::create(2);
    col->insertData("a", 1);
    col->insertData("b", 1);
    col->insertData("c", 1);
    col->insertData("d", 1);
    col->insertData("e", 1);
    col->insertData("f", 1);
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnNullableAndVector)
try
{
    auto col_with_type_and_name = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
        {MyDateTime(2020, 1, 10, 0, 0, 0, 0).toPackedUInt(),
         MyDateTime(2020, 2, 10, 0, 0, 0, 0).toPackedUInt(),
         MyDateTime(2020, 3, 10, 0, 0, 0, 0).toPackedUInt(),
         {}, // Null
         MyDateTime(2020, 4, 10, 0, 0, 0, 0).toPackedUInt(),
         MyDateTime(2020, 5, 10, 0, 0, 0, 0).toPackedUInt()});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnConst)
try
{
    auto col_with_type_and_name = createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
        6,
        {MyDateTime(2020, 2, 10, 0, 0, 0, 0).toPackedUInt()});
    doTestWork(col_with_type_and_name);

    /// Const Null
    col_with_type_and_name = createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(6, {});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnNothing)
try
{
    auto col = ColumnNothing::create(6);
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnSet)
try
{
    const SetPtr & set = nullptr;
    auto col = ColumnSet::create(6, set);
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnArray)
try
{
    auto nested_col = createColumn<UInt64>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}).column;
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert(2);
    offsets->insert(4);
    offsets->insert(6);
    offsets->insert(8);
    offsets->insert(10);
    offsets->insert(12);
    auto col = ColumnArray::create(nested_col, std::move(offsets));
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnScatterTo, TestColumnTuple)
try
{
    auto string_col = createColumn<String>({"1", "2", "3", "4", "5", "6"}).column;
    auto int_col = createColumn<UInt64>({1, 2, 3, 4, 5, 6}).column;
    MutableColumns mutableColumns;
    mutableColumns.push_back(string_col->assumeMutable());
    mutableColumns.push_back(int_col->assumeMutable());
    auto col = ColumnTuple::create(std::move(mutableColumns));
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

} // namespace tests
} // namespace DB