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
class TestColumnInsertFrom : public ::testing::Test
{
public:
    static void compareColumn(
        const ColumnWithTypeAndName & expected_col_with_type_name,
        const ColumnWithTypeAndName & actual_col_with_type_name)
    {
        const ColumnPtr expected = expected_col_with_type_name.column;
        const ColumnPtr actual = actual_col_with_type_name.column;
        if unlikely (typeid_cast<const ColumnSet *>(expected.get()) || typeid_cast<const ColumnSet *>(actual.get()))
        {
            /// ColumnSet compares size only now, since the test ensures data is equal
            const auto * expected_set = typeid_cast<const ColumnSet *>(expected.get());
            const auto * actual_set = typeid_cast<const ColumnSet *>(actual.get());
            ASSERT_TRUE(expected_set && actual_set);
            ASSERT_TRUE(expected_set->size() == actual_set->size());
            return;
        }
        ASSERT_COLUMN_EQ(expected, actual);
    }

    void doTestWork(ColumnWithTypeAndName & col_with_type_and_name) const
    {
        auto column_ptr = col_with_type_and_name.column;
        ASSERT_TRUE(rows == column_ptr->size());
        MutableColumns cols(2);
        for (size_t i = 0; i < 2; ++i)
        {
            cols[i] = column_ptr->cloneEmpty();
        }

        /// Test insertManyFrom
        {
            for (size_t i = 0; i < 3; ++i)
                cols[0]->insertFrom(*column_ptr, 1);
            for (size_t i = 0; i < 3; ++i)
                cols[0]->insertFrom(*column_ptr, 1);
            cols[1]->insertManyFrom(*column_ptr, 1, 3);
            cols[1]->insertManyFrom(*column_ptr, 1, 3);
            {
                ColumnWithTypeAndName ref(std::move(cols[0]), col_with_type_and_name.type, "");
                ColumnWithTypeAndName result(std::move(cols[1]), col_with_type_and_name.type, "");
                compareColumn(ref, result);
            }
        }

        /// Test insertSelectiveFrom
        {
            for (size_t i = 0; i < 2; ++i)
            {
                cols[i] = column_ptr->cloneEmpty();
            }
            IColumn::Offsets selective_offsets;
            selective_offsets.push_back(0);
            selective_offsets.push_back(2);
            selective_offsets.push_back(4);
            for (size_t position : selective_offsets)
                cols[0]->insertFrom(*column_ptr, position);
            for (size_t position : selective_offsets)
                cols[0]->insertFrom(*column_ptr, position);
            cols[1]->insertSelectiveFrom(*column_ptr, selective_offsets);
            cols[1]->insertSelectiveFrom(*column_ptr, selective_offsets);
            {
                ColumnWithTypeAndName ref(std::move(cols[0]), col_with_type_and_name.type, "");
                ColumnWithTypeAndName result(std::move(cols[1]), col_with_type_and_name.type, "");
                compareColumn(ref, result);
            }
        }

        /// Test insertManyDefaults
        {
            for (size_t i = 0; i < 2; ++i)
            {
                cols[i] = column_ptr->cloneEmpty();
            }
            for (size_t i = 0; i < 3; ++i)
                cols[0]->insertDefault();
            for (size_t i = 0; i < 3; ++i)
                cols[0]->insertDefault();
            cols[1]->insertManyDefaults(3);
            cols[1]->insertManyDefaults(3);
            {
                ColumnWithTypeAndName ref(std::move(cols[0]), col_with_type_and_name.type, "");
                ColumnWithTypeAndName result(std::move(cols[1]), col_with_type_and_name.type, "");
                compareColumn(ref, result);
            }
        }

        /// Test insertMany
        {
            for (size_t i = 0; i < 2; ++i)
                cols[i] = column_ptr->cloneEmpty();
            for (size_t i = 0; i < 6; ++i)
                cols[0]->insertFrom(*column_ptr, 1);
            if (unlikely(
                    typeid_cast<const ColumnNothing *>(column_ptr.get())
                    || typeid_cast<const ColumnSet *>(column_ptr.get())))
            {
                /// ColumnNothing and ColumnSet are not allowed to insertMany
                return;
            }
            auto v = (*column_ptr)[1];
            cols[1]->insertMany(v, 6);
            {
                ColumnWithTypeAndName ref(std::move(cols[0]), col_with_type_and_name.type, "");
                ColumnWithTypeAndName result(std::move(cols[1]), col_with_type_and_name.type, "");
                compareColumn(ref, result);
            }
        }
    }
    const size_t rows = 6;
};

TEST_F(TestColumnInsertFrom, TestColumnDecimal)
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

TEST_F(TestColumnInsertFrom, TestColumnNumber)
try
{
    auto col_with_type_and_name = createColumn<UInt64>({1, 2, 3, 4, 5, 6});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnInsertFrom, TestColumnString)
try
{
    auto col_with_type_and_name = createColumn<String>({"1", "2", "3", "4", "5", "6"});
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnInsertFrom, TestColumnFixedString)
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

TEST_F(TestColumnInsertFrom, TestColumnNullableAndVector)
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

TEST_F(TestColumnInsertFrom, TestColumnConst)
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

TEST_F(TestColumnInsertFrom, TestColumnNothing)
try
{
    auto col = ColumnNothing::create(6);
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnInsertFrom, TestColumnSet)
try
{
    const SetPtr & set = nullptr;
    auto col = ColumnSet::create(6, set);
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

TEST_F(TestColumnInsertFrom, TestColumnArray)
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

TEST_F(TestColumnInsertFrom, TestColumnTuple)
try
{
    auto string_col = createColumn<String>({"1", "2", "3", "4", "5", "6"}).column;
    auto int_col = createColumn<UInt64>({1, 2, 3, 4, 5, 6}).column;
    MutableColumns mutable_columns;
    mutable_columns.push_back(string_col->assumeMutable());
    mutable_columns.push_back(int_col->assumeMutable());
    auto col = ColumnTuple::create(std::move(mutable_columns));
    auto col_with_type_and_name
        = ColumnWithTypeAndName{std::move(col), std::make_shared<DataTypeString>(), String("col")};
    doTestWork(col_with_type_and_name);
}
CATCH

} // namespace tests
} // namespace DB
