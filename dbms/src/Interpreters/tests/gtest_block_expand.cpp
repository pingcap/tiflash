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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Expand.h>
#include <Interpreters/sortBlock.h>
#include <TestUtils/FunctionTestUtils.h>

namespace DB
{
namespace tests
{

class BlockExpand : public ::testing::Test
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColInt64Type = typename TypeTraits<Int64>::FieldType;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt64 = std::vector<ColInt64Type>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

    const String single_col_name{"single_col"};
    const ColumnWithString
        col0_ori{"col0-1  ", "col0-7", "col0-0    ", "col0-3", "col0-4", "col0-6", "col0-2 ", "col0-5"};
    const std::vector<String> col_name{"age", "gender", "country", "region", "zip"};
};

/// todo: for some column overlapping in different grouping set, we should copy the overlapped column as a new column
///  and the upper layer OP's computation should be shifted and based on the new one's id. Need a plan side control,
///  tiflash side is ready to go.
///
///  just a overlapped case for grouping set 1: <{a}>, grouping set 2: <{a,b}>
///  count(distinct a) and count(distinct a, b) planner will clone a new column from a for either one of them
///  count(distinct a') and count(distinct a, b) = need a more column a' here (maybe from lower projection or something)
///  then, according a' and a,b 's index offset in PB, to describe the grouping set definition.
///  when targeting a' 's replicate group, fill a and b as null in the group.
///  when targeting a and b 's replicate group, fill a' as null in the group.
TEST_F(BlockExpand, ExpandLogic4Overlap)
try
{
    {
        // test basic block expand operation. (two grouping set: {<age>}, {<gender, country>})
        const ColumnsWithTypeAndName ori_col = {
            toVec<Int64>(col_name[0], ColumnWithInt64{1, 0, -1}),
            toVec<String>(col_name[1], ColumnWithString{"1   ", "1  ", "1 "}),
            toVec<String>(col_name[2], ColumnWithString{"1", "2", "3"}),
            toVec<UInt64>(col_name[3], ColumnWithUInt64{1, 1, 0}),
        };
        // group set<gender>, group set<country>
        GroupingSet g_age = GroupingSet{GroupingColumnNames{col_name[0]}};
        GroupingSet g_gender_country = GroupingSet{GroupingColumnNames{col_name[1], col_name[2]}};
        GroupingSets group_sets = GroupingSets{g_age, g_gender_country};
        Expand expand = Expand(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        expand.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto expand_rows = block.rows();
        auto grouping_set_num = 2;
        ASSERT_EQ(origin_rows * grouping_set_num, expand_rows); // 6
        // assert grouping set column are nullable.
        ASSERT_EQ(block.getColumns()[0].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[1].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[2].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[3].get()->isColumnNullable(), false);
        ASSERT_EQ(block.getColumns()[4].get()->isColumnNullable(), false);

        // assert the rows layout
        //          "age", "gender", "country", "region", "groupingID"
        //  ori_col   1      null       null       1         1
        //  rpt_col  null    "1   "     "1"        1         2
        //
        //  ori_col   0      null       null       1         1
        //  rpt_col  null    "1  "      "2"        1         2
        //
        //  ori_col  -1      null       null       0         1
        //  rpt_col  null    "1 "       "3"        0         2
        const auto num4_null = 100;
        const auto res0 = ColumnWithInt64{1, num4_null, 0, num4_null, -1, num4_null};
        const auto * col_0 = typeid_cast<const ColumnNullable *>(block.getColumns()[0].get());
        const auto * col_0_nest = &static_cast<const ColumnInt64 &>(col_0->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res0[i] == num4_null)
            {
                ASSERT_EQ(col_0->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_0_nest->getElement(i), res0[i]);
            }
        }

        const auto res1 = ColumnWithString{"null", "1   ", "null", "1  ", "null", "1 "};
        const auto * col_1 = typeid_cast<const ColumnNullable *>(block.getColumns()[1].get());
        const auto * col_1_nest = &static_cast<const ColumnString &>(col_1->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res1[i] == "null")
            {
                ASSERT_EQ(col_1->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_1_nest->getDataAt(i), res1[i]);
            }
        }

        const auto res2 = ColumnWithString{"null", "1", "null", "2", "null", "3"};
        const auto * col_2 = typeid_cast<const ColumnNullable *>(block.getColumns()[2].get());
        const auto * col_2_nest = &static_cast<const ColumnString &>(col_2->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res2[i] == "null")
            {
                ASSERT_EQ(col_2->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_2_nest->getDataAt(i), res2[i]);
            }
        }

        const auto res3 = ColumnWithUInt64{1, 1, 1, 1, 0, 0};
        const auto * col_3 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[3].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_3->getElement(i), res3[i]);
        }

        const auto res4 = ColumnWithUInt64{1, 2, 1, 2, 1, 2};
        const auto * col_4 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[4].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_4->getElement(i), res4[i]);
        }
    }
}
CATCH

TEST_F(BlockExpand, ExpandLogic)
try
{
    {
        // test basic block expand operation. (two grouping set)
        const ColumnsWithTypeAndName ori_col = {
            toVec<Int64>(col_name[0], ColumnWithInt64{1, 0, -1}),
            toVec<String>(col_name[1], ColumnWithString{"1   ", "1  ", "1 "}),
            toVec<String>(col_name[2], ColumnWithString{"1", "2", "3"}),
            toVec<UInt64>(col_name[3], ColumnWithUInt64{1, 1, 0}),
        };
        // group set<gender>, group set<country>
        GroupingSet g_gender = GroupingSet{GroupingColumnNames{col_name[1]}};
        GroupingSet g_country = GroupingSet{GroupingColumnNames{col_name[2]}};
        GroupingSets group_sets = GroupingSets{g_gender, g_country};
        Expand expand = Expand(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        expand.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto expand_rows = block.rows();
        auto grouping_set_num = 2;
        ASSERT_EQ(origin_rows * grouping_set_num, expand_rows); // 6
        // assert grouping set column are nullable.
        ASSERT_EQ(block.getColumns()[0].get()->isColumnNullable(), false);
        ASSERT_EQ(block.getColumns()[1].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[2].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[3].get()->isColumnNullable(), false);
        ASSERT_EQ(block.getColumns()[4].get()->isColumnNullable(), false);

        // assert the rows layout
        //          "age", "gender", "country", "region", "groupingID"
        //  ori_col   1     "1   "    null       1         1
        //  rpt_col   1     null      "1"        1         2
        //
        //  ori_col   0     "1  "     null       1         1
        //  rpt_col   0     null      "2"        1         2
        //
        //  ori_col  -1     "1 "      null       0         1
        //  rpt_col  -1     null      "3"        0         2

        const auto res0 = ColumnWithInt64{1, 1, 0, 0, -1, -1};
        const auto * col_0 = typeid_cast<const ColumnInt64 *>(block.getColumns()[0].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_0->getElement(i), res0[i]);
        }

        const auto res1 = ColumnWithString{"1   ", "null", "1  ", "null", "1 ", "null"};
        const auto * col_1 = typeid_cast<const ColumnNullable *>(block.getColumns()[1].get());
        const auto * col_1_nest = &static_cast<const ColumnString &>(col_1->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res1[i] == "null")
            {
                ASSERT_EQ(col_1->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_1_nest->getDataAt(i), res1[i]);
            }
        }

        const auto res2 = ColumnWithString{"null", "1", "null", "2", "null", "3"};
        const auto * col_2 = typeid_cast<const ColumnNullable *>(block.getColumns()[2].get());
        const auto * col_2_nest = &static_cast<const ColumnString &>(col_2->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res2[i] == "null")
            {
                ASSERT_EQ(col_2->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_2_nest->getDataAt(i), res2[i]);
            }
        }

        const auto res3 = ColumnWithUInt64{1, 1, 1, 1, 0, 0};
        const auto * col_3 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[3].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_3->getElement(i), res3[i]);
        }

        const auto res4 = ColumnWithUInt64{1, 2, 1, 2, 1, 2};
        const auto * col_4 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[4].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_4->getElement(i), res4[i]);
        }
    }
    {
        // test block expand operation for multi grouping set (triple here)
        const ColumnsWithTypeAndName ori_col = {
            toVec<Int64>(col_name[0], ColumnWithInt64{1, 0, -1}),
            toVec<String>(col_name[1], ColumnWithString{"aaa", "bbb", "ccc"}),
            toVec<String>(col_name[2], ColumnWithString{"1", "2", "3"}),
            toVec<UInt64>(col_name[3], ColumnWithUInt64{1, 1, 0}),
        };
        // group set<gender>, group set<country>
        GroupingSet g_gender = GroupingSet{GroupingColumnNames{col_name[1]}};
        GroupingSet g_country = GroupingSet{GroupingColumnNames{col_name[2]}};
        GroupingSet g_region = GroupingSet{GroupingColumnNames{col_name[3]}};
        GroupingSets group_sets = GroupingSets{g_gender, g_country, g_region};
        Expand expand = Expand(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        expand.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto expand_rows = block.rows();
        auto grouping_set_num = 3;
        ASSERT_EQ(origin_rows * grouping_set_num, expand_rows); // 9
        // assert grouping set column are nullable.
        ASSERT_EQ(block.getColumns()[0].get()->isColumnNullable(), false);
        ASSERT_EQ(block.getColumns()[1].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[2].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[3].get()->isColumnNullable(), true);
        ASSERT_EQ(block.getColumns()[4].get()->isColumnNullable(), false);

        // assert the rows layout
        //          "age", "gender", "country", "region", "groupingID"
        //  ori_col   1     "aaa"     null      null       1
        //  rpt_col   1     null      "1"       null       2
        //  rpt_col   1     null      null       1         3
        //
        //  ori_col   0     "bbb"     null      null       1
        //  rpt_col   0     null      "2"       null       2
        //  rpt_col   0     null      null       1         3

        //  ori_col  -1     "ccc"     null      null       1
        //  rpt_col  -1     null      "3"       null       2
        //  rpt_col  -1     null      null       0         3

        const auto res0 = ColumnWithInt64{1, 1, 1, 0, 0, 0, -1, -1, -1};
        const auto * col_0 = typeid_cast<const ColumnInt64 *>(block.getColumns()[0].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_0->getElement(i), res0[i]);
        }

        const auto res1 = ColumnWithString{"aaa", "null", "null", "bbb", "null", "null", "ccc", "null", "null"};
        const auto * col_1 = typeid_cast<const ColumnNullable *>(block.getColumns()[1].get());
        const auto * col_1_nest = &static_cast<const ColumnString &>(col_1->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res1[i] == "null")
            {
                ASSERT_EQ(col_1->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_1_nest->getDataAt(i), res1[i]);
            }
        }

        const auto res2 = ColumnWithString{"null", "1", "null", "null", "2", "null", "null", "3", "null"};
        const auto * col_2 = typeid_cast<const ColumnNullable *>(block.getColumns()[2].get());
        const auto * col_2_nest = &static_cast<const ColumnString &>(col_2->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res2[i] == "null")
            {
                ASSERT_EQ(col_2->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_2_nest->getDataAt(i), res2[i]);
            }
        }

        // use UInt64(-1) to represent null.
        const auto res3 = ColumnWithUInt64{
            static_cast<UInt64>(-1),
            static_cast<UInt64>(-1),
            1,
            static_cast<UInt64>(-1),
            static_cast<UInt64>(-1),
            1,
            static_cast<UInt64>(-1),
            static_cast<UInt64>(-1),
            0};
        const auto * col_3 = typeid_cast<const ColumnNullable *>(block.getColumns()[3].get());
        const auto * col_3_nest = &typeid_cast<const ColumnUInt64 &>(col_3->getNestedColumn());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            if (res3[i] == static_cast<UInt64>(-1))
            {
                ASSERT_EQ(col_3->isNullAt(i), true);
            }
            else
            {
                ASSERT_EQ(col_3_nest->getElement(i), res3[i]);
            }
        }

        const auto res4 = ColumnWithUInt64{1, 2, 3, 1, 2, 3, 1, 2, 3};
        const auto * col_4 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[4].get());
        for (int i = 0; i < static_cast<int>(expand_rows); ++i)
        {
            ASSERT_EQ(col_4->getElement(i), res4[i]);
        }
    }
    {
        /// test a empty block
        const ColumnsWithTypeAndName ori_col = {
            toVec<Int64>(col_name[0], ColumnWithInt64{}), // without data.
            toVec<String>(col_name[1], ColumnWithString{}),
            toVec<String>(col_name[2], ColumnWithString{}),
            toVec<UInt64>(col_name[3], ColumnWithUInt64{}),
        };
        // group set<gender>, group set<country>
        GroupingSet g_gender = GroupingSet{GroupingColumnNames{col_name[1]}};
        GroupingSet g_country = GroupingSet{GroupingColumnNames{col_name[2]}};
        GroupingSet g_region = GroupingSet{GroupingColumnNames{col_name[3]}};
        GroupingSets group_sets = GroupingSets{g_gender, g_country, g_region};
        Expand expand = Expand(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        expand.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto expand_rows = block.rows();
        auto grouping_set_num = 3;
        ASSERT_EQ(origin_rows, 0);
        ASSERT_EQ(origin_rows * grouping_set_num, expand_rows); // 0
        // assert grouping set column are nullable.
    }
}
CATCH

} // namespace tests
} // namespace DB
