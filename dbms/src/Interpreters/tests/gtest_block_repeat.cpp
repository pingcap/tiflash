// Copyright 2022 PingCAP, Ltd.
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
#include <Interpreters/sortBlock.h>
#include <TestUtils/FunctionTestUtils.h>
#include <Interpreters/Repeat.h>

namespace DB
{
namespace tests
{

class BlockRepeat : public ::testing::Test
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColInt64Type = typename TypeTraits<Int64>::FieldType;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt64 = std::vector<ColInt64Type>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

    const String single_col_name{"single_col"};
    const ColumnWithString col0_ori{"col0-1  ", "col0-7", "col0-0    ", "col0-3", "col0-4", "col0-6", "col0-2 ", "col0-5"};
    const std::vector<String> col_name{"age", "gender", "country", "region", "zip"};
};

TEST_F(BlockRepeat, Limit)
try
{
    {
        // test basic block repeat operation. (two grouping set)
        const ColumnsWithTypeAndName
            ori_col
            = {
                toVec<Int64>(col_name[0], ColumnWithInt64{1, 0, -1}),
                toVec<String>(col_name[1], ColumnWithString{"1   ", "1  ", "1 "}),
                toVec<String>(col_name[2], ColumnWithString{"1", "2", "3"}),
                toVec<UInt64>(col_name[3], ColumnWithUInt64{1, 1, 0}),
            };
        // group set<gender>, group set<country>
        GroupingSet g_gender = GroupingSet{GroupingColumnNames{col_name[1]}};
        GroupingSet g_country = GroupingSet{GroupingColumnNames{col_name[2]}};
        GroupingSets group_sets = GroupingSets{g_gender, g_country};
        Repeat repeat = Repeat(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        repeat.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto repeat_rows = block.rows();
        auto grouping_set_num = repeat.getGroupSetNum();
        ASSERT_EQ(origin_rows * grouping_set_num, repeat_rows); // 6
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
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            ASSERT_EQ(col_0->getElement(i), res0[i]);
        }

        const auto res1 = ColumnWithString{"1   ", "null", "1  ", "null", "1 ", "null"};
        const auto * col_1 = typeid_cast<const ColumnNullable *>(block.getColumns()[1].get());
        const auto * col_1_nest = &static_cast<const ColumnString &>(col_1->getNestedColumn());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            if (res1[i] == "null") {
                ASSERT_EQ(col_1->isNullAt(i), true);
            } else {
                ASSERT_EQ(col_1_nest->getDataAt(i), res1[i]);
            }
        }

        const auto res2 = ColumnWithString{"null", "1", "null", "2", "null", "3"};
        const auto * col_2 = typeid_cast<const ColumnNullable *>(block.getColumns()[2].get());
        const auto * col_2_nest = &static_cast<const ColumnString &>(col_2->getNestedColumn());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            if (res2[i] == "null") {
                ASSERT_EQ(col_2->isNullAt(i), true);
            } else {
                ASSERT_EQ(col_2_nest->getDataAt(i), res2[i]);
            }
        }

        const auto res3 = ColumnWithUInt64{1, 1,1,1, 0,0};
        const auto * col_3 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[3].get());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            ASSERT_EQ(col_3->getElement(i), res3[i]);
        }

        const auto res4 = ColumnWithUInt64{1, 2, 1, 2, 1, 2};
        const auto * col_4 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[4].get());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            ASSERT_EQ(col_4->getElement(i), res4[i]);
        }
    }
    {
        // test block repeat operation for multi grouping set (triple here)
        const ColumnsWithTypeAndName
            ori_col
            = {
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
        Repeat repeat = Repeat(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        repeat.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto repeat_rows = block.rows();
        auto grouping_set_num = repeat.getGroupSetNum();
        ASSERT_EQ(origin_rows * grouping_set_num, repeat_rows); // 9
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
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            ASSERT_EQ(col_0->getElement(i), res0[i]);
        }

        const auto res1 = ColumnWithString{"aaa", "null", "null", "bbb", "null", "null", "ccc", "null", "null"};
        const auto * col_1 = typeid_cast<const ColumnNullable *>(block.getColumns()[1].get());
        const auto * col_1_nest = &static_cast<const ColumnString &>(col_1->getNestedColumn());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            if (res1[i] == "null") {
                ASSERT_EQ(col_1->isNullAt(i), true);
            } else {
                ASSERT_EQ(col_1_nest->getDataAt(i), res1[i]);
            }
        }

        const auto res2 = ColumnWithString{"null", "1", "null", "null", "2", "null", "null", "3", "null"};
        const auto * col_2 = typeid_cast<const ColumnNullable *>(block.getColumns()[2].get());
        const auto * col_2_nest = &static_cast<const ColumnString &>(col_2->getNestedColumn());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            if (res2[i] == "null") {
                ASSERT_EQ(col_2->isNullAt(i), true);
            } else {
                ASSERT_EQ(col_2_nest->getDataAt(i), res2[i]);
            }
        }

        // use UInt64(-1) to represent null.
        const auto res3 = ColumnWithUInt64{UInt64(-1), UInt64(-1), 1, UInt64(-1), UInt64(-1), 1, UInt64(-1), UInt64(-1), 0};
        const auto * col_3 = typeid_cast<const ColumnNullable *>(block.getColumns()[3].get());
        const auto * col_3_nest = &typeid_cast<const ColumnUInt64 &>(col_3->getNestedColumn());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            if (res3[i] == UInt64(-1)) {
                ASSERT_EQ(col_3->isNullAt(i), true);
            } else {
                ASSERT_EQ(col_3_nest->getElement(i), res3[i]);
            }
        }

        const auto res4 = ColumnWithUInt64{1, 2, 3, 1, 2, 3, 1, 2, 3};
        const auto * col_4 = typeid_cast<const ColumnUInt64 *>(block.getColumns()[4].get());
        for (int i = 0; i < int(repeat_rows); ++i)
        {
            ASSERT_EQ(col_4->getElement(i), res4[i]);
        }
    }
    {
        /// test a empty block
        const ColumnsWithTypeAndName
            ori_col
            = {
                toVec<Int64>(col_name[0], ColumnWithInt64{}),  // without data.
                toVec<String>(col_name[1], ColumnWithString{}),
                toVec<String>(col_name[2], ColumnWithString{}),
                toVec<UInt64>(col_name[3], ColumnWithUInt64{}),
            };
        // group set<gender>, group set<country>
        GroupingSet g_gender = GroupingSet{GroupingColumnNames{col_name[1]}};
        GroupingSet g_country = GroupingSet{GroupingColumnNames{col_name[2]}};
        GroupingSet g_region = GroupingSet{GroupingColumnNames{col_name[3]}};
        GroupingSets group_sets = GroupingSets{g_gender, g_country, g_region};
        Repeat repeat = Repeat(group_sets);
        Block block(ori_col);
        auto origin_rows = block.rows();

        repeat.replicateAndFillNull(block);
        // assert the col size is added with 1.
        ASSERT_EQ(block.getColumns().size(), size_t(5));
        // assert the new col groupingID is appended.
        ASSERT_EQ(block.getColumnsWithTypeAndName()[4].name, "groupingID");
        // assert the block size is equal to origin rows * grouping set num.
        auto repeat_rows = block.rows();
        auto grouping_set_num = repeat.getGroupSetNum();
        ASSERT_EQ(origin_rows, 0);
        ASSERT_EQ(origin_rows * grouping_set_num, repeat_rows); // 0
        // assert grouping set column are nullable.
    }
}
CATCH

} // namespace tests
} // namespace DB
