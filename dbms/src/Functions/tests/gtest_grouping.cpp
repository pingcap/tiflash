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

#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <tipb/metadata.pb.h>

namespace DB
{
namespace tests
{

struct MetaData
{
    tipb::GroupingMode mode{};
    std::vector<UInt64> grouping_id;
    std::vector<std::set<UInt64>> grouping_ids;
};

FuncMetaData buildFuncMetaData(const MetaData & meta_data)
{
    FuncMetaData func_meta;
    tipb::GroupingFunctionMetadata grouping_meta;
    grouping_meta.set_mode(meta_data.mode);
    if (meta_data.mode == tipb::GroupingMode::ModeBitAnd || meta_data.mode == tipb::GroupingMode::ModeNumericCmp)
    {
        for (auto one_id : meta_data.grouping_id)
        {
            tipb::GroupingMark * grouping_mark = grouping_meta.add_grouping_marks();
            grouping_mark->add_grouping_nums(one_id);
        }
    }
    else
    {
        for (const auto & one_grouping_mark : meta_data.grouping_ids)
        {
            tipb::GroupingMark * grouping_mark = grouping_meta.add_grouping_marks();
            for (auto one_num : one_grouping_mark)
            {
                grouping_mark->add_grouping_nums(one_num);
            }
        }
    }

    func_meta.val = grouping_meta.SerializeAsString();
    return func_meta;
}

class TestGrouping : public DB::tests::FunctionTest
{
protected:
    const String func_name = "grouping";
};

TEST_F(TestGrouping, ModeBitAnd)
try
{
    MetaData meta_data;
    meta_data.mode = tipb::GroupingMode::ModeBitAnd;

    // const
    {
        std::vector<UInt64> grouping_id{1, 1, 1, 4, 4, 4, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 2, 1, 2, 4, 8};
        std::vector<UInt64> expect{0, 1, 1, 1, 1, 0, 1};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt64>(1, expect[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    func_meta,
                    nullptr));
        }
    }

    // vector
    {
        std::vector<UInt64> grouping_id{1, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 4, 8};
        std::vector<std::vector<UInt64>> expects{{0, 1}, {1, 1}, {1, 0}, {1, 1}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt64>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }

    // nullable
    {
        std::vector<std::optional<UInt64>> grouping_id{1, 4, {}};
        std::vector<UInt64> meta_grouping_id{1, 2, 4, 8};
        std::vector<std::vector<std::optional<UInt64>>> expects{{0, 1, {}}, {1, 1, {}}, {1, 0, {}}, {1, 1, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt64>>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }
}
CATCH

TEST_F(TestGrouping, ModeNumericCmp)
try
{
    MetaData meta_data;
    meta_data.mode = tipb::GroupingMode::ModeNumericCmp;

    // const
    {
        std::vector<UInt64> grouping_id{0, 0, 1, 1, 1, 2, 2};
        std::vector<UInt64> meta_grouping_id{0, 1, 0, 1, 2, 1, 2};
        std::vector<UInt64> expect{1, 1, 0, 1, 1, 0, 1};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt64>(1, expect[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    func_meta,
                    nullptr));
        }
    }

    // vector
    {
        std::vector<UInt64> grouping_id{2, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 3, 4, 5, 6, 7};
        std::vector<std::vector<UInt64>> expects{{0, 0}, {1, 0}, {1, 0}, {1, 1}, {1, 1}, {1, 1}, {1, 1}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt64>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }

    // nullable
    {
        std::vector<std::optional<UInt64>> grouping_id{2, 4, {}};
        std::vector<UInt64> meta_grouping_id{1, 2, 3, 4, 5, 6, 7};
        std::vector<std::vector<std::optional<UInt64>>>
            expects{{0, 0, {}}, {1, 0, {}}, {1, 0, {}}, {1, 1, {}}, {1, 1, {}}, {1, 1, {}}, {1, 1, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = std::vector<UInt64>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt64>>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }
}
CATCH

TEST_F(TestGrouping, ModeNumericSet)
try
{
    MetaData meta_data;
    meta_data.mode = tipb::GroupingMode::ModeNumericSet;

    // const
    {
        std::vector<UInt64> grouping_id{2, 2, 2, 2};
        std::vector<std::set<UInt64>> meta_grouping_ids{{0, 2}, {2}, {3}, {1, 3}};
        std::vector<UInt64> expect{0, 0, 1, 1};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_ids = std::vector<std::set<UInt64>>{meta_grouping_ids[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt64>(1, expect[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    func_meta,
                    nullptr));
        }
    }

    // vector
    {
        std::vector<UInt64> grouping_id{1, 2, 3, 4};
        std::vector<std::set<UInt64>> meta_grouping_id{{2}, {3}, {2, 3}, {1, 3}};
        std::vector<std::vector<UInt64>> expects{{1, 0, 1, 1}, {1, 1, 0, 1}, {1, 0, 0, 1}, {0, 1, 0, 1}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_ids = std::vector<std::set<UInt64>>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt64>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }

    // nullable
    {
        std::vector<std::optional<UInt64>> grouping_id{1, 2, 3, 4, {}};
        std::vector<std::set<UInt64>> meta_grouping_id{{2}, {3}, {2, 3}, {1, 3}};
        std::vector<std::vector<std::optional<UInt64>>> expects{
            {1, 0, 1, 1, {}},
            {1, 1, 0, 1, {}},
            {1, 0, 0, 1, {}},
            {0, 1, 0, 1, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_ids = std::vector<std::set<UInt64>>{meta_grouping_id[i]};
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt64>>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }
}
CATCH

TEST_F(TestGrouping, MultiGroupingMeta)
try
{
    // one dimension grouping meta: groupingMark and grouping_ids
    typedef std::vector<std::set<UInt64>> GroupingMark;
    typedef std::vector<UInt64> GroupingIDs;

    // BitAnd
    {
        MetaData meta_data;
        meta_data.mode = tipb::GroupingMode::ModeBitAnd;

        // when grouping id is 0, it means all the grouping col as filled null and grouped. (= 1)
        // every grouping mark & 0 will get 0, deriving 1 in the result.
        std::vector<UInt64> grouping_id{2, 1, 3, 0};
        std::vector<GroupingIDs> meta_grouping_id{{0, 1}, {1, 2}, {1, 2}, {2, 1}};
        // res: 11; 01; 00; 11
        std::vector<UInt64> expect{3, 1, 0, 3};
        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt64>(1, expect[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    func_meta,
                    nullptr));
        }
    }
    // NumericCmp
    {
        MetaData meta_data;
        meta_data.mode = tipb::GroupingMode::ModeNumericCmp;

        std::vector<UInt64> grouping_id{2, 2, 1, 3};
        // when gid > id means the col is needed, not be filling with null and grouped.(= 0)
        std::vector<GroupingIDs> meta_grouping_id{{0, 1}, {4, 1}, {4, 2}, {2, 1}};

        // res: 00; 10; 11; 00
        std::vector<UInt64> expect{0, 2, 3, 0};
        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt64>(1, expect[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    func_meta,
                    nullptr));
        }
    }
    // Numeric Set
    {
        MetaData meta_data;
        meta_data.mode = tipb::GroupingMode::ModeNumericSet;

        std::vector<UInt64> grouping_id{2, 1, 3, 0};
        std::vector<GroupingMark> meta_grouping_ids{
            {{0, 2}, {2}, {3}, {1, 3}},
            {{0, 2}, {3}, {2}, {1, 3}},
            {{0, 1}, {3}, {3}, {2, 3}},
        };
        // res:
        // 2: 0011, 0101, 1110
        // 1: 1110, 1110, 0111
        // 3: 1100, 1010, 1000
        // 4: 0111, 0111, 0111
        std::vector<std::vector<UInt64>> expects{{3, 14, 12, 7}, {5, 14, 10, 7}, {14, 7, 8, 7}};
        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_ids = meta_grouping_ids[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt64>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
