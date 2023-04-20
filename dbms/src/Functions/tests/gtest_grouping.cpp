// Copyright 2023 PingCAP, Ltd.
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
    UInt64 grouping_id{};
    std::set<UInt64> grouping_ids;
};

FuncMetaData buildFuncMetaData(const MetaData & meta_data)
{
    FuncMetaData func_meta;
    tipb::GroupingFunctionMetadata grouping_meta;
    grouping_meta.set_mode(meta_data.mode);
    if (meta_data.mode == tipb::GroupingMode::ModeBitAnd || meta_data.mode == tipb::GroupingMode::ModeNumericCmp)
    {
        grouping_meta.add_grouping_marks(meta_data.grouping_id);
    }
    else
    {
        for (auto grouping_id : meta_data.grouping_ids)
            grouping_meta.add_grouping_marks(grouping_id);
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
    FuncMetaData func_meta;
    meta_data.mode = tipb::GroupingMode::ModeBitAnd;

    // const
    {
        std::vector<UInt64> grouping_id{1, 1, 1, 4, 4, 4, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 2, 1, 2, 4, 8};
        std::vector<UInt64> expect{1, 0, 0, 0, 0, 1, 0};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
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
        std::vector<std::vector<UInt64>> expects{
            {1, 0},
            {0, 0},
            {0, 1},
            {0, 0}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
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
        std::vector<std::vector<std::optional<UInt64>>> expects{
            {1, 0, {}},
            {0, 0, {}},
            {0, 1, {}},
            {0, 0, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
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
        std::vector<UInt64> expect{0, 0, 1, 0, 0, 1, 0};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
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
        std::vector<std::vector<UInt64>> expects{
            {1, 1},
            {0, 1},
            {0, 1},
            {0, 0},
            {0, 0},
            {0, 0},
            {0, 0}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
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
        std::vector<std::vector<std::optional<UInt64>>> expects{
            {1, 1, {}},
            {0, 1, {}},
            {0, 1, {}},
            {0, 0, {}},
            {0, 0, {}},
            {0, 0, {}},
            {0, 0, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
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
            meta_data.grouping_ids = meta_grouping_ids[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
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
        std::vector<std::vector<UInt64>> expects{
            {1, 0, 1, 1},
            {1, 1, 0, 1},
            {1, 0, 0, 1},
            {0, 1, 0, 1}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_ids = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
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
            meta_data.grouping_ids = meta_grouping_id[i];
            FuncMetaData func_meta = buildFuncMetaData(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
                executeFunctionWithMetaData(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    func_meta,
                    nullptr));
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
