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
#include <tipb/expression.pb.h>

namespace DB
{
namespace tests
{

struct MetaData
{
    UInt32 version{};
    UInt64 grouping_id{};
    std::set<UInt64> grouping_ids;
};

tipb::Expr buildTiPBExpr(const MetaData & meta_data)
{
    tipb::Expr expr;
    auto * grouping_meta = expr.mutable_groupingmeta();
    grouping_meta->set_version(meta_data.version);
    if (meta_data.version == 1 || meta_data.version == 2)
    {
        grouping_meta->add_grouping_ids(meta_data.grouping_id);
    }
    else
    {
        for (auto grouping_id : meta_data.grouping_ids)
            grouping_meta->add_grouping_ids(grouping_id);
    }
    return expr;
}

class TestGrouping : public DB::tests::FunctionTest
{
protected:
    const String func_name = "grouping";
};

TEST_F(TestGrouping, TestVersion1)
try
{
    MetaData meta_data;
    meta_data.version = 1;
    const TiDB::TiDBCollatorPtr collator = nullptr;

    // const
    {
        std::vector<UInt64> grouping_id{1, 1, 1, 4, 4, 4, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 3, 1, 2, 4, 6};
        std::vector<UInt64> expect{1, 0, 1, 0, 0, 1, 1};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    collator,
                    false,
                    &expr));
        }
    }

    // vector
    {
        std::vector<UInt64> grouping_id{1, 4};
        std::vector<UInt64> meta_grouping_id{1, 2, 3, 4, 5, 6, 7};
        std::vector<std::vector<UInt64>> expects{
            {1, 0},
            {0, 0},
            {1, 0},
            {0, 1},
            {1, 1},
            {0, 1},
            {1, 1}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
        }
    }

    // nullable
    {
        std::vector<std::optional<UInt64>> grouping_id{1, 4, {}};
        std::vector<UInt64> meta_grouping_id{1, 2, 3, 4, 5, 6, 7};
        std::vector<std::vector<std::optional<UInt64>>> expects{
            {1, 0, {}},
            {0, 0, {}},
            {1, 0, {}},
            {0, 1, {}},
            {1, 1, {}},
            {0, 1, {}},
            {1, 1, {}}};

        for (size_t i = 0; i < expects.size(); ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
        }
    }
}
CATCH

TEST_F(TestGrouping, TestVersion2)
try
{
    MetaData meta_data;
    meta_data.version = 2;
    const TiDB::TiDBCollatorPtr collator = nullptr;

    // const
    {
        std::vector<UInt64> grouping_id{0, 0, 1, 1, 1, 2, 2};
        std::vector<UInt64> meta_grouping_id{0, 1, 0, 1, 2, 1, 2};
        std::vector<UInt64> expect{0, 0, 1, 0, 0, 1, 0};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_id = meta_grouping_id[i];
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    collator,
                    false,
                    &expr));
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
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
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
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
        }
    }
}
CATCH

TEST_F(TestGrouping, TestVersion3)
try
{
    MetaData meta_data;
    meta_data.version = 3;
    const TiDB::TiDBCollatorPtr collator = nullptr;

    // const
    {
        std::vector<UInt64> grouping_id{2, 2, 2, 2};
        std::vector<std::set<UInt64>> meta_grouping_ids{{0, 2}, {2}, {3}, {1, 3}};
        std::vector<UInt64> expect{0, 0, 1, 1};

        size_t case_num = grouping_id.size();
        for (size_t i = 0; i < case_num; ++i)
        {
            meta_data.grouping_ids = meta_grouping_ids[i];
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createConstColumn<UInt8>(1, expect[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createConstColumn<UInt64>(1, grouping_id[i])},
                    collator,
                    false,
                    &expr));
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
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<UInt8>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<UInt64>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
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
            tipb::Expr expr = buildTiPBExpr(meta_data);
            ASSERT_COLUMN_EQ(
                createColumn<Nullable<UInt8>>(expects[i]),
                executeFunction(
                    func_name,
                    std::vector<ColumnWithTypeAndName>{createColumn<Nullable<UInt64>>(grouping_id)},
                    nullptr,
                    false,
                    &expr));
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
