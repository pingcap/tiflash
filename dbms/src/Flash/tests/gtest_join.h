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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>

#include <ext/enumerate.h>
#include <tuple>

namespace DB
{
namespace tests
{
class JoinTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"test_db", "test_table"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toVec<String>("s", {"banana", "banana"}), toVec<String>("join_c", {"apple", "banana"})});

        context.addMockTable(
            {"test_db", "r_table_2"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toVec<String>("s", {"banana", "banana", "banana"}), toVec<String>("join_c", {"apple", "apple", "apple"})});

        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toVec<String>("s", {"banana", "banana"}), toVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver(
            "exchange_r_table",
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", "banana"}), toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver(
            "exchange_l_table",
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", "banana"}), toNullableVec<String>("join_c", {"apple", "banana"})});

        /// for ScanHashMapData test
        DB::MockColumnInfoVec left_column_infos{{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}};
        DB::MockColumnInfoVec right_column_infos{{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}};
        DB::MockColumnInfoVec right_partition_column_infos{{"a", TiDB::TP::TypeLong}};
        ColumnsWithTypeAndName left_column_data;
        ColumnsWithTypeAndName right_column_data;
        ColumnsWithTypeAndName common_column_data;
        size_t table_rows = 61440;
        size_t common_rows = 12288;
        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(left_column_infos))
        {
            ColumnGeneratorOpts opts{
                common_rows / 2,
                getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
                RANDOM,
                column_info.name};
            common_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(left_column_infos))
        {
            ColumnGeneratorOpts opts{
                table_rows - common_rows,
                getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
                RANDOM,
                column_info.name};
            left_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(right_column_infos))
        {
            ColumnGeneratorOpts opts{
                table_rows - common_rows,
                getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
                RANDOM,
                column_info.name};
            right_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (size_t i = 0; i < common_column_data.size(); ++i)
        {
            left_column_data[i].column->assumeMutable()->insertRangeFrom(
                *common_column_data[i].column,
                0,
                common_rows / 2);
            left_column_data[i].column->assumeMutable()->insertRangeFrom(
                *common_column_data[i].column,
                0,
                common_rows / 2);
            right_column_data[i].column->assumeMutable()->insertRangeFrom(
                *common_column_data[i].column,
                0,
                common_rows / 2);
            right_column_data[i].column->assumeMutable()->insertRangeFrom(
                *common_column_data[i].column,
                0,
                common_rows / 2);
        }

        ColumnWithTypeAndName shuffle_column = ColumnGenerator::instance().generate({table_rows, "UInt64", RANDOM});
        IColumn::Permutation perm;
        shuffle_column.column->getPermutation(false, 0, -1, perm);
        for (auto & column : left_column_data)
        {
            column.column = column.column->permute(perm, 0);
        }
        for (auto & column : right_column_data)
        {
            column.column = column.column->permute(perm, 0);
        }

        context.addMockTable("outer_join_test", "left_table_1_concurrency", left_column_infos, left_column_data, 1);
        context.addMockTable("outer_join_test", "left_table_3_concurrency", left_column_infos, left_column_data, 3);
        context.addMockTable("outer_join_test", "left_table_5_concurrency", left_column_infos, left_column_data, 5);
        context.addMockTable("outer_join_test", "left_table_10_concurrency", left_column_infos, left_column_data, 10);
        context.addMockTable("outer_join_test", "right_table_1_concurrency", right_column_infos, right_column_data, 1);
        context.addMockTable("outer_join_test", "right_table_3_concurrency", right_column_infos, right_column_data, 3);
        context.addMockTable("outer_join_test", "right_table_5_concurrency", right_column_infos, right_column_data, 5);
        context
            .addMockTable("outer_join_test", "right_table_10_concurrency", right_column_infos, right_column_data, 10);
        context.addExchangeReceiver(
            "right_exchange_receiver_1_concurrency",
            right_column_infos,
            right_column_data,
            1,
            right_partition_column_infos);
        context.addExchangeReceiver(
            "right_exchange_receiver_3_concurrency",
            right_column_infos,
            right_column_data,
            3,
            right_partition_column_infos);
        context.addExchangeReceiver(
            "right_exchange_receiver_5_concurrency",
            right_column_infos,
            right_column_data,
            5,
            right_partition_column_infos);
        context.addExchangeReceiver(
            "right_exchange_receiver_10_concurrency",
            right_column_infos,
            right_column_data,
            10,
            right_partition_column_infos);
    }

    ColumnsWithTypeAndName genScalarCountResults(const ColumnsWithTypeAndName & ref)
    {
        ColumnsWithTypeAndName ret;
        ret.push_back(toVec<UInt64>({ref.empty() ? 0 : ref[0].column == nullptr ? 0 : ref[0].column->size()}));
        return ret;
    }
    ColumnsWithTypeAndName genScalarCountResults(UInt64 result)
    {
        ColumnsWithTypeAndName ret;
        ret.push_back(toVec<UInt64>({result}));
        return ret;
    }

    static constexpr size_t join_type_num = 7;

    static constexpr tipb::JoinType join_types[join_type_num] = {
        tipb::JoinType::TypeInnerJoin,
        tipb::JoinType::TypeLeftOuterJoin,
        tipb::JoinType::TypeRightOuterJoin,
        tipb::JoinType::TypeSemiJoin,
        tipb::JoinType::TypeAntiSemiJoin,
        tipb::JoinType::TypeLeftOuterSemiJoin,
        tipb::JoinType::TypeAntiLeftOuterSemiJoin,
    };
};

} // namespace tests
} // namespace DB
