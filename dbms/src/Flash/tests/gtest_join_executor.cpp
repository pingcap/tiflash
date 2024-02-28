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
class JoinExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable({"test_db", "r_table"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana"}),
                              toVec<String>("join_c", {"apple", "banana"})});

        context.addMockTable({"test_db", "r_table_2"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana", "banana"}),
                              toVec<String>("join_c", {"apple", "apple", "apple"})});

        context.addMockTable({"test_db", "l_table"},
                             {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                             {toVec<String>("s", {"banana", "banana"}),
                              toVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver("exchange_r_table",
                                    {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver("exchange_l_table",
                                    {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

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
            ColumnGeneratorOpts opts{common_rows, getDataTypeByColumnInfoForComputingLayer(column_info)->getName(), RANDOM, column_info.name};
            common_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(left_column_infos))
        {
            ColumnGeneratorOpts opts{table_rows - common_rows, getDataTypeByColumnInfoForComputingLayer(column_info)->getName(), RANDOM, column_info.name};
            left_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(right_column_infos))
        {
            ColumnGeneratorOpts opts{table_rows - common_rows, getDataTypeByColumnInfoForComputingLayer(column_info)->getName(), RANDOM, column_info.name};
            right_column_data.push_back(ColumnGenerator::instance().generate(opts));
        }

        for (size_t i = 0; i < common_column_data.size(); ++i)
        {
            left_column_data[i].column->assumeMutable()->insertRangeFrom(*common_column_data[i].column, 0, common_rows);
            right_column_data[i].column->assumeMutable()->insertRangeFrom(*common_column_data[i].column, 0, common_rows);
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
        context.addMockTable("outer_join_test", "right_table_10_concurrency", right_column_infos, right_column_data, 10);
        context.addExchangeReceiver("right_exchange_receiver_1_concurrency", right_column_infos, right_column_data, 1, right_partition_column_infos);
        context.addExchangeReceiver("right_exchange_receiver_3_concurrency", right_column_infos, right_column_data, 3, right_partition_column_infos);
        context.addExchangeReceiver("right_exchange_receiver_5_concurrency", right_column_infos, right_column_data, 5, right_partition_column_infos);
        context.addExchangeReceiver("right_exchange_receiver_10_concurrency", right_column_infos, right_column_data, 10, right_partition_column_infos);
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

TEST_F(JoinExecutorTestRunner, SimpleJoin)
try
{
    constexpr size_t simple_test_num = 4;

    context.addMockTable("simple_test", "t1", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "2", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});
    context.addMockTable("simple_test", "t2", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "3", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});

    // names of left table, right table and join key column
    const std::tuple<String, String, String> join_cases[simple_test_num] = {
        std::make_tuple("t1", "t2", "a"),
        std::make_tuple("t2", "t1", "a"),
        std::make_tuple("t1", "t2", "b"),
        std::make_tuple("t2", "t1", "b"),
    };

    const ColumnsWithTypeAndName expected_cols[simple_test_num * join_type_num] = {
        // inner join
        {toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({{}, "3", {}, "3"}), toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({"3", "3", {}, {}})},
        {toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({{}, "3", {}, "3"}), toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({"3", "3", {}, {}})},
        {toNullableVec<String>({{}, "1", "2", {}, "1"}), toNullableVec<String>({"3", "3", "4", "3", "3"}), toNullableVec<String>({"1", "1", "3", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3"})},
        {toNullableVec<String>({{}, "1", "3", {}, "1"}), toNullableVec<String>({"3", "3", "4", "3", "3"}), toNullableVec<String>({"1", "1", "2", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3"})},
        // left join
        {toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}), toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})},
        {toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}), toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})},
        {toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        {toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        // right join
        {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}), toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})},
        {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}), toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})},
        {toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        {toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})},
        // semi join
        {toNullableVec<String>({"1", "1"}), toNullableVec<String>({"3", {}})},
        {toNullableVec<String>({"1", "1"}), toNullableVec<String>({"3", {}})},
        {toNullableVec<String>({"1", "2", {}}), toNullableVec<String>({"3", "4", "3"})},
        {toNullableVec<String>({"1", "3", {}}), toNullableVec<String>({"3", "4", "3"})},
        // anti semi join
        {toNullableVec<String>({"2", {}, {}}), toNullableVec<String>({"4", "3", {}})},
        {toNullableVec<String>({"3", {}, {}}), toNullableVec<String>({"4", "3", {}})},
        {toNullableVec<String>({"1", {}}), toNullableVec<String>({{}, {}})},
        {toNullableVec<String>({"1", {}}), toNullableVec<String>({{}, {}})},
        // left outer semi join
        {toNullableVec<String>({"1", "2", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({1, 0, 0, 1, 0})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({1, 0, 0, 1, 0})},
        {toNullableVec<String>({"1", "2", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({1, 1, 1, 0, 0})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({1, 1, 1, 0, 0})},
        // anti left outer semi join
        {toNullableVec<String>({"1", "2", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({0, 1, 1, 0, 1})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({0, 1, 1, 0, 1})},
        {toNullableVec<String>({"1", "2", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({0, 0, 0, 1, 1})},
        {toNullableVec<String>({"1", "3", {}, "1", {}}), toNullableVec<String>({"3", "4", "3", {}, {}}), toNullableVec<Int8>({0, 0, 0, 1, 1})},
    };

    for (size_t i = 0; i < join_type_num; ++i)
    {
        for (size_t j = 0; j < simple_test_num; ++j)
        {
            const auto & [l, r, k] = join_cases[j];
            auto request = context.scan("simple_test", l)
                               .join(context.scan("simple_test", r), join_types[i], {col(k)})
                               .build(context);

            {
                context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
                executeAndAssertColumnsEqual(request, expected_cols[i * simple_test_num + j]);

                // for spill to disk tests
                context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(10000)));
                ASSERT_THROW(executeStreams(request), Exception);
                auto concurrences = {2, 5, 10};
                for (auto concurrency : concurrences)
                {
                    ASSERT_COLUMNS_EQ_UR(expected_cols[i * simple_test_num + j], executeStreams(request, concurrency));
                }
            }
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiJoin)
try
{
    context.addMockTable("multi_test", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 3, 0}), toVec<Int32>("b", {2, 2, 0}), toVec<Int32>("c", {3, 2, 0})});

    context.addMockTable("multi_test", "t2", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {3, 3, 0}), toVec<Int32>("b", {4, 2, 0}), toVec<Int32>("c", {5, 3, 0})});

    context.addMockTable("multi_test", "t3", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 2, 0}), toVec<Int32>("b", {2, 2, 0})});

    context.addMockTable("multi_test", "t4", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {3, 2, 0}), toVec<Int32>("b", {4, 2, 0})});

    const ColumnsWithTypeAndName expected_cols[join_type_num * join_type_num] = {
        /// inner x inner x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})},
        /// inner x left x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})},
        /// inner x right x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})},
        /// inner x semi x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0})},
        /// inner x anti semi x inner
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})},
        /// inner x left outer semi x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int8>({1, 1, 1})},
        /// inner x anti left outer semi x inner
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int8>({0, 0, 0})},

        /// left x inner x left
        {toNullableVec<Int32>({1, 1, 3, 3, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, 3, 3, 3, 3, 0}), toNullableVec<Int32>({{}, {}, 4, 4, 2, 2, 0}), toNullableVec<Int32>({{}, {}, 5, 5, 3, 3, 0}), toNullableVec<Int32>({1, 2, 1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, 2, {}, 2, {}, 2, 0}), toNullableVec<Int32>({{}, 2, {}, 2, {}, 2, 0})},
        /// left x left x left
        {toNullableVec<Int32>({1, 1, 3, 3, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, 3, 3, 3, 3, 0}), toNullableVec<Int32>({{}, {}, 2, 2, 4, 4, 0}), toNullableVec<Int32>({{}, {}, 3, 3, 5, 5, 0}), toNullableVec<Int32>({2, 1, 2, 1, 2, 1, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({2, {}, 2, {}, 2, {}, 0}), toNullableVec<Int32>({2, {}, 2, {}, 2, {}, 0})},
        /// left x right x left
        {toNullableVec<Int32>({1, 3, 3, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 3, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, {}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, {}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, {}, 5, 3, 0}), toNullableVec<Int32>({1, 1, 1, 2, 2, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, {}, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, {}, 2, 2, 2, 0})},
        /// left x semi x left
        {toNullableVec<Int32>({1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, 0})},
        /// left x anti semi x left
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})},
        /// left x left outer semi x left
        {toNullableVec<Int32>({1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, 0}), toNullableVec<Int8>({1, 1, 1, 1})},
        /// left x anti left outer semi x left
        {toNullableVec<Int32>({1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, 0}), toNullableVec<Int8>({0, 0, 0, 0})},

        /// right x inner x right
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})},
        /// right x left x right
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})},
        /// right x right x right
        {toNullableVec<Int32>({{}, 3, 3, 0}), toNullableVec<Int32>({{}, 2, 2, 0}), toNullableVec<Int32>({{}, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, 0}), toNullableVec<Int32>({{}, 2, 2, 0}), toNullableVec<Int32>({{}, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 0}), toNullableVec<Int32>({4, 2, 2, 0})},
        /// right x semi x right
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0})},
        /// right x anti semi x right
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})},
        /// right x left outer semi x right
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int8>({1, 1, 1})},
        /// right x anti left outer semi x right
        {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int8>({0, 0, 0})},

        /// semi x inner x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0})},
        /// semi x left x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0})},
        /// semi x right x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0})},
        /// semi x semi x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0})},
        /// semi x anti semi x semi
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})},
        /// semi x left outer semi x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int8>({1, 1})},
        /// semi x anti left outer semi x semi
        {toNullableVec<Int32>({3, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int32>({2, 0}), toNullableVec<Int8>({0, 0})},

        /// anti semi x inner x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3}), toNullableVec<Int32>({1}), toNullableVec<Int32>({2})},
        /// anti semi x left x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3}), toNullableVec<Int32>({1}), toNullableVec<Int32>({2})},
        /// anti semi x right x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3}), toNullableVec<Int32>({1}), toNullableVec<Int32>({2})},
        /// anti semi x semi x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3})},
        /// anti semi x anti semi x anti semi
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})},
        /// anti semi x left outer semi x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3}), toNullableVec<Int8>({1})},
        /// anti semi x left outer anti semi x anti semi
        {toNullableVec<Int32>({1}), toNullableVec<Int32>({2}), toNullableVec<Int32>({3}), toNullableVec<Int8>({0})},

        /// left outer semi x inner x left outer semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({0, 0, 1, 1, 1}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({0, 1, 0, 1, 1})},
        /// left outer semi x left x left outer semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({0, 0, 1, 1, 1}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({0, 1, 0, 1, 1})},
        /// left outer semi x right x left outer semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({0, 0, 1, 1, 1}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({0, 1, 0, 1, 1})},
        /// left outer semi x semi x left outer semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({0, 1, 1})},
        /// left outer semi x anti semi x left outer semi
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int8>({})},
        /// left outer semi x left outer semi x left outer semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({0, 1, 1}), toNullableVec<Int8>({1, 1, 1})},
        /// left outer semi x left outer anti semi x left outer semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({0, 1, 1}), toNullableVec<Int8>({0, 0, 0})},

        /// left outer anti semi x inner x left outer anti semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({1, 1, 0, 0, 0}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({1, 0, 1, 0, 0})},
        /// left outer anti semi x left x left outer anti semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({1, 1, 0, 0, 0}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({1, 0, 1, 0, 0})},
        /// left outer anti semi x right x left outer anti semi
        {toNullableVec<Int32>({1, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 0}), toNullableVec<Int8>({1, 1, 0, 0, 0}), toNullableVec<Int32>({1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 0}), toNullableVec<Int8>({1, 0, 1, 0, 0})},
        /// left outer anti semi x semi x left outer anti semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({1, 0, 0})},
        /// left outer anti semi x anti semi x left outer anti semi
        {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int8>({})},
        /// left outer anti semi x left outer semi x left outer anti semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({1, 0, 0}), toNullableVec<Int8>({1, 1, 1})},
        /// left outer anti semi x left outer anti semi x left outer anti semi
        {toNullableVec<Int32>({1, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 2, 0}), toNullableVec<Int8>({1, 0, 0}), toNullableVec<Int8>({0, 0, 0})},
    };

    /// select * from (t1 JT1 t2 using (a)) JT2 (t3 JT1 t4 using (a)) using (b)
    for (auto [i, jt1] : ext::enumerate(join_types))
    {
        for (auto [j, jt2] : ext::enumerate(join_types))
        {
            auto t1 = context.scan("multi_test", "t1");
            auto t2 = context.scan("multi_test", "t2");
            auto t3 = context.scan("multi_test", "t3");
            auto t4 = context.scan("multi_test", "t4");
            auto request = t1.join(t2, jt1, {col("a")})
                               .join(t3.join(t4, jt1, {col("a")}),
                                     jt2,
                                     {col("b")})
                               .build(context);

            executeAndAssertColumnsEqual(request, expected_cols[i * join_type_num + j]);
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinCast)
try
{
    auto cast_request = [&]() {
        return context.scan("cast", "t1")
            .join(context.scan("cast", "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
            .build(context);
    };

    /// int(1) == float(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeFloat}}, {toVec<Float32>("a", {1.0})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<Int32>({1}), toNullableVec<Float32>({1.0})});

    /// int(1) == double(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeDouble}}, {toVec<Float64>("a", {1.0})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<Int32>({1}), toNullableVec<Float64>({1.0})});

    /// float(1) == double(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeFloat}}, {toVec<Float32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeDouble}}, {toVec<Float64>("a", {1})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<Float32>({1}), toNullableVec<Float64>({1})});

    /// varchar('x') == char('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeString}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// tinyblob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeTinyBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// mediumBlob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeMediumBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// blob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// longBlob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLongBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeAndAssertColumnsEqual(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// decimal with different scale
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeNewDecimal}}, {createColumn<Decimal256>(std::make_tuple(9, 4), {"0.12"}, "a")});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeNewDecimal}}, {createColumn<Decimal256>(std::make_tuple(9, 3), {"0.12"}, "a")});

    executeAndAssertColumnsEqual(cast_request(), {createNullableColumn<Decimal256>(std::make_tuple(65, 0), {"0.12"}, {0}), createNullableColumn<Decimal256>(std::make_tuple(65, 0), {"0.12"}, {0})});

    /// datetime(1970-01-01 00:00:01) == timestamp(1970-01-01 00:00:01)
    context.addMockTable("cast", "t1", {{"datetime", TiDB::TP::TypeDatetime}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    context.addMockTable("cast", "t2", {{"datetime", TiDB::TP::TypeTimestamp}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    auto cast_request_1 = [&]() {
        return context.scan("cast", "t1")
            .join(context.scan("cast", "t2"), tipb::JoinType::TypeInnerJoin, {col("datetime")})
            .build(context);
    };
    executeAndAssertColumnsEqual(cast_request_1(), {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0), createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0)});
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinAgg)
try
{
    context.addMockTable("join_agg", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 3, 4}), toVec<Int32>("b", {1, 1, 4, 1})});

    context.addMockTable("join_agg", "t2", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 4, 2}), toVec<Int32>("b", {2, 6, 2})});

    const ColumnsWithTypeAndName expected_cols[join_type_num] = {
        {toNullableVec<Int32>({4}), toNullableVec<Int32>({1}), toVec<UInt64>({3}), toNullableVec<Int32>({1})},
        {toNullableVec<Int32>({4, 3}), toNullableVec<Int32>({1, 3}), toVec<UInt64>({3, 1}), toNullableVec<Int32>({1, 4})},
        {toNullableVec<Int32>({4, {}}), toNullableVec<Int32>({1, {}}), toVec<UInt64>({3, 0}), toNullableVec<Int32>({1, {}})},
        {toNullableVec<Int32>({4}), toNullableVec<Int32>({1}), toVec<UInt64>({3}), toNullableVec<Int32>({1})},
        {toNullableVec<Int32>({3}), toNullableVec<Int32>({3}), toVec<UInt64>({1}), toNullableVec<Int32>({4})},
        {toNullableVec<Int32>({4, 3}), toNullableVec<Int32>({1, 3}), toVec<UInt64>({3, 1}), toNullableVec<Int32>({1, 4})},
        {toNullableVec<Int32>({4, 3}), toNullableVec<Int32>({1, 3}), toVec<UInt64>({3, 1}), toNullableVec<Int32>({1, 4})},
    };

    for (auto [i, tp] : ext::enumerate(join_types))
    {
        auto request = context.scan("join_agg", "t1")
                           .join(context.scan("join_agg", "t2"), tp, {col("a")})
                           .aggregation({Max(col("a")), Min(col("a")), Count(col("a"))}, {col("b")})
                           .build(context);

        executeAndAssertColumnsEqual(request, expected_cols[i]);
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, CrossJoinWithCondition)
try
{
    context.addMockTable("cross_join", "t1", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "2", {}, "1"}), toNullableVec<String>("b", {"3", "4", "3", {}})});
    context.addMockTable("cross_join", "t2", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "3", {}, "2"}), toNullableVec<String>("b", {"3", "4", "3", {}})});

    const auto cond = gt(col("a"), lit(Field("1", 1)));

    const auto table_scan = [&]() -> std::tuple<DAGRequestBuilder, DAGRequestBuilder> {
        return {context.scan("cross_join", "t1"), context.scan("cross_join", "t2")};
    };

    const ColumnsWithTypeAndName expected_cols[join_type_num] = {
        // inner
        {toNullableVec<String>({"2", "2", "2", "2"}), toNullableVec<String>({"4", "4", "4", "4"}), toNullableVec<String>({"1", "3", {}, "2"}), toNullableVec<String>({"3", "4", "3", {}})},
        // left
        {toNullableVec<String>({"1", "2", "2", "2", "2", {}, "1"}), toNullableVec<String>({"3", "4", "4", "4", "4", "3", {}}), toNullableVec<String>({{}, "2", {}, "3", "1", {}, {}}), toNullableVec<String>({{}, {}, "3", "4", "3", {}, {}})},
        // right
        {toNullableVec<String>({{}, "1", {}, "2", "1", {}, "1", {}, "2", "1"}), toNullableVec<String>({{}, {}, "3", "4", "3", {}, {}, "3", "4", "3"}), toNullableVec<String>({"1", "3", "3", "3", "3", {}, "2", "2", "2", "2"}), toNullableVec<String>({"3", "4", "4", "4", "4", "3", {}, {}, {}, {}})},
        // semi
        {toNullableVec<String>({"2"}), toNullableVec<String>({"4"})},
        // anti semi
        {toNullableVec<String>({"1", "1", {}}), toNullableVec<String>({{}, "3", "3"})},
        // left outer semi
        {toNullableVec<String>({"1", "2", {}, "1"}), toNullableVec<String>({"3", "4", "3", {}}), toNullableVec<Int8>({0, 1, 0, 0})},
        // anti left outer semi
        {toNullableVec<String>({"1", "2", {}, "1"}), toNullableVec<String>({"3", "4", "3", {}}), toNullableVec<Int8>({1, 0, 1, 1})},
    };

    /// for cross join, there is no join columns
    size_t i = 0;

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeInnerJoin, {}, {}, {}, {cond}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeLeftOuterJoin, {}, {cond}, {}, {}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeRightOuterJoin, {}, {}, {cond}, {}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeSemiJoin, {}, {}, {}, {cond}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeAntiSemiJoin, {}, {}, {}, {cond}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }

    {
        auto [t1, t2] = table_scan();
        auto request = t1
                           .join(t2, tipb::JoinType::TypeAntiLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, expected_cols[i++]);
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, Issue8791)
try
{
    // clang-format off
    auto build_key = toNullableVec<Int64>(
        "id",
        {
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 3, 3, 3, 3, 3, 3, 3,
            4, 4, 4, 4, 4,
            5, 5, 5, 5, 5, 5,
            6, 6, 6, 6, 6, 6,
            7, 7, 7, 7, 7, 7,
        });
    auto build_col = toNullableVec<Int64>(
        "build_value",
        {
            10, 30, 10, 30, 10, 30, 10, 10, 10, 30, 10, 30, 10, 30, 30,
            10, 30, 10, 30, 10, 30, 10, 30,
            30, 30, 30, 30, 30,
            30, 30, 30, 30, 30, 10,
            30, 30, 10, 30, 30, 30,
            30, 30, 30, 10, 10, 30,
        });
    auto probe_key = toNullableVec<Int64>(
        "id",
        {
            1,
            2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3,
            4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4,
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6,
            7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7,
        });
    auto probe_col = toNullableVec<Int64>(
        "probe_value",
        {
            20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
            20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
        });
    // clang-format on
    context.addMockTable(
        "issue_8791",
        "build_table",
        {{"id", TiDB::TP::TypeLongLong}, {"build_value", TiDB::TP::TypeLongLong}},
        {build_key, build_col});
    context.addMockTable(
        "issue_8791",
        "probe_table",
        {{"id", TiDB::TP::TypeLongLong}, {"probe_value", TiDB::TP::TypeLongLong}},
        {probe_key, probe_col});

    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(90)));
    {
        auto anti_join_request = context.scan("issue_8791", "probe_table")
                                     .join(
                                         context.scan("issue_8791", "build_table"),
                                         tipb::JoinType::TypeAntiSemiJoin,
                                         {col("id")},
                                         {},
                                         {},
                                         {gt(col("probe_value"), col("build_value"))},
                                         {})
                                     .aggregation({Count(col("id"))}, {})
                                     .build(context);

        auto expected_columns = {toVec<UInt64>({16})};
        ASSERT_COLUMNS_EQ_UR(expected_columns, executeStreams(anti_join_request, 1));
    }
    {
        auto inner_join_request = context.scan("issue_8791", "probe_table")
                                      .join(
                                          context.scan("issue_8791", "build_table"),
                                          tipb::JoinType::TypeInnerJoin,
                                          {col("id")},
                                          {},
                                          {},
                                          {gt(col("probe_value"), col("build_value"))},
                                          {})
                                      .aggregation({Count(col("id"))}, {})
                                      .build(context);

        auto expected_columns = {toVec<UInt64>({240})};
        ASSERT_COLUMNS_EQ_UR(expected_columns, executeStreams(inner_join_request, 1));
    }
}
CATCH
    
TEST_F(JoinExecutorTestRunner, JoinWithTableScan)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                       .build(context);
    {
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }

    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                  .project({"s", "join_c"})
                  .build(context);
    {
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }

    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table_2"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                  .build(context);
    {
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana", "banana", "banana", "banana"}), toNullableVec<String>({"apple", "apple", "apple", "banana"}), toNullableVec<String>({"banana", "banana", "banana", {}}), toNullableVec<String>({"apple", "apple", "apple", {}})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithExchangeReceiver)
try
{
    auto request = context
                       .receive("exchange_l_table")
                       .join(context.receive("exchange_r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                       .build(context);
    {
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithTableScanAndReceiver)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.receive("exchange_r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                       .build(context);
    {
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithNullTable)
try
{
    context.addMockTable("null_test", "t", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toVec<Int32>("b", {1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toVec<Int32>("c", {1, 1, 1, 1, 1, 2, 2, 2, 2, 2})});
    context.addMockTable("null_test", "null_table", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {}), toVec<Int32>("b", {}), toVec<Int32>("c", {})});

    std::shared_ptr<tipb::DAGRequest> request;

    // inner join
    {
        // null table join non-null table
        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeInnerJoin, {col("a")})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        // non-null table join null table
        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeInnerJoin, {col("a")})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})});

        // null table join null table
        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeInnerJoin, {col("a")})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});
    }

    // cross join
    const auto cond = gt(col("a"), lit(Field(static_cast<Int64>(5))));
    // non-null table join null table
    {
        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeInnerJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeLeftOuterJoin, {}, {cond}, {}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toNullableVec<Int32>({1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 1, 1, 2, 2, 2, 2, 2}), toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}), toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}), toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}})});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeRightOuterJoin, {}, {}, {cond}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({}), toNullableVec<Int32>({}), toNullableVec<Int32>({})});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeAntiSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toNullableVec<Int32>({1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 1, 1, 2, 2, 2, 2, 2})});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        // the 4th col is left outer semi helper col.
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toNullableVec<Int32>({1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 1, 1, 2, 2, 2, 2, 2}), toNullableVec<Int8>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0})});

        request = context.scan("null_test", "t")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeAntiLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        // the 4th col is left outer semi helper col.
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toNullableVec<Int32>({1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 1, 1, 2, 2, 2, 2, 2}), toNullableVec<Int8>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1})});
    }

    // null table join non-null table
    {
        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeInnerJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeLeftOuterJoin, {}, {cond}, {}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeRightOuterJoin, {}, {}, {cond}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}), toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}), toNullableVec<Int32>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}), toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toNullableVec<Int32>({1, 1, 1, 1, 1, 1, 1, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 1, 1, 2, 2, 2, 2, 2})});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeSemiJoin, {}, {}, {}, {cond}, {}, 0)
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeAntiSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeAntiSemiJoin, {}, {}, {}, {cond}, {}, 0)
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "t"), tipb::JoinType::TypeAntiLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});
    }

    // null table join null table
    {
        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeInnerJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeLeftOuterJoin, {}, {cond}, {}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeRightOuterJoin, {}, {}, {cond}, {}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeAntiSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});

        request = context.scan("null_test", "null_table")
                      .join(context.scan("null_test", "null_table"), tipb::JoinType::TypeAntiLeftOuterSemiJoin, {}, {}, {}, {cond}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, {});
    }
}
CATCH


// Currently only support join with `using`
TEST_F(JoinExecutorTestRunner, RawQuery)
try
{
    String query = "select * from test_db.l_table left outer join test_db.r_table_2 using join_c";
    auto cols = {toNullableVec<String>({"banana", "banana", "banana", "banana"}), toNullableVec<String>({"apple", "apple", "apple", "banana"}), toNullableVec<String>({"banana", "banana", "banana", {}}), toNullableVec<String>({"apple", "apple", "apple", {}})};
    ASSERT_COLUMNS_EQ_R(executeRawQuery(query, 1), cols);
}
CATCH

TEST_F(JoinExecutorTestRunner, SplitJoinResult)
try
{
    context.addMockTable("split_test", "t1", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 1, 1, 1, 1, 1, 1, 1})});
    context.addMockTable("split_test", "t2", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 1, 1})});

    auto request = context
                       .scan("split_test", "t1")
                       .join(context.scan("split_test", "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
                       .build(context);

    std::vector<size_t> block_sizes{1, 2, 7, 25, 49, 50, 51, DEFAULT_BLOCK_SIZE};
    std::vector<std::vector<size_t>> expect{{5, 5, 5, 5, 5, 5, 5, 5, 5, 5}, {5, 5, 5, 5, 5, 5, 5, 5, 5, 5}, {5, 5, 5, 5, 5, 5, 5, 5, 5, 5}, {25, 25}, {45, 5}, {50}, {50}, {50}};
    for (size_t i = 0; i < block_sizes.size(); ++i)
    {
        context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_sizes[i])));
        auto blocks = getExecuteStreamsReturnBlocks(request);
        ASSERT_EQ(expect[i].size(), blocks.size());
        for (size_t j = 0; j < blocks.size(); ++j)
        {
            ASSERT_EQ(expect[i][j], blocks[j].rows());
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, MergeAfterSplit)
try
{
    context.addMockTable("split_test", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}), toVec<Int32>("b", {2, 2, 2, 2, 2, 2, 2, 2, 2, 2})});
    context.addMockTable("split_test", "t2", {{"a", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 1, 1}), toVec<Int32>("c", {1, 2, 3, 4, 5})});

    std::vector<size_t> block_sizes{
        1,
        2,
        7,
        25,
        49,
        50,
        51,
        DEFAULT_BLOCK_SIZE};
    auto join_types = {tipb::JoinType::TypeInnerJoin, tipb::JoinType::TypeSemiJoin};
    std::vector<std::vector<std::vector<size_t>>> expects{
        {
            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            {4, 3, 2, 1},
            {5, 5},
            {9, 1},
            {10},
            {10},
            {10},
        },
        {
            {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
            {2, 2, 2, 2, 2},
            {7, 3},
            {10},
            {10},
            {10},
            {10},
            {10},
        },
    };
    for (size_t index = 0; index < join_types.size(); index++)
    {
        auto request = context
                           .scan("split_test", "t1")
                           .join(context.scan("split_test", "t2"), *(join_types.begin() + index), {col("a")}, {}, {}, {gt(col("b"), col("c"))}, {})
                           .build(context);
        auto & expect = expects[index];

        for (size_t i = 0; i < block_sizes.size(); ++i)
        {
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_sizes[i])));
            auto blocks = getExecuteStreamsReturnBlocks(request);
            ASSERT_EQ(expect[i].size(), blocks.size());
            for (size_t j = 0; j < blocks.size(); ++j)
            {
                ASSERT_EQ(expect[i][j], blocks[j].rows());
            }
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, SpillToDisk)
try
{
    context.addMockTable("split_test", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}), toVec<Int32>("b", {2, 2, 2, 2, 2, 2, 2, 2, 2, 2})});
    context.addMockTable("split_test", "t2", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 0, 0, 0})});

    auto request = context
                       .scan("split_test", "t1")
                       .join(context.scan("split_test", "t2"), tipb::JoinType::TypeInnerJoin, {col("a")})
                       .build(context);

    auto join_restore_concurrences = {-1, 0, 1, 5};
    auto concurrences = {2, 5, 10};
    const ColumnsWithTypeAndName expect = {toNullableVec<Int32>({1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 0, 0, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}), toNullableVec<Int32>({1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 0, 0, 0})};
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(10000)));
    for (const auto & join_restore_concurrency : join_restore_concurrences)
    {
        context.context->setSetting("join_restore_concurrency", Field(static_cast<Int64>(join_restore_concurrency)));
        ASSERT_THROW(executeStreams(request), Exception);
        for (auto concurrency : concurrences)
        {
            ASSERT_COLUMNS_EQ_UR(expect, executeStreams(request, concurrency));
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, ScanHashMapAfterProbeData)
try
{
    UInt64 max_block_size = 800;
    size_t original_max_streams = 20;
    size_t original_max_streams_small = 4;
    std::vector<String> left_table_names = {"left_table_1_concurrency", "left_table_3_concurrency", "left_table_5_concurrency", "left_table_10_concurrency"};
    std::vector<String> right_table_names = {"right_table_1_concurrency", "right_table_3_concurrency", "right_table_5_concurrency", "right_table_10_concurrency"};
    std::vector<size_t> right_exchange_receiver_concurrency = {1, 3, 5, 10};
    /// disable spill
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    /// case 1, right join without right condition
    auto request = context
                       .scan("outer_join_test", right_table_names[0])
                       .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
                       .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    auto ref_columns = executeStreams(request, original_max_streams);

    /// case 1.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")})
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 1.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {}, {}, {}, exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
    /// case 2, right join with right condition
    request = context
                  .scan("outer_join_test", right_table_names[0])
                  .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")}, {gt(col(right_table_names[0] + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, {}, 0)
                  .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    ref_columns = executeStreams(request, original_max_streams);
    /// case 2.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(right_table_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, 0)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 2.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            String exchange_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(exchange_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, ScanHashMapAfterProbeDataWithSpillEnabledAndSpillTriggered)
try
{
    UInt64 max_block_size = 800;
    size_t original_max_streams = 20;
    /// used to test the case that max_stream less than fine_grained_stream_count
    size_t original_max_streams_small = 4;
    std::vector<String> left_table_names = {"left_table_1_concurrency", "left_table_3_concurrency", "left_table_5_concurrency", "left_table_10_concurrency"};
    std::vector<String> right_table_names = {"right_table_1_concurrency", "right_table_3_concurrency", "right_table_5_concurrency", "right_table_10_concurrency"};
    std::vector<size_t> right_exchange_receiver_concurrency = {1, 3, 5, 10};
    UInt64 max_bytes_before_external_join = 20000;
    /// case 1, right join without right condition
    auto request = context
                       .scan("outer_join_test", right_table_names[0])
                       .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
                       .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    /// use right_table left join left_table as the reference
    auto ref_columns = executeStreams(request, original_max_streams);

    /// case 1.1 table scan join table scan
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(max_bytes_before_external_join)));
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")})
                          .build(context);
            if (right_table_name == "right_table_1_concurrency")
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            }
        }
    }
    /// case 1.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {}, {}, {}, exchange_concurrency)
                          .build(context);
            if (exchange_concurrency == 1)
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
                if (original_max_streams_small < exchange_concurrency)
                    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
            }
        }
    }
    /// case 2, right join with right condition
    request = context
                  .scan("outer_join_test", right_table_names[0])
                  .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")}, {gt(col(right_table_names[0] + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, {}, 0)
                  .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    ref_columns = executeStreams(request, original_max_streams);

    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(max_bytes_before_external_join)));
    /// case 2.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(right_table_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, 0)
                          .build(context);
            if (right_table_name == "right_table_1_concurrency")
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            }
        }
    }
    /// case 2.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            String exchange_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(exchange_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, exchange_concurrency)
                          .build(context);
            if (exchange_concurrency == 1)
            {
                ASSERT_THROW(executeStreams(request, original_max_streams), Exception);
            }
            else
            {
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
                if (original_max_streams_small < exchange_concurrency)
                    ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
            }
        }
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, ScanHashMapAfterProbeDataWithSpillEnabledAndSpillNotTriggered)
try
{
    UInt64 max_block_size = 800;
    size_t original_max_streams = 20;
    /// used to test the case that max_stream less than fine_grained_stream_count
    size_t original_max_streams_small = 4;
    std::vector<String> left_table_names = {"left_table_1_concurrency", "left_table_3_concurrency", "left_table_5_concurrency", "left_table_10_concurrency"};
    std::vector<String> right_table_names = {"right_table_1_concurrency", "right_table_3_concurrency", "right_table_5_concurrency", "right_table_10_concurrency"};
    std::vector<size_t> right_exchange_receiver_concurrency = {1, 3, 5, 10};
    UInt64 max_bytes_before_external_join_will_no_spill_happens = 1024ULL * 1024 * 1024 * 1024;
    /// case 1, right join without right condition
    auto request = context
                       .scan("outer_join_test", right_table_names[0])
                       .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")})
                       .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                       .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    /// use right_table left join left_table as the reference
    auto ref_columns = executeStreams(request, original_max_streams);

    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(max_bytes_before_external_join_will_no_spill_happens)));
    /// case 1.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")})
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 1.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {}, {}, {}, exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
    /// case 2, right join with right condition
    request = context
                  .scan("outer_join_test", right_table_names[0])
                  .join(context.scan("outer_join_test", left_table_names[0]), tipb::JoinType::TypeLeftOuterJoin, {col("a")}, {gt(col(right_table_names[0] + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, {}, 0)
                  .project({fmt::format("{}.a", left_table_names[0]), fmt::format("{}.b", left_table_names[0]), fmt::format("{}.a", right_table_names[0]), fmt::format("{}.b", right_table_names[0])})
                  .build(context);
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(max_block_size)));
    /// use right_table left join left_table as the reference
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(0)));
    ref_columns = executeStreams(request, original_max_streams);
    context.context->setSetting("max_bytes_before_external_join", Field(static_cast<UInt64>(max_bytes_before_external_join_will_no_spill_happens)));
    /// case 2.1 table scan join table scan
    for (auto & left_table_name : left_table_names)
    {
        for (auto & right_table_name : right_table_names)
        {
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.scan("outer_join_test", right_table_name), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(right_table_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, 0)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
        }
    }
    /// case 2.2 table scan join fine grained exchange receiver
    for (auto & left_table_name : left_table_names)
    {
        for (size_t exchange_concurrency : right_exchange_receiver_concurrency)
        {
            String exchange_name = fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency);
            request = context
                          .scan("outer_join_test", left_table_name)
                          .join(context.receive(fmt::format("right_exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency), tipb::JoinType::TypeRightOuterJoin, {col("a")}, {}, {gt(col(exchange_name + ".b"), lit(Field(static_cast<Int64>(1000))))}, {}, {}, exchange_concurrency)
                          .build(context);
            ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams));
            if (original_max_streams_small < exchange_concurrency)
                ASSERT_COLUMNS_EQ_UR(ref_columns, executeStreams(request, original_max_streams_small));
        }
    }
}
CATCH

ColumnsWithTypeAndName genSemiJoinResult(tipb::JoinType type, const ColumnsWithTypeAndName & left, const ColumnWithTypeAndName & left_semi_res)
{
    ColumnsWithTypeAndName res = left;
    if (type == tipb::JoinType::TypeLeftOuterSemiJoin)
    {
        res.emplace_back(left_semi_res);
    }
    else if (type == tipb::JoinType::TypeAntiLeftOuterSemiJoin)
    {
        auto new_column = left_semi_res.column->cloneEmpty();
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(left_semi_res.column.get());
        const auto & nested_column_data = static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
        for (size_t i = 0; i < nullable_column->size(); ++i)
        {
            if (nullable_column->isNullAt(i))
                new_column->insert(FIELD_NULL);
            else if (nested_column_data[i])
                new_column->insert(FIELD_INT8_0);
            else
                new_column->insert(FIELD_INT8_1);
        }
        auto anti_left_semi_ans = left_semi_res.cloneEmpty();
        anti_left_semi_ans.column = std::move(new_column);

        res.emplace_back(anti_left_semi_ans);
    }
    else if (type == tipb::JoinType::TypeAntiSemiJoin)
    {
        IColumn::Filter filter(left_semi_res.column->size());
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(left_semi_res.column.get());
        const auto & nested_column_data = static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
        for (size_t i = 0; i < nullable_column->size(); ++i)
        {
            if (nullable_column->isNullAt(i) || nested_column_data[i])
                filter[i] = 0;
            else
                filter[i] = 1;
        }
        for (auto & r : res)
            r.column = r.column->filter(filter, -1);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Semi join Type {} is not supported", type);
    return res;
}

TEST_F(JoinExecutorTestRunner, NullAwareSemiJoin)
try
{
    using tipb::JoinType;
    /// One join key(t.a = s.a) + no other condition.
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t1 = {
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, {}, {}, 4, 5})},
            {toNullableVec<Int32>("a", {})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5})},
            toNullableVec<Int8>({1, 1, 1, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {6, 7, 8, 9, 10})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, 8, 9, 10})},
            toNullableVec<Int8>({1, 1, {}, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, {}, 5})},
            {toNullableVec<Int32>("a", {1, {}, 3, 4, {}})},
            toNullableVec<Int8>({1, {}, 1, {}, {}}),
        }};

    for (const auto & [left, right, res] : t1)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeLong}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeLong}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a")},
                                     {},
                                     {},
                                     {},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }

    /// One join key(t.a = s.a) + other condition(t.c < s.c).
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t2 = {
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({1, 1, 1, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, {}, 5}), toNullableVec<Int32>("c", {2, {}, 2, 2, 2})},
            {toNullableVec<Int32>("a", {}), toNullableVec<Int32>("c", {})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, {}, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {6, 7, 8, 9, 10}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, 2, 8, 9, 10}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({1, 1, {}, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, {}, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, {}, 3, 4, {}}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({1, {}, 1, {}, {}}),
        }};

    for (const auto & [left, right, res] : t2)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a")},
                                     {},
                                     {},
                                     {lt(col("t.c"), col("s.c"))},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }

    /// Two join keys(t.a = s.a and t.b = s.b) + no other condition.
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t3 = {
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5})},
            toNullableVec<Int8>({1, 1, 1, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, {}, 3, {}, 5}), toNullableVec<Int32>("b", {1, 2, {}, {}, 5})},
            {toNullableVec<Int32>("a", {}), toNullableVec<Int32>("b", {})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, {}, 3, {}, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {6, 7, 8, 9, 10})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {1, {}, 3, {}, 4, 4}), toNullableVec<Int32>("b", {1, 2, {}, 4, {}, 4})},
            toNullableVec<Int8>({1, {}, {}, 1, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4}), toNullableVec<Int32>("b", {1, 2, {}, 4})},
            {toNullableVec<Int32>("a", {1, {}, 3, {}}), toNullableVec<Int32>("b", {1, 2, {}, {}})},
            toNullableVec<Int8>({1, {}, {}, {}}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5, {}, 4, {}}), toNullableVec<Int32>("b", {{}, 3, 2, 4, 5, 1, {}, {}})},
            {toNullableVec<Int32>("a", {2, 2, 2, 3, 4, 4}), toNullableVec<Int32>("b", {1, 3, {}, {}, 4, {}})},
            toNullableVec<Int8>({0, 1, {}, 1, 0, {}, {}, {}}),
        },
    };

    for (const auto & [left, right, res] : t3)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a"), col("b")},
                                     {},
                                     {},
                                     {},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }

    /// Two join keys(t.a = s.a and t.b = s.b) + other condition(t.c < s.c).
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t4 = {
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({1, 1, 1, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {}), toNullableVec<Int32>("b", {}), toNullableVec<Int32>("c", {})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {6, 7, 8, 9, 10}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {{}, {}, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {2, 2, 3, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({0, {}, 1, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            toNullableVec<Int8>({0, 0, 0, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {1, {}, 3, {}, 4, 4}), toNullableVec<Int32>("b", {1, 2, {}, 4, {}, 4}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2, 2})},
            toNullableVec<Int8>({1, {}, {}, 1, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 6}), toNullableVec<Int32>("b", {1, 2, 3, {}, {}}), toNullableVec<Int32>("c", {1, 2, 1, 2, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, {}}), toNullableVec<Int32>("c", {2, 1, 2, 1, 2})},
            toNullableVec<Int8>({1, 0, {}, 0, 0}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 3, {}, 6}), toNullableVec<Int32>("b", {1, 2, 3, 3, {}, {}}), toNullableVec<Int32>("c", {1, 3, 1, 2, 3, 1})},
            {toNullableVec<Int32>("a", {{}, 2, 3, 4, 5}), toNullableVec<Int32>("b", {{}, 2, 3, 4, {}}), toNullableVec<Int32>("c", {3, 1, 2, 1, 2})},
            toNullableVec<Int8>({{}, 0, 1, {}, 0, {}}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            {toNullableVec<Int32>("a", {1, 1, 1, 2, 2, 2, 3, 3, {}, 4, 4, 4}),
             toNullableVec<Int32>("b", {1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, {}}),
             toNullableVec<Int32>("c", {1, 2, 3, 1, 2, 2, 1, 2, 2, 1, 2, 3})},
            toNullableVec<Int8>({1, 0, 0, {}, 0}),
        },
    };

    for (const auto & [left, right, res] : t4)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a"), col("b")},
                                     {},
                                     {},
                                     {lt(col("t.c"), col("s.c"))},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }

    /// Two join keys(t.a = s.a and t.b = s.b) and other condition(t.c < s.d or t.a = s.a)
    /// Test the case that other condition has a condition that is same to one of join key equal conditions.
    /// In other words, test if these two expression can be handled normally when column reuse happens.
    /// For more details, see the comments in `NASemiJoinHelper::runAndCheckExprResult`.
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t5 = {
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {{}, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, {}, 3, 4, 5}), toNullableVec<Int32>("d", {2, 2, 2, 2, 2})},
            toNullableVec<Int8>({{}, {}, {}, 1, 1}),
        },
        {
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 6}), toNullableVec<Int32>("b", {1, 2, 3, {}, 5}), toNullableVec<Int32>("c", {1, 2, 1, 2, 1})},
            {toNullableVec<Int32>("a", {1, 2, 3, 4, 5}), toNullableVec<Int32>("b", {1, 2, 3, 4, {}}), toNullableVec<Int32>("d", {2, 1, 2, 1, 2})},
            toNullableVec<Int8>({1, 1, {}, {}, 0}),
        },
    };

    for (const auto & [left, right, res] : t5)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"d", TiDB::TP::TypeLong}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a"), col("b")},
                                     {},
                                     {},
                                     {Or(lt(col("c"), col("d")), eq(col("t.a"), col("s.a")))},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }

    /// Two join keys(t.a = s.a and t.b = s.b) + no other condition + collation(UTF8MB4_UNICODE_CI).
    /// left table(t) + right table(s) + result column.
    context.setCollation(TiDB::ITiDBCollator::UTF8MB4_UNICODE_CI);
    const std::vector<std::tuple<ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnWithTypeAndName>> t6 = {
        {
            {toNullableVec<String>("a", {"a", "b", "c", "d", "e"}), toNullableVec<String>("b", {"A", "b", "c", "dd", "e"})},
            {toNullableVec<String>("a", {"a", {}, "c", {}, "D", "E"}), toNullableVec<String>("b", {"a", "b", {}, "dD", "DD", {}})},
            toNullableVec<Int8>({1, {}, {}, 1, {}}),
        },
        {
            {toNullableVec<String>("a", {"aa", "bb", "cc", "dd"}), toNullableVec<String>("b", {"aa", "bb", {}, "dd"})},
            {toNullableVec<String>("a", {"AA", {}, "cC", {}}), toNullableVec<String>("b", {"aa", "bb", {}, {}})},
            toNullableVec<Int8>({1, {}, {}, {}}),
        },
        {
            {toNullableVec<String>("a", {"a", "Bb", {}, "d", "E", {}, "d", {}}), toNullableVec<String>("b", {{}, "CC", "bb", "dD", "EE", "AA", {}, {}})},
            {toNullableVec<String>("a", {"b", "bb", "b", "C", "D", "d"}), toNullableVec<String>("b", {"AA", "cc", {}, {}, "Dd", {}})},
            toNullableVec<Int8>({0, 1, {}, 1, 0, {}, {}, {}}),
        },
    };

    for (const auto & [left, right, res] : t6)
    {
        context.addMockTable("null_aware_semi", "t", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, left);
        context.addMockTable("null_aware_semi", "s", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, right);

        for (const auto type : {JoinType::TypeLeftOuterSemiJoin, JoinType::TypeAntiLeftOuterSemiJoin, JoinType::TypeAntiSemiJoin})
        {
            auto request = context.scan("null_aware_semi", "t")
                               .join(context.scan("null_aware_semi", "s"),
                                     type,
                                     {col("a"), col("b")},
                                     {},
                                     {},
                                     {},
                                     {},
                                     0,
                                     true)
                               .build(context);
            executeAndAssertColumnsEqual(request, genSemiJoinResult(type, left, res));
        }
    }
}
CATCH


TEST_F(JoinExecutorTestRunner, RightSemiFamilyJoin)
try
{
    using tipb::JoinType;
    /// One join key(t.a = s.a) + no other condition.
    /// type + left table(t) + right table(s) + result column.
    const std::vector<std::tuple<JoinType, ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnsWithTypeAndName>> t1 = {
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 3, {}, 4})},
            {toNullableVec<Int32>("a", {1, 4})},
        },
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {2, 3, {}, 7})},
            {toNullableVec<Int32>("a", {2})},
        },
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 1, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 1, 4, 5})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 3, {}, 4})},
            {toNullableVec<Int32>("a", {{}, 2, 5})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5})},
            {toNullableVec<Int32>("a", {{}})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, 2, {}, {}, 5, 5})},
            {toNullableVec<Int32>("a", {3, 2, {}, 7})},
            {toNullableVec<Int32>("a", {{}, {}, 1, 5, 5})},
        }};

    for (const auto & [type, left, right, res] : t1)
    {
        context.addMockTable("right_semi_family", "t", {{"a", TiDB::TP::TypeLong}}, left);
        context.addMockTable("right_semi_family", "s", {{"a", TiDB::TP::TypeLong}}, right);

        auto request = context.scan("right_semi_family", "t")
                           .join(context.scan("right_semi_family", "s"),
                                 type,
                                 {col("a")},
                                 {},
                                 {},
                                 {},
                                 {},
                                 0,
                                 false,
                                 0)
                           .build(context);
        executeAndAssertColumnsEqual(request, res);
    }

    /// One join key(t.a = s.a) + other condition(t.c < s.c).
    /// left table(t) + right table(s) + result column.
    const std::vector<std::tuple<JoinType, ColumnsWithTypeAndName, ColumnsWithTypeAndName, ColumnsWithTypeAndName>> t2 = {
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 2, 1})},
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            {toNullableVec<Int32>("a", {1, 2, 5}), toNullableVec<Int32>("c", {1, 1, 1})},
        },
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {1, 1, 1, 1, 1})},
            {toNullableVec<Int32>("a", {3, 2, {}, 4, 6}), toNullableVec<Int32>("c", {2, 2, 2, 2, 2})},
            {toNullableVec<Int32>("a", {2, 4}), toNullableVec<Int32>("c", {1, 1})},
        },
        {
            JoinType::TypeSemiJoin,
            {toNullableVec<Int32>("a", {1, 1, 2, {}, 4, 4, 5}), toNullableVec<Int32>("c", {1, 2, 3, 4, 5, 6, 7})},
            {toNullableVec<Int32>("a", {1, 4}), toNullableVec<Int32>("c", {2, 9})},
            {toNullableVec<Int32>("a", {1, 4, 4}), toNullableVec<Int32>("c", {1, 5, 6})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {1, 1, 2, 7, 1})},
            {toNullableVec<Int32>("a", {1, 5, 8}), toNullableVec<Int32>("c", {0, 2, 8})},
            {toNullableVec<Int32>("a", {{}, 1, 2, 4}), toNullableVec<Int32>("c", {2, 1, 1, 7})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {1, 2, 3, 4, 5})},
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {0, 1, 5, 6, 7})},
            {toNullableVec<Int32>("a", {{}, 1, 2}), toNullableVec<Int32>("c", {3, 1, 2})},
        },
        {
            JoinType::TypeAntiSemiJoin,
            {toNullableVec<Int32>("a", {1, 1, 2, 2, {}, {}, 4, 5}), toNullableVec<Int32>("c", {1, 2, 3, 4, 5, 6, 7, 8})},
            {toNullableVec<Int32>("a", {1, 2, {}, 4, 5}), toNullableVec<Int32>("c", {2, 1, 5, 6, 7})},
            {toNullableVec<Int32>("a", {{}, {}, 1, 2, 2, 4, 5}), toNullableVec<Int32>("c", {5, 6, 2, 3, 4, 7, 8})},
        }};

    for (const auto & [type, left, right, res] : t2)
    {
        context.addMockTable("right_semi_family", "t", {{"a", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, left);
        context.addMockTable("right_semi_family", "s", {{"a", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, right);

        auto request = context.scan("right_semi_family", "t")
                           .join(context.scan("right_semi_family", "s"),
                                 type,
                                 {col("a")},
                                 {},
                                 {},
                                 {lt(col("t.c"), col("s.c"))},
                                 {},
                                 0,
                                 false,
                                 0)
                           .build(context);
        executeAndAssertColumnsEqual(request, res);
    }
}
CATCH

} // namespace tests
} // namespace DB
