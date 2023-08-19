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
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver("exchange_l_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});
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
                executeAndAssertColumnsEqual(request, expected_cols[i * simple_test_num + j]);
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
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeDatetime}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeTimestamp}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    executeAndAssertColumnsEqual(cast_request(), {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0), createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0)});
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


// Currently only support join with `using`
TEST_F(JoinExecutorTestRunner, RawQuery)
try
{
    String query = "select * from test_db.l_table left outer join test_db.r_table_2 using join_c";
    auto cols = {toNullableVec<String>({"banana", "banana", "banana", "banana"}), toNullableVec<String>({"apple", "apple", "apple", "banana"}), toNullableVec<String>({"banana", "banana", "banana", {}}), toNullableVec<String>({"apple", "apple", "apple", {}})};
    ASSERT_COLUMNS_EQ_R(executeRawQuery(query, 1), cols);
}
CATCH

} // namespace tests
} // namespace DB
