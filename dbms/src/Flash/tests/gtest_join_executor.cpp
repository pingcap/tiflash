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

#include <TestUtils/ExecutorTestUtils.h>

#include <tuple>

namespace DB
{
namespace tests
{
class JoinExecutorTestRunner : public DB::tests::ExecutorTest
{
    static const size_t max_concurrency_level = 10;

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

        context.addMockTable("simple_test", "t1", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "2", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});

        context.addMockTable("simple_test", "t2", {{"a", TiDB::TP::TypeString}, {"b", TiDB::TP::TypeString}}, {toNullableVec<String>("a", {"1", "3", {}, "1", {}}), toNullableVec<String>("b", {"3", "4", "3", {}, {}})});

        context.addMockTable("multi_test", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 3, 0}), toVec<Int32>("b", {2, 2, 0}), toVec<Int32>("c", {3, 2, 0})});

        context.addMockTable("multi_test", "t2", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}, {"c", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {3, 3, 0}), toVec<Int32>("b", {4, 2, 0}), toVec<Int32>("c", {5, 3, 0})});

        context.addMockTable("multi_test", "t3", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 2, 0}), toVec<Int32>("b", {2, 2, 0})});

        context.addMockTable("multi_test", "t4", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {3, 2, 0}), toVec<Int32>("b", {4, 2, 0})});

        context.addMockTable("join_agg", "t1", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 1, 3, 4}), toVec<Int32>("b", {1, 1, 4, 1})});

        context.addMockTable("join_agg", "t2", {{"a", TiDB::TP::TypeLong}, {"b", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1, 4, 2}), toVec<Int32>("b", {2, 6, 2})});

        context.addExchangeReceiver("exchange_r_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});

        context.addExchangeReceiver("exchange_l_table",
                                    {{"s1", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
                                    {toNullableVec<String>("s", {"banana", "banana"}),
                                     toNullableVec<String>("join_c", {"apple", "banana"})});
    }

    std::tuple<DAGRequestBuilder, DAGRequestBuilder, DAGRequestBuilder, DAGRequestBuilder> multiTestScan()
    {
        return {context.scan("multi_test", "t1"), context.scan("multi_test", "t2"), context.scan("multi_test", "t3"), context.scan("multi_test", "t4")};
    }

    void executeWithConcurrency(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
    {
        for (size_t i = 1; i <= max_concurrency_level; ++i)
        {
            ASSERT_COLUMNS_EQ_UR(expect_columns, executeStreams(request, i));
        }
    }
};

TEST_F(JoinExecutorTestRunner, SimpleInnerJoin)
try
{
    auto request = context.scan("simple_test", "t1")
                       .join(context.scan("simple_test", "t2"), {col("a")}, ASTTableJoin::Kind::Inner)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({{}, "3", {}, "3"}), toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({"3", "3", {}, {}})});
    }

    request = context.scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("a")}, ASTTableJoin::Kind::Inner)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({{}, "3", {}, "3"}), toNullableVec<String>({"1", "1", "1", "1"}), toNullableVec<String>({"3", "3", {}, {}})});
    }

    request = context.scan("simple_test", "t1")
                  .join(context.scan("simple_test", "t2"), {col("b")}, ASTTableJoin::Kind::Inner)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({{}, "1", "2", {}, "1"}), toNullableVec<String>({"3", "3", "4", "3", "3"}), toNullableVec<String>({"1", "1", "3", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3"})});
    }

    request = context.scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("b")}, ASTTableJoin::Kind::Inner)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({{}, "1", "3", {}, "1"}), toNullableVec<String>({"3", "3", "4", "3", "3"}), toNullableVec<String>({"1", "1", "2", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3"})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, SimpleLeftJoin)
try
{
    auto request = context.scan("simple_test", "t1")
                       .join(context.scan("simple_test", "t2"), {col("a")}, ASTTableJoin::Kind::Left)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}), toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})});
    }

    request = context.scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("a")}, ASTTableJoin::Kind::Left)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}}), toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}})});
    }

    request = context.scan("simple_test", "t1")
                  .join(context.scan("simple_test", "t2"), {col("b")}, ASTTableJoin::Kind::Left)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})});
    }

    request = context.scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("b")}, ASTTableJoin::Kind::Left)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, SimpleRightJoin)
try
{
    auto request = context
                       .scan("simple_test", "t1")
                       .join(context.scan("simple_test", "t2"), {col("a")}, ASTTableJoin::Kind::Right)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}), toNullableVec<String>({"1", "1", "3", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})});
    }

    request = context
                  .scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("a")}, ASTTableJoin::Kind::Right)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"1", "1", {}, {}, "1", "1", {}}), toNullableVec<String>({{}, "3", {}, {}, {}, "3", {}}), toNullableVec<String>({"1", "1", "2", {}, "1", "1", {}}), toNullableVec<String>({"3", "3", "4", "3", {}, {}, {}})});
    }

    request = context
                  .scan("simple_test", "t1")
                  .join(context.scan("simple_test", "t2"), {col("b")}, ASTTableJoin::Kind::Right)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({{}, "1", "2", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({"1", "1", "3", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})});
    }

    request = context
                  .scan("simple_test", "t2")
                  .join(context.scan("simple_test", "t1"), {col("b")}, ASTTableJoin::Kind::Right)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({{}, "1", "3", {}, "1", {}, {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}}), toNullableVec<String>({"1", "1", "2", {}, {}, "1", {}}), toNullableVec<String>({"3", "3", "4", "3", "3", {}, {}})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiInnerLeftJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Inner)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Inner),
                             {col("b")},
                             ASTTableJoin::Kind::Left)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiInnerRightJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Inner)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Inner),
                             {col("b")},
                             ASTTableJoin::Kind::Right)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiLeftInnerJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Left)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Left),
                             {col("b")},
                             ASTTableJoin::Kind::Inner)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({1, 1, 3, 3, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 3, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, 3, 3, 3, 3, 0}), toNullableVec<Int32>({{}, {}, 4, 4, 2, 2, 0}), toNullableVec<Int32>({{}, {}, 5, 5, 3, 3, 0}), toNullableVec<Int32>({1, 2, 1, 2, 1, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, 2, {}, 2, {}, 2, 0}), toNullableVec<Int32>({{}, 2, {}, 2, {}, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiLeftRightJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Left)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Left),
                             {col("b")},
                             ASTTableJoin::Kind::Right)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({1, 3, 3, 1, 3, 3, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({3, 2, 2, 3, 2, 2, 0}), toNullableVec<Int32>({{}, 3, 3, {}, 3, 3, 0}), toNullableVec<Int32>({{}, 4, 2, {}, 4, 2, 0}), toNullableVec<Int32>({{}, 5, 3, {}, 5, 3, 0}), toNullableVec<Int32>({1, 1, 1, 2, 2, 2, 0}), toNullableVec<Int32>({2, 2, 2, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, {}, 2, 2, 2, 0}), toNullableVec<Int32>({{}, {}, {}, 2, 2, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiRightInnerJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Right)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Right),
                             {col("b")},
                             ASTTableJoin::Kind::Inner)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, MultiRightLeftJoin)
try
{
    auto [t1, t2, t3, t4] = multiTestScan();
    auto request = t1.join(t2, {col("a")}, ASTTableJoin::Kind::Right)
                       .join(t3.join(t4, {col("a")}, ASTTableJoin::Kind::Right),
                             {col("b")},
                             ASTTableJoin::Kind::Left)
                       .build(context);

    executeWithConcurrency(request, {toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({3, 3, 0}), toNullableVec<Int32>({4, 2, 0}), toNullableVec<Int32>({5, 3, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0}), toNullableVec<Int32>({2, 2, 0})});
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinCast)
try
{
    auto cast_request = [&]() {
        return context.scan("cast", "t1")
            .join(context.scan("cast", "t2"), {col("a")}, ASTTableJoin::Kind::Inner)
            .build(context);
    };

    /// int(1) == float(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeFloat}}, {toVec<Float32>("a", {1.0})});

    executeWithConcurrency(cast_request(), {toNullableVec<Int32>({1}), toNullableVec<Float32>({1.0})});

    /// int(1) == double(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLong}}, {toVec<Int32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeDouble}}, {toVec<Float64>("a", {1.0})});

    executeWithConcurrency(cast_request(), {toNullableVec<Int32>({1}), toNullableVec<Float64>({1.0})});

    /// float(1) == double(1.0)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeFloat}}, {toVec<Float32>("a", {1})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeDouble}}, {toVec<Float64>("a", {1})});

    executeWithConcurrency(cast_request(), {toNullableVec<Float32>({1}), toNullableVec<Float64>({1})});

    /// varchar('x') == char('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeString}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeWithConcurrency(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// tinyblob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeTinyBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeWithConcurrency(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// mediumBlob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeMediumBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeWithConcurrency(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// blob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeWithConcurrency(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// longBlob('x') == varchar('x')
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeLongBlob}}, {toVec<String>("a", {"x"})});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeVarchar}}, {toVec<String>("a", {"x"})});

    executeWithConcurrency(cast_request(), {toNullableVec<String>({"x"}), toNullableVec<String>({"x"})});

    /// decimal with different scale
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeNewDecimal}}, {createColumn<Decimal256>(std::make_tuple(9, 4), {"0.12"}, "a")});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeNewDecimal}}, {createColumn<Decimal256>(std::make_tuple(9, 3), {"0.12"}, "a")});

    executeWithConcurrency(cast_request(), {createNullableColumn<Decimal256>(std::make_tuple(65, 0), {"0.12"}, {0}), createNullableColumn<Decimal256>(std::make_tuple(65, 0), {"0.12"}, {0})});

    /// datetime(1970-01-01 00:00:01) == timestamp(1970-01-01 00:00:01)
    context.addMockTable("cast", "t1", {{"a", TiDB::TP::TypeDatetime}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    context.addMockTable("cast", "t2", {{"a", TiDB::TP::TypeTimestamp}}, {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 6)});

    executeWithConcurrency(cast_request(), {createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0), createDateTimeColumn({{{1970, 1, 1, 0, 0, 1, 0}}}, 0)});
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinAgg)
try
{
    auto request = context.scan("join_agg", "t1")
                       .join(context.scan("join_agg", "t2"), {col("a")}, ASTTableJoin::Kind::Inner)
                       .aggregation({Max(col("a")), Min(col("a")), Count(col("a"))}, {col("b")})
                       .build(context);

    {
        executeWithConcurrency(request, {toNullableVec<Int32>({4}), toNullableVec<Int32>({1}), toVec<UInt64>({3}), toNullableVec<Int32>({1})});
    }

    request = context.scan("join_agg", "t1")
                  .join(context.scan("join_agg", "t2"), {col("a")}, ASTTableJoin::Kind::Left)
                  .aggregation({Max(col("a")), Min(col("a")), Count(col("a"))}, {col("b")})
                  .build(context);

    {
        executeWithConcurrency(request, {toNullableVec<Int32>({4, 3}), toNullableVec<Int32>({1, 3}), toVec<UInt64>({3, 1}), toNullableVec<Int32>({1, 4})});
    }

    request = context.scan("join_agg", "t1")
                  .join(context.scan("join_agg", "t2"), {col("a")}, ASTTableJoin::Kind::Right)
                  .aggregation({Max(col("a")), Min(col("a")), Count(col("a"))}, {col("b")})
                  .build(context);

    {
        executeWithConcurrency(request, {toNullableVec<Int32>({4, {}}), toNullableVec<Int32>({1, {}}), toVec<UInt64>({3, 0}), toNullableVec<Int32>({1, {}})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithTableScan)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.scan("test_db", "r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }

    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                  .project({"s", "join_c"})
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }

    request = context
                  .scan("test_db", "l_table")
                  .join(context.scan("test_db", "r_table_2"), {col("join_c")}, ASTTableJoin::Kind::Left)
                  .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"banana", "banana", "banana", "banana"}), toNullableVec<String>({"apple", "apple", "apple", "banana"}), toNullableVec<String>({"banana", "banana", "banana", {}}), toNullableVec<String>({"apple", "apple", "apple", {}})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithExchangeReceiver)
try
{
    auto request = context
                       .receive("exchange_l_table")
                       .join(context.receive("exchange_r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }
}
CATCH

TEST_F(JoinExecutorTestRunner, JoinWithTableScanAndReceiver)
try
{
    auto request = context
                       .scan("test_db", "l_table")
                       .join(context.receive("exchange_r_table"), {col("join_c")}, ASTTableJoin::Kind::Left)
                       .build(context);
    {
        executeWithConcurrency(request, {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"}), toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    }
}
CATCH

} // namespace tests
} // namespace DB
