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
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class FilterExecutorTestRunner : public DB::tests::ExecutorTest
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

        context.addExchangeReceiver(
            "exchange1",
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "filter"},
            {
                {"int8_col", TiDB::TP::TypeTiny},
                {"int32_col", TiDB::TP::TypeLong},
                {"int64_col", TiDB::TP::TypeLongLong},
                {"float_col", TiDB::TP::TypeFloat},
                {"double_col", TiDB::TP::TypeDouble},
                {"string_col", TiDB::TP::TypeString},
            },
            {
                toNullableVec<Int8>("int8_col", {0, 1, 0, 1, 1, 0, 1, 0}),
                toNullableVec<Int32>("int32_col", {0, 1, 2, 3, 4, 5, 6, 7}),
                toNullableVec<Int64>("int64_col", {0, 1, 2, 3, 4, 5, 6, 7}),
                toNullableVec<Float32>("float_col", {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7}),
                toNullableVec<Float64>("double_col", {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7}),
                toNullableVec<String>("string_col", {"", "a", "1", "0", "ab", "  ", "\t", "\n"}),
            });

        // with 200 rows.
        std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
        std::vector<std::optional<String>> value(200);
        for (size_t i = 0; i < 200; ++i)
        {
            key[i] = i % 15;
            value[i] = {fmt::format("val_{}", i)};
        }
        context.addMockTable(
            {"test_db", "big_table"},
            {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
    }
};

TEST_F(FilterExecutorTestRunner, equals)
try
{
    auto request = context.scan("test_db", "test_table").filter(eq(col("s1"), col("s2"))).build(context);
    executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    request = context.receive("exchange1").filter(eq(col("s1"), col("s2"))).build(context);
    executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    request = context.receive("exchange1").filter(eq(col("s1"), col("s1"))).build(context);
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});

    request = context.receive("exchange1").filter(eq(col("s1"), lit(Field(String("0"))))).build(context);
    executeAndAssertColumnsEqual(request, {});
}
CATCH

TEST_F(FilterExecutorTestRunner, andOr)
try
{
    auto test_one = [&](const ASTPtr & condition, const ColumnsWithTypeAndName & expect_columns) {
        auto request = context.receive("exchange1").filter(condition).build(context);
        executeAndAssertColumnsEqual(request, expect_columns);
    };

    auto const_true = lit(Field(static_cast<UInt64>(1)));
    auto const_false = lit(Field(static_cast<UInt64>(0)));
    test_one(
        const_true,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_one(const_false, {});

    auto column_not_null_true = eq(col("s1"), col("s1"));
    auto column_false = eq(col("s1"), lit(Field(String("0"))));
    auto column_other = eq(col("s1"), col("s2"));
    test_one(
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_one(column_false, {});
    test_one(column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    // and
    auto test_and = [&](const ASTPtr & a, const ASTPtr & b, const ColumnsWithTypeAndName & expect_columns) {
        auto request = context.receive("exchange1").filter(And(a, b)).build(context);
        executeAndAssertColumnsEqual(request, expect_columns);

        request = context.receive("exchange1").filter(And(b, a)).build(context);
        executeAndAssertColumnsEqual(request, expect_columns);
    };

    test_and(
        const_true,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_and(const_true, column_false, {});
    test_and(const_true, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    test_and(const_false, column_not_null_true, {});
    test_and(const_false, column_false, {});
    test_and(const_false, column_other, {});

    test_and(
        column_not_null_true,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_and(column_not_null_true, column_false, {});
    test_and(
        column_not_null_true,
        column_other,
        {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
    test_and(column_false, column_not_null_true, {});
    test_and(column_false, column_false, {});
    test_and(column_false, column_other, {});
    test_and(
        column_other,
        column_not_null_true,
        {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
    test_and(column_other, column_false, {});
    test_and(column_other, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    test_and(
        const_true,
        const_true,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_and(const_false, const_true, {});
    test_and(const_false, const_false, {});

    // or
    auto test_or = [&](const ASTPtr & a, const ASTPtr & b, const ColumnsWithTypeAndName & expect_columns) {
        auto request = context.receive("exchange1").filter(Or(a, b)).build(context);
        executeAndAssertColumnsEqual(request, expect_columns);

        request = context.receive("exchange1").filter(Or(b, a)).build(context);
        executeAndAssertColumnsEqual(request, expect_columns);
    };

    test_or(
        const_true,
        column_not_null_true,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(
        const_true,
        column_false,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(
        const_true,
        column_other,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});

    test_or(
        const_false,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(const_false, column_false, {});
    test_or(const_false, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    test_or(
        column_not_null_true,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(
        column_not_null_true,
        column_false,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(
        column_not_null_true,
        column_other,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(
        column_false,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(column_false, column_false, {});
    test_or(column_false, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
    test_or(
        column_other,
        column_not_null_true,
        {toNullableVec<String>({"banana", "banana"}), toNullableVec<String>({"apple", "banana"})});
    test_or(column_other, column_false, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
    test_or(column_other, column_other, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});

    test_or(
        const_true,
        const_true,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(
        const_false,
        const_true,
        {toNullableVec<String>({"banana", {}, "banana"}), toNullableVec<String>({"apple", {}, "banana"})});
    test_or(const_false, const_false, {});
}
CATCH

TEST_F(FilterExecutorTestRunner, FilterWithQualifiedFormat)
try
{
    auto request
        = context.scan("test_db", "test_table").filter(eq(col("test_table.s1"), col("test_table.s2"))).build(context);
    executeAndAssertColumnsEqual(request, {toNullableVec<String>({"banana"}), toNullableVec<String>({"banana"})});
}
CATCH

TEST_F(FilterExecutorTestRunner, convert_bool)
try
{
    {
        auto request
            = context.scan("test_db", "filter").filter(col("int8_col")).project({col("int8_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int8>({1, 1, 1, 1})});
    }
    {
        auto request
            = context.scan("test_db", "filter").filter(col("int32_col")).project({col("int32_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int32>({1, 2, 3, 4, 5, 6, 7})});
    }
    {
        auto request
            = context.scan("test_db", "filter").filter(col("int64_col")).project({col("int64_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Int64>({1, 2, 3, 4, 5, 6, 7})});
    }
    {
        auto request
            = context.scan("test_db", "filter").filter(col("float_col")).project({col("float_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Float32>({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7})});
    }
    {
        auto request
            = context.scan("test_db", "filter").filter(col("double_col")).project({col("double_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<Float64>({0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7})});
    }
    {
        auto request
            = context.scan("test_db", "filter").filter(col("string_col")).project({col("string_col")}).build(context);
        executeAndAssertColumnsEqual(request, {toNullableVec<String>({"1"})});
    }
}
CATCH

TEST_F(FilterExecutorTestRunner, BigTable)
try
{
    auto request = context.scan("test_db", "big_table")
                       .filter(gt(col("key"), lit(Field(static_cast<UInt64>(7)))))
                       .build(context);
    auto expect = executeStreams(request, 1);
    executeAndAssertColumnsEqual(request, expect);
}
CATCH

TEST_F(FilterExecutorTestRunner, PushDownExecutor)
try
{
    context.mockStorage()->setUseDeltaMerge(true);
    context.addMockDeltaMerge(
        {"test_db", "test_table1"},
        {{"i1", TiDB::TP::TypeLongLong}, {"s2", TiDB::TP::TypeString}, {"b3", TiDB::TP::TypeTiny}},
        {toVec<Int64>("i1", {1, 2, 3}),
         toNullableVec<String>("s2", {"apple", {}, "banana"}),
         toVec<Int8>("b3", {true, false, true})});

    auto request = context.scan("test_db", "test_table1")
                       .filter(lt(col("i1"), lit(Field(static_cast<Int64>(2)))))
                       .build(context);

    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<Int64>({1}), toNullableVec<String>({"apple"}), toNullableVec<Int8>({true})});

    request = context.scan("test_db", "test_table1").filter(col("b3")).build(context);

    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<Int64>({1, 3}), toNullableVec<String>({"apple", "banana"}), toNullableVec<Int8>({true, true})});

    request = context.scan("test_db", "test_table1")
                  .filter(lt(col("i1"), lit(Field(static_cast<Int64>(3)))))
                  .build(context);

    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<Int64>({1, 2}), toNullableVec<String>({"apple", {}}), toNullableVec<Int8>({true, false})});

    for (size_t i = 4; i < 10; ++i)
    {
        request = context.scan("test_db", "test_table1")
                      .filter(lt(col("i1"), lit(Field(static_cast<Int64>(i)))))
                      .build(context);

        executeAndAssertColumnsEqual(
            request,
            {toNullableVec<Int64>({1, 2, 3}),
             toNullableVec<String>({"apple", {}, "banana"}),
             toNullableVec<Int8>({true, false, true})});
    }

    for (size_t i = 0; i < 10; ++i)
    {
        request = context.scan("test_db", "test_table1")
                      .filter(gt(col("i1"), lit(Field(static_cast<Int64>(-i)))))
                      .build(context);

        executeAndAssertColumnsEqual(
            request,
            {toNullableVec<Int64>({1, 2, 3}),
             toNullableVec<String>({"apple", {}, "banana"}),
             toNullableVec<Int8>({true, false, true})});
    }

    for (size_t i = 0; i < 10; ++i)
    {
        request = context.scan("test_db", "test_table1")
                      .filter(gt(col("i1"), lit(Field(static_cast<Int64>(-i)))))
                      .project({col("i1")})
                      .build(context);

        executeAndAssertColumnsEqual(request, {toNullableVec<Int64>({1, 2, 3})});
    }

    context.mockStorage()->setUseDeltaMerge(false);
}
CATCH

} // namespace tests
} // namespace DB
