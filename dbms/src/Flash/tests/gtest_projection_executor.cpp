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

class ProjectionExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            {db_name, table_name},
            {{col_names[0], TiDB::TP::TypeString},
             {col_names[1], TiDB::TP::TypeString},
             {col_names[2], TiDB::TP::TypeString},
             {col_names[3], TiDB::TP::TypeLong},
             {col_names[4], TiDB::TP::TypeLong}},
            {toNullableVec<String>(col_names[0], col0),
             toNullableVec<String>(col_names[1], col1),
             toNullableVec<String>(col_names[2], col2),
             toNullableVec<Int32>(col_names[3], col3),
             toNullableVec<Int32>(col_names[4], col4)});

        context.addMockTable(
            {"test_db", "test_table2"},
            {{"s1", TiDB::TP::TypeString},
             {"s2", TiDB::TP::TypeString},
             {"s3", TiDB::TP::TypeLongLong},
             {"s4", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("s1", {"a1", "", "a3", {}, "a5"}),
             toNullableVec<String>("s2", {"", "a2", "a3", "a4", {}}),
             toNullableVec<Int64>("s3", {2, {}, 4, 5, 6}),
             toNullableVec<Int64>("s4", {1, 2, 3, {}, 5})});

        context.addMockTable(
            {"test_db", "test_table3"},
            {{"s1", TiDB::TP::TypeLongLong},
             {"s2", TiDB::TP::TypeLongLong},
             {"s3", TiDB::TP::TypeLongLong},
             {"s4", TiDB::TP::TypeLongLong}},
            {toNullableVec<Int64>("s1", {1, 1, 1, 1, 1}),
             toNullableVec<Int64>("s2", {1, 1, 1, 1, 1}),
             toNullableVec<Int64>("s3", {1, 1, 1, 1, 1}),
             toNullableVec<Int64>("s4", {1, 1, 1, 1, 1})});

        // with 200 rows.
        std::vector<std::optional<TypeTraits<Int64>::FieldType>> key(200);
        std::vector<std::optional<String>> value(200);
        for (size_t i = 0; i < 200; ++i)
        {
            key[i] = i % 15;
            value[i] = {fmt::format("val_{}", i)};
        }
        context.addMockTable(
            {"test_db", "big_table"},
            {{"key", TiDB::TP::TypeLongLong}, {"value", TiDB::TP::TypeString}},
            {toNullableVec<Int64>("key", key), toNullableVec<String>("value", value)});
    }

    template <typename T>
    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(T param)
    {
        return context.scan(db_name, table_name).project(param).build(context);
    };

    /// Prepare column data
    const ColumnWithNullableString col0{"col0-0", "col0-1", "", "col0-2", {}, "col0-3", ""};
    const ColumnWithNullableString col1{"col1-0", {}, "", "col1-1", "", "col1-2", "col1-3"};
    const ColumnWithNullableString col2{"", "col2-0", "col2-1", {}, "col2-3", {}, "col2-4"};
    const ColumnWithNullableInt32 col3{1, {}, 0, -111111, {}, 0, 9999};

    /** Each value in col4 should be different from each other so that topn 
     *  could sort the columns into an unique result, or multi-results could
     *  be right.
     */
    const ColumnWithNullableInt32 col4{0, 5, -123, -234, {}, 24353, 9999};

    /// Results after sorted by col4
    const ColumnWithNullableString col0_sorted_asc{{}, "col0-2", "", "col0-0", "col0-1", "", "col0-3"};
    const ColumnWithNullableString col1_sorted_asc{"", "col1-1", "", "col1-0", {}, "col1-3", "col1-2"};
    const ColumnWithNullableString col2_sorted_asc{"col2-3", {}, "col2-1", "", "col2-0", "col2-4", {}};
    const ColumnWithNullableInt32 col3_sorted_asc{{}, -111111, 0, 1, {}, 9999, 0};
    const ColumnWithNullableInt32 col4_sorted_asc{{}, -234, -123, 0, 5, 9999, 24353};

    /// Prepare some names
    std::vector<String> col_names{"col0", "col1", "col2", "col3", "col4"};
    const String db_name{"test_db"};
    const String table_name{"projection_test_table"};
};

TEST_F(ProjectionExecutorTestRunner, Projection)
try
{
    /// Check single column
    auto request = buildDAGRequest<MockColumnNameVec>({col_names[4]});
    executeAndAssertColumnsEqual(request, {toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Check multi columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[0], col_names[4]});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>(col_names[0], col0_sorted_asc), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Check multi columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[0], col_names[1], col_names[4]});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<String>(col_names[0], col0_sorted_asc),
         toNullableVec<String>(col_names[1], col1_sorted_asc),
         toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Check duplicate columns
    request = buildDAGRequest<MockColumnNameVec>({col_names[4], col_names[4], col_names[4]});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<Int32>(col_names[4], col4_sorted_asc),
         toNullableVec<Int32>(col_names[4], col4_sorted_asc),
         toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    {
        /// Check large number of columns
        const size_t col_num = 100;
        MockColumnNameVec projection_input;
        ColumnsWithTypeAndName columns;
        auto expect_column = toNullableVec<Int32>(col_names[4], col4_sorted_asc);

        for (size_t i = 0; i < col_num; ++i)
        {
            projection_input.push_back(col_names[4]);
            columns.push_back(expect_column);
        }

        request = buildDAGRequest<MockColumnNameVec>(projection_input);
        executeAndAssertColumnsEqual(request, columns);
    }
}
CATCH

TEST_F(ProjectionExecutorTestRunner, ProjectionFunction)
try
{
    std::shared_ptr<tipb::DAGRequest> request;

    /// Test "equal" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({eq(col(col_names[0]), col(col_names[0])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 1, 1, 1, 1, 1, 1}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({eq(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 1, 0, {}, 0, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({eq(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 0, 0, {}, 1, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});


    /// Test "greater" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({gt(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 0, 0, {}, 0, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({gt(col(col_names[1]), col(col_names[0])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 1, 0, 1, {}, 1, 1}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({gt(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 1, 1, {}, 0, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({gt(col(col_names[4]), col(col_names[3])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 1, 0, 0, {}, 0, 1}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});


    /// Test "and" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>({And(col(col_names[0]), col(col_names[0])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 0, 0, 0, 0, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    request = buildDAGRequest<MockAstVec>({And(col(col_names[0]), col(col_names[1])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({0, 0, 0, 0, 0, 0, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({And(col(col_names[3]), col(col_names[4])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 1, 0, 0, {}, 1, 0}), toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Test "not" function

    /// Data type: TypeString
    request = buildDAGRequest<MockAstVec>(
        {NOT(col(col_names[0])), NOT(col(col_names[1])), NOT(col(col_names[2])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 1, 1, 1, 1, 1, 1}),
         toNullableVec<UInt64>({1, 1, 1, 1, {}, 1, 1}),
         toNullableVec<UInt64>({1, {}, 1, 1, 1, 1, {}}),
         toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// Data type: TypeLong
    request = buildDAGRequest<MockAstVec>({NOT(col(col_names[3])), NOT(col(col_names[4])), col(col_names[4])});
    executeAndAssertColumnsEqual(
        request,
        {toNullableVec<UInt64>({{}, 0, 1, 0, {}, 0, 1}),
         toNullableVec<UInt64>({{}, 0, 0, 1, 0, 0, 0}),
         toNullableVec<Int32>(col_names[4], col4_sorted_asc)});

    /// TODO more functions...
}
CATCH

TEST_F(ProjectionExecutorTestRunner, MultiFunction)
try
{
    MockAstVec functions = {
        concat(col("s1"), col("s2")),
        plusInt(col("s3"), col("s4")),
        minusInt(col("s3"), col("s4")),
    };
    ColumnsWithTypeAndName functions_result
        = {toNullableVec<String>("concat", {"a1", "a2", "a3a3", {}, {}}),
           toNullableVec<Int64>("plusInt", {3, {}, 7, {}, 11}),
           toNullableVec<Int64>("minusInt", {1, {}, 1, {}, 1})};
    auto test_single_function = [&](size_t index) {
        auto req = context.scan("test_db", "test_table2").project({functions[index]}).build(context);
        executeAndAssertColumnsEqual(req, {functions_result[index]});
    };
    for (size_t i = 0; i < functions.size(); ++i)
        test_single_function(i);

    auto multi_functions = [&](const MockAstVec & fs) {
        return context.scan("test_db", "test_table2").project(fs).build(context);
    };

    auto multi_functions_then_agg = [&](const MockAstVec & fs) {
        return context.scan("test_db", "test_table2")
            .project(fs)
            .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
            .build(context);
    };

    MockAstVec fs;
    ColumnsWithTypeAndName fs_result;
    for (size_t i = 0; i < functions.size(); ++i)
    {
        fs.push_back(functions[i]);
        fs_result.push_back(functions_result[i]);
        for (size_t j = 0; j < functions.size(); ++j)
        {
            fs.push_back(functions[j]);
            fs_result.push_back(functions_result[j]);
            for (size_t k = 0; k < functions.size(); ++k)
            {
                fs.push_back(functions[k]);
                fs_result.push_back(functions_result[k]);

                executeAndAssertColumnsEqual(multi_functions(fs), fs_result);
                executeAndAssertColumnsEqual(multi_functions_then_agg(fs), createColumns({toVec<UInt64>({5})}));

                fs.pop_back();
                fs_result.pop_back();
            }
            fs.pop_back();
            fs_result.pop_back();
        }
        fs.pop_back();
        fs_result.pop_back();
    }

    auto req = context.scan("test_db", "test_table3")
                   .project(
                       {plusInt(col("s1"), col("s2")),
                        plusInt(plusInt(col("s1"), col("s2")), col("s3")),
                        plusInt(plusInt(plusInt(col("s1"), col("s2")), col("s3")), col("s4"))})
                   .build(context);
    executeAndAssertColumnsEqual(
        req,
        {toNullableVec<Int64>({2, 2, 2, 2, 2}),
         toNullableVec<Int64>({3, 3, 3, 3, 3}),
         toNullableVec<Int64>({4, 4, 4, 4, 4})});
}
CATCH

TEST_F(ProjectionExecutorTestRunner, MultiProjection)
try
{
    auto req = context.scan("test_db", "test_table3")
                   .project({col("s1"), col("s2"), col("s3"), col("s4")})
                   .project({col("s1"), col("s2"), col("s3")})
                   .project({col("s1"), col("s2")})
                   .project({col("s1")})
                   .build(context);
    executeAndAssertColumnsEqual(req, {toNullableVec<Int64>({1, 1, 1, 1, 1})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1"), col("s2"), col("s3"), col("s4"), plusInt(col("s1"), col("s2"))})
              .project({col("s1"), col("s2"), col("s3"), col("s4"), plusInt(plusInt(col("s1"), col("s2")), col("s3"))})
              .project({plusInt(plusInt(plusInt(col("s1"), col("s2")), col("s3")), col("s4"))})
              .build(context);
    executeAndAssertColumnsEqual(req, {toNullableVec<Int64>({4, 4, 4, 4, 4})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1"), col("s2"), col("s3"), col("s4")})
              .project({col("s1"), col("s2"), col("s3"), col("s4")})
              .build(context);
    executeAndAssertColumnsEqual(
        req,
        {toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1})});

    req = context.scan("test_db", "test_table3").project({lit(Field(String("a")))}).build(context);
    executeAndAssertColumnsEqual(req, {createColumns({createConstColumn<String>(5, "a")})});

    req = context.scan("test_db", "test_table3")
              .project(MockAstVec{})
              .project(MockAstVec{})
              .project({lit(Field(String("a")))})
              .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({createConstColumn<String>(5, "a")})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1"), col("s2"), col("s3"), col("s4")})
              .project(MockAstVec{})
              .project({lit(Field(String("a")))})
              .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({createConstColumn<String>(5, "a")})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1")})
              .project({col("s1"), col("s1"), col("s1"), col("s1"), col("s1")})
              .build(context);
    executeAndAssertColumnsEqual(
        req,
        {toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1}),
         toNullableVec<Int64>({1, 1, 1, 1, 1})});
}
CATCH

TEST_F(ProjectionExecutorTestRunner, ProjectionThenAgg)
try
{
    auto req = context.scan("test_db", "test_table3")
                   .project({col("s1"), col("s2"), col("s3"), col("s4")})
                   .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                   .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({toVec<UInt64>({5})})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1"), col("s2"), col("s3"), col("s4")})
              .project({col("s1"), col("s2"), col("s3")})
              .project({col("s1"), col("s2")})
              .project({col("s1")})
              .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
              .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({toVec<UInt64>({5})})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1"), col("s2"), col("s3"), col("s4"), plusInt(col("s1"), col("s2"))})
              .project({col("s1"), col("s2"), col("s3"), col("s4"), plusInt(plusInt(col("s1"), col("s2")), col("s3"))})
              .project({plusInt(plusInt(plusInt(col("s1"), col("s2")), col("s3")), col("s4"))})
              .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
              .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({toVec<UInt64>({5})})});

    req = context.scan("test_db", "test_table3")
              .project(
                  {plusInt(col("s1"), col("s2")),
                   plusInt(plusInt(col("s1"), col("s2")), col("s3")),
                   plusInt(plusInt(plusInt(col("s1"), col("s2")), col("s3")), col("s4"))})
              .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
              .build(context);
    executeAndAssertColumnsEqual(req, {createColumns({toVec<UInt64>({5})})});

    req = context.scan("test_db", "test_table3")
              .project({col("s1")})
              .aggregation({Count(col("s1")), Sum(col("s1")), Max(col("s1")), Min(col("s1"))}, {})
              .build(context);
    executeAndAssertColumnsEqual(
        req,
        {toVec<UInt64>({5}), toVec<UInt64>({5}), toNullableVec<Int64>({1}), toNullableVec<Int64>({1})});
}
CATCH

TEST_F(ProjectionExecutorTestRunner, BigTable)
try
{
    auto request
        = context.scan("test_db", "big_table")
              .project({plusInt(col("key"), lit(Field(static_cast<UInt64>(7)))), concat(col("value"), col("value"))})
              .build(context);
    auto expect = executeStreams(request, 1);
    executeAndAssertColumnsEqual(request, expect);
}
CATCH

} // namespace tests
} // namespace DB
