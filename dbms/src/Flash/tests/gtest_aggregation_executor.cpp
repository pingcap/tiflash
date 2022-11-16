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
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

#define DT DecimalField<Decimal32>
#define COL_GROUP2(a, b)                               \
    {                                                  \
        col(types_col_name[a]), col(types_col_name[b]) \
    }
#define COL_PROJ2(a, b)                      \
    {                                        \
        types_col_name[a], types_col_name[b] \
    }

class ExecutorAggTestRunner : public ExecutorTest
{
public:
    using ColStringNullableType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColInt8NullableType = std::optional<typename TypeTraits<Int8>::FieldType>;
    using ColInt16NullableType = std::optional<typename TypeTraits<Int16>::FieldType>;
    using ColInt32NullableType = std::optional<typename TypeTraits<Int32>::FieldType>;
    using ColInt64NullableType = std::optional<typename TypeTraits<Int64>::FieldType>;
    using ColFloat32NullableType = std::optional<typename TypeTraits<Float32>::FieldType>;
    using ColFloat64NullableType = std::optional<typename TypeTraits<Float64>::FieldType>;
    using ColMyDateNullableType = std::optional<typename TypeTraits<MyDate>::FieldType>;
    using ColMyDateTimeNullableType = std::optional<typename TypeTraits<MyDateTime>::FieldType>;
    using ColDecimalNullableType = std::optional<typename TypeTraits<Decimal32>::FieldType>;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
    using ColFloat64Type = typename TypeTraits<Float64>::FieldType;
    using ColStringType = typename TypeTraits<String>::FieldType;

    using ColumnWithNullableString = std::vector<ColStringNullableType>;
    using ColumnWithNullableInt8 = std::vector<ColInt8NullableType>;
    using ColumnWithNullableInt16 = std::vector<ColInt16NullableType>;
    using ColumnWithNullableInt32 = std::vector<ColInt32NullableType>;
    using ColumnWithNullableInt64 = std::vector<ColInt64NullableType>;
    using ColumnWithNullableFloat32 = std::vector<ColFloat32NullableType>;
    using ColumnWithNullableFloat64 = std::vector<ColFloat64NullableType>;
    using ColumnWithNullableMyDate = std::vector<ColMyDateNullableType>;
    using ColumnWithNullableMyDateTime = std::vector<ColMyDateTimeNullableType>;
    using ColumnWithNullableDecimal = std::vector<ColDecimalNullableType>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;
    using ColumnWithFloat64 = std::vector<ColFloat64Type>;
    using ColumnWithString = std::vector<ColStringType>;

    ~ExecutorAggTestRunner() override = default;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        /// Create table for tests of group by
        context.addMockTable(/* name= */ {db_name, table_types},
                             /* columnInfos= */
                             {{types_col_name[0], TiDB::TP::TypeLong},
                              {types_col_name[1], TiDB::TP::TypeDecimal},
                              {types_col_name[2], TiDB::TP::TypeTiny},
                              {types_col_name[3], TiDB::TP::TypeShort},
                              {types_col_name[4], TiDB::TP::TypeLong},
                              {types_col_name[5], TiDB::TP::TypeLongLong},
                              {types_col_name[6], TiDB::TP::TypeFloat},
                              {types_col_name[7], TiDB::TP::TypeDouble},
                              {types_col_name[8], TiDB::TP::TypeDate},
                              {types_col_name[9], TiDB::TP::TypeDatetime},
                              {types_col_name[10], TiDB::TP::TypeString}},
                             /* columns= */
                             {toNullableVec<Int32>(types_col_name[0], col_id),
                              toNullableVec<Decimal32>(types_col_name[1], col_decimal),
                              toNullableVec<Int8>(types_col_name[2], col_tinyint),
                              toNullableVec<Int16>(types_col_name[3], col_smallint),
                              toNullableVec<Int32>(types_col_name[4], col_int),
                              toNullableVec<Int64>(types_col_name[5], col_bigint),
                              toNullableVec<Float32>(types_col_name[6], col_float),
                              toNullableVec<Float64>(types_col_name[7], col_double),
                              toNullableVec<MyDate>(types_col_name[8], col_mydate),
                              toNullableVec<MyDateTime>(types_col_name[9], col_mydatetime),
                              toNullableVec<String>(types_col_name[10], col_string)});

        /// Create table for tests of aggregation functions
        context.addMockTable(/* name= */ {db_name, table_name},
                             /* columnInfos= */
                             {{col_name[0], TiDB::TP::TypeLong},
                              {col_name[1], TiDB::TP::TypeString},
                              {col_name[2], TiDB::TP::TypeString},
                              {col_name[3], TiDB::TP::TypeDouble},
                              {col_name[4], TiDB::TP::TypeLong}},
                             /* columns= */
                             {toNullableVec<Int32>(col_name[0], col_age),
                              toNullableVec<String>(col_name[1], col_gender),
                              toNullableVec<String>(col_name[2], col_country),
                              toNullableVec<Float64>(col_name[3], col_salary),
                              toVec<UInt64>(col_name[4], col_pr)});

        context.addMockTable({"aggnull_test", "t1"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeLongLong}, {"s2", TiDB::TP::TypeLongLong}},
                             {toVec<Int64>("s1", {1, 2, 3}),
                              toVec<Int64>("s2", {1, 2, 3})});

        context.addMockTable({"test_db", "test_table_not_null"},
                             {
                                 {"c1_i64", TiDB::TP::TypeLongLong},
                                 {"c2_f64", TiDB::TP::TypeDouble},
                                 {"c3_str", TiDB::TP::TypeString},
                                 {"c4_str", TiDB::TP::TypeString},
                                 {"c5_date_time", TiDB::TP::TypeDatetime},
                             },
                             {
                                 toVec<Int64>("c1_i64", {1, 2, 2}),
                                 toVec<Float64>("c2_f64", {1, 3, 3}),
                                 toVec<String>("c3_str", {"1", "4  ", "4 "}),
                                 toVec<String>("c4_str", {"1", "2  ", "2 "}),
                                 toVec<MyDateTime>("c5_date_time", {2000000, 12000000, 12000000}),
                             });
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(std::pair<String, String> src, MockAstVec agg_funcs, MockAstVec group_by_exprs, MockColumnNameVec proj)
    {
        /// We can filter the group by column with project operator.
        /// project is applied to get partial aggregation output, so that we can remove redundant outputs and compare results with less handwriting codes.
        return context.scan(src.first, src.second).aggregation(agg_funcs, group_by_exprs).project(proj).build(context);
    }

    const String db_name{"test_db"};

    /// Prepare some data and names for tests of group by
    const String table_types{"types"};
    const std::vector<String> types_col_name{"id", "decimal_", "tinyint_", "smallint_", "int_", "bigint_", "float_", "double_", "date_", "datetime_", "string_"};
    ColumnWithNullableInt32 col_id{1, 2, 3, 4, 5, 6, 7, 8, 9};
    ColumnWithNullableDecimal col_decimal{DT(55, 1), {}, DT(-24, 1), DT(40, 1), DT(-40, 1), DT(40, 1), {}, DT(55, 1), DT(0, 1)};
    ColumnWithNullableInt8 col_tinyint{1, 2, 3, {}, {}, 0, 0, -1, -2};
    ColumnWithNullableInt16 col_smallint{2, 3, {}, {}, 0, -1, -2, 4, 0};
    ColumnWithNullableInt32 col_int{4, {}, {}, 0, 123, -1, -1, 123, 4};
    ColumnWithNullableInt64 col_bigint{2, 2, {}, 0, -1, {}, -1, 0, 123};
    ColumnWithNullableFloat32 col_float{3.3, {}, 0, 4.0, 3.3, 5.6, -0.1, -0.1, {}};
    ColumnWithNullableFloat64 col_double{0.1, 0, 1.1, 1.1, 1.2, {}, {}, -1.2, -1.2};
    ColumnWithNullableMyDate col_mydate{1000000, 2000000, {}, 300000, 1000000, {}, 0, 2000000, {}};
    ColumnWithNullableMyDateTime col_mydatetime{2000000, 0, {}, 3000000, 1000000, {}, 0, 2000000, 1000000};
    ColumnWithNullableString col_string{{}, "pingcap", "PingCAP", {}, "PINGCAP", "PingCAP", {}, "Shanghai", "Shanghai"};

    /// Prepare some data and names for aggregation functions
    const String table_name{"clerk"};
    const std::vector<String> col_name{"age", "gender", "country", "salary", "pr"};
    ColumnWithNullableInt32 col_age{30, {}, 27, 32, 25, 36, {}, 22, 34};
    ColumnWithNullableString col_gender{
        "male",
        "female",
        "female",
        "male",
        "female",
        "female",
        "male",
        "female",
        "male",
    };
    ColumnWithNullableString col_country{"russia", "korea", "usa", "usa", "usa", "china", "china", "china", "china"};
    ColumnWithNullableFloat64 col_salary{1000.1, 1300.2, 0.3, {}, -200.4, 900.5, -999.6, 2000.7, -300.8};
    ColumnWithUInt64 col_pr{1, 2, 0, 3290124, 968933, 3125, 31236, 4327, 80000};
};

/// Guarantee the correctness of group by
TEST_F(ExecutorAggTestRunner, GroupBy)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    std::vector<MockAstVec> group_by_exprs;
    std::vector<MockColumnNameVec> projections;
    std::vector<ColumnsWithTypeAndName> expect_cols;
    size_t test_num;

    {
        /// group by single column
        group_by_exprs = {{col(types_col_name[2])}, {col(types_col_name[3])}, {col(types_col_name[4])}, {col(types_col_name[5])}, {col(types_col_name[6])}, {col(types_col_name[7])}, {col(types_col_name[8])}, {col(types_col_name[9])}, {col(types_col_name[10])}};
        projections = {{types_col_name[2]}, {types_col_name[3]}, {types_col_name[4]}, {types_col_name[5]}, {types_col_name[6]}, {types_col_name[7]}, {types_col_name[8]}, {types_col_name[9]}, {types_col_name[10]}};
        expect_cols = {
            {toNullableVec<Int8>(types_col_name[2], ColumnWithNullableInt8{-1, 2, {}, 0, 1, 3, -2})}, /// select tinyint_ from test_db.types group by tinyint_;
            {toNullableVec<Int16>(types_col_name[3], ColumnWithNullableInt16{-1, 2, -2, {}, 0, 4, 3})}, /// select smallint_ from test_db.types group by smallint_;
            {toNullableVec<Int32>(types_col_name[4], ColumnWithNullableInt32{-1, {}, 4, 0, 123})}, /// select int_ from test_db.types group by int_;
            {toNullableVec<Int64>(types_col_name[5], ColumnWithNullableInt64{2, -1, 0, 123, {}})}, /// select bigint_ from test_db.types group by bigint_;
            {toNullableVec<Float32>(types_col_name[6], ColumnWithNullableFloat32{0, 4, 3.3, {}, 5.6, -0.1})}, /// select float_ from test_db.types group by float_;
            {toNullableVec<Float64>(types_col_name[7], ColumnWithNullableFloat64{0, {}, -1.2, 1.1, 1.2, 0.1})}, /// select double_ from test_db.types group by double_;
            {toNullableVec<MyDate>(types_col_name[8], ColumnWithNullableMyDate{{}, 0, 300000, 1000000, 2000000})}, /// select date_ from test_db.types group by date_;
            {toNullableVec<MyDateTime>(types_col_name[9], ColumnWithNullableMyDateTime{{}, 0, 1000000, 2000000, 3000000})}, /// select datetime_ from test_db.types group by datetime_;
            {toNullableVec<String>(types_col_name[10], ColumnWithNullableString{{}, "pingcap", "PingCAP", "PINGCAP", "Shanghai"})}}; /// select string_ from test_db.types group by string_;
        test_num = expect_cols.size();
        ASSERT_EQ(group_by_exprs.size(), test_num);
        ASSERT_EQ(projections.size(), test_num);

        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair(db_name, table_types), {}, group_by_exprs[i], projections[i]);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }

    {
        /// group by two columns
        group_by_exprs = {COL_GROUP2(2, 6), COL_GROUP2(3, 9), COL_GROUP2(4, 7), COL_GROUP2(5, 10), COL_GROUP2(8, 9), COL_GROUP2(9, 10)};
        projections = {COL_PROJ2(2, 6), COL_PROJ2(3, 9), COL_PROJ2(4, 7), COL_PROJ2(5, 10), COL_PROJ2(8, 9), COL_PROJ2(9, 10)};
        expect_cols = {/// select tinyint_, float_ from test_db.types group by tinyint_, float_;
                       {toNullableVec<Int8>(types_col_name[2], ColumnWithNullableInt8{1, 2, {}, 3, 0, 0, -1, {}, -2}),
                        toNullableVec<Float32>(types_col_name[6], ColumnWithNullableFloat32{3.3, {}, 4, 0, -0.1, 5.6, -0.1, 3.3, {}})},
                       /// select smallint_, datetime_ from test_db.types group by smallint_, datetime_;
                       {toNullableVec<Int16>(types_col_name[3], ColumnWithNullableInt16{2, 3, {}, {}, 0, -1, -2, 4}),
                        toNullableVec<MyDateTime>(types_col_name[9], ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, {}, 0, 2000000})},
                       /// select int_, double_ from test_db.types group by int_, double_;
                       {toNullableVec<Int32>(types_col_name[4], ColumnWithNullableInt32{{}, 123, -1, 0, {}, 4, 4, 123}),
                        toNullableVec<Float64>(types_col_name[7], ColumnWithNullableFloat64{0, -1.2, {}, 1.1, 1.1, -1.2, 0.1, 1.2})},
                       /// select bigint_, string_ from test_db.types group by bigint_, string_;
                       {toNullableVec<Int64>(types_col_name[5], ColumnWithNullableInt64{-1, 0, 0, 123, 2, {}, -1, 2}),
                        toNullableVec<String>(types_col_name[10], ColumnWithNullableString{{}, {}, "Shanghai", "Shanghai", {}, "PingCAP", "PINGCAP", "pingcap"})},
                       /// select date_, datetime_ from test_db.types group by date_, datetime_;
                       {toNullableVec<MyDate>(types_col_name[8], ColumnWithNullableMyDate{1000000, 2000000, {}, 300000, 1000000, 0, 2000000, {}}),
                        toNullableVec<MyDateTime>(types_col_name[9], ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, 0, 2000000, 1000000})},
                       /// select datetime_, string_ from test_db.types group by datetime_, string_;
                       {toNullableVec<MyDateTime>(types_col_name[9], ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, 0, 2000000, 1000000}),
                        toNullableVec<String>(types_col_name[10], ColumnWithNullableString{{}, "pingcap", "PingCAP", {}, "PINGCAP", {}, "Shanghai", "Shanghai"})}};
        test_num = expect_cols.size();
        ASSERT_EQ(group_by_exprs.size(), test_num);
        ASSERT_EQ(projections.size(), test_num);

        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair(db_name, table_types), {}, group_by_exprs[i], projections[i]);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }

    /// TODO type: decimal, enum and unsigned numbers
}
CATCH

TEST_F(ExecutorAggTestRunner, AggregationMaxAndMin)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func0 = Max(col(col_name[0])); /// select max(age) from clerk group by country;
    auto agg_func1 = Max(col(col_name[3])); /// select max(salary) from clerk group by country, gender;

    auto group_by_expr0 = col(col_name[2]);
    auto group_by_expr10 = col(col_name[2]);
    auto group_by_expr11 = col(col_name[1]);

    /// Prepare some data for max function test
    std::vector<ColumnsWithTypeAndName> expect_cols{
        {toNullableVec<Int32>("max(age)", ColumnWithNullableInt32{36, 32, 30, {}})},
        {toNullableVec<Float64>("max(salary)", ColumnWithNullableFloat64{2000.7, 1300.2, 1000.1, 0.3, -300.8, {}})}};
    std::vector<MockAstVec> group_by_exprs{{group_by_expr0}, {group_by_expr10, group_by_expr11}};
    std::vector<MockColumnNameVec> projections{{"max(age)"}, {"max(salary)"}};
    std::vector<MockAstVec> agg_funcs{{agg_func0}, {agg_func1}};
    size_t test_num = expect_cols.size();

    /// Start to test max function
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(std::make_pair(db_name, table_name), agg_funcs[i], group_by_exprs[i], projections[i]);
        executeAndAssertColumnsEqual(request, expect_cols[i]);
    }

    /// Min function tests

    agg_func0 = Min(col(col_name[0])); /// select min(age) from clerk group by country;
    agg_func1 = Min(col(col_name[3])); /// select min(salary) from clerk group by country, gender;

    expect_cols = {
        {toNullableVec<Int32>("min(age)", ColumnWithNullableInt32{30, 25, 22, {}})},
        {toNullableVec<Float64>("min(salary)", ColumnWithNullableFloat64{1300.2, 1000.1, 900.5, -200.4, -999.6, {}})}};
    projections = {{"min(age)"}, {"min(salary)"}};
    agg_funcs = {{agg_func0}, {agg_func1}};
    test_num = expect_cols.size();

    /// Start to test min function
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(std::make_pair(db_name, table_name), agg_funcs[i], group_by_exprs[i], projections[i]);
        executeAndAssertColumnsEqual(request, expect_cols[i]);
    }
}
CATCH

TEST_F(ExecutorAggTestRunner, AggregationCount)
try
{
    /// Prepare some data
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func0 = Count(col(col_name[0])); /// select count(age) from clerk group by country;
    auto agg_func1 = Count(col(col_name[1])); /// select count(gender) from clerk group by country, gender;
    auto agg_func2 = Count(lit(Field(static_cast<UInt64>(1)))); /// select count(1) from clerk;
    auto agg_func3 = Count(lit(Field())); /// select count(NULL) from clerk;
    auto agg_func4 = Count(lit(Field(static_cast<UInt64>(1)))); /// select count(1) from clerk group by country;
    auto agg_func5 = Count(lit(Field())); /// select count(NULL) from clerk group by country;
    auto agg_func6 = Count(col(col_name[4])); /// select count(pr) from clerk group by country;
    std::vector<MockAstVec> agg_funcs = {{agg_func0}, {agg_func1}, {agg_func2}, {agg_func3}, {agg_func4}, {agg_func5}, {agg_func6}};

    auto group_by_expr0 = col(col_name[2]);
    auto group_by_expr10 = col(col_name[2]);
    auto group_by_expr11 = col(col_name[1]);
    auto group_by_expr4 = col(col_name[2]);
    auto group_by_expr5 = col(col_name[2]);
    auto group_by_expr6 = col(col_name[2]);

    std::vector<ColumnsWithTypeAndName> expect_cols{
        {toVec<UInt64>("count(age)", ColumnWithUInt64{3, 3, 1, 0})},
        {toVec<UInt64>("count(gender)", ColumnWithUInt64{2, 2, 2, 1, 1, 1})},
        {toVec<UInt64>("count(1)", ColumnWithUInt64{9})},
        {toVec<UInt64>("count(NULL)", ColumnWithUInt64{0})},
        {toVec<UInt64>("count(1)", ColumnWithUInt64{4, 3, 1, 1})},
        {toVec<UInt64>("count(NULL)", ColumnWithUInt64{0, 0, 0, 0})},
        {toVec<UInt64>("count(pr)", ColumnWithUInt64{4, 3, 1, 1})}};
    std::vector<MockAstVec> group_by_exprs{{group_by_expr0}, {group_by_expr10, group_by_expr11}, {}, {}, {group_by_expr4}, {group_by_expr5}, {group_by_expr6}};
    std::vector<MockColumnNameVec> projections{{"count(age)"}, {"count(gender)"}, {"count(1)"}, {"count(NULL)"}, {"count(1)"}, {"count(NULL)"}, {"count(pr)"}};
    size_t test_num = expect_cols.size();

    /// Start to test
    for (size_t i = 0; i < test_num; ++i)
    {
        request = buildDAGRequest(std::make_pair(db_name, table_name), {agg_funcs[i]}, group_by_exprs[i], projections[i]);
        executeAndAssertColumnsEqual(request, expect_cols[i]);
    }
}
CATCH

TEST_F(ExecutorAggTestRunner, AggregationCountGroupByFastPathMultiKeys)
try
{
    /// Prepare some data
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func = Count(lit(Field(static_cast<UInt64>(1)))); /// select count(1) from `test_table_not_null` group by ``;
    std::string agg_func_res_name = "count(1)";

    auto group_by_expr_c1_i64 = col("c1_i64");
    auto group_by_expr_c2_f64 = col("c2_f64");
    auto group_by_expr_c3_str = col("c3_str");
    auto group_by_expr_c4_str = col("c4_str");
    auto group_by_expr_c5_date_time = col("c5_date_time");

    std::vector<MockAstVec> group_by_exprs{
        {group_by_expr_c3_str, group_by_expr_c2_f64, group_by_expr_c1_i64},
        {group_by_expr_c1_i64, group_by_expr_c2_f64},
        {group_by_expr_c1_i64, group_by_expr_c3_str},
        {group_by_expr_c3_str, group_by_expr_c2_f64},
        {group_by_expr_c3_str, group_by_expr_c4_str},
        {group_by_expr_c1_i64},
        {group_by_expr_c3_str},
        {group_by_expr_c3_str, group_by_expr_c5_date_time},
    };

    std::vector<ColumnsWithTypeAndName> expect_cols{
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
        {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 2})},
    };

    std::vector<MockColumnNameVec> projections{
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
        {agg_func_res_name},
    };
    size_t test_num = expect_cols.size();

    ASSERT_EQ(test_num, projections.size());
    ASSERT_EQ(test_num, group_by_exprs.size());

    {
        context.setCollation(TiDB::ITiDBCollator::UTF8MB4_BIN);
        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair("test_db", "test_table_not_null"), {agg_func}, group_by_exprs[i], projections[i]);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }
    {
        context.setCollation(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair("test_db", "test_table_not_null"), {agg_func}, group_by_exprs[i], projections[i]);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }
    for (auto collation_id : {0, static_cast<int>(TiDB::ITiDBCollator::BINARY)})
    {
        // 0: no collation
        // binnary collation
        context.setCollation(collation_id);

        std::vector<MockAstVec> group_by_exprs{
            {group_by_expr_c1_i64, group_by_expr_c3_str},
            {group_by_expr_c3_str, group_by_expr_c2_f64},
            {group_by_expr_c3_str, group_by_expr_c4_str},
            {group_by_expr_c3_str},
        };
        std::vector<ColumnsWithTypeAndName> expect_cols{
            {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 1, 1})},
            {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 1, 1})},
            {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 1, 1})},
            {toVec<UInt64>(agg_func_res_name, ColumnWithUInt64{1, 1, 1})},
        };
        std::vector<MockColumnNameVec> projections{
            {agg_func_res_name},
            {agg_func_res_name},
            {agg_func_res_name},
            {agg_func_res_name},
        };
        size_t test_num = expect_cols.size();
        ASSERT_EQ(test_num, projections.size());
        ASSERT_EQ(test_num, group_by_exprs.size());
        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair("test_db", "test_table_not_null"), {agg_func}, group_by_exprs[i], projections[i]);
            executeAndAssertColumnsEqual(request, expect_cols[i]);
        }
    }
}
CATCH

TEST_F(ExecutorAggTestRunner, AggNull)
try
{
    auto request = context
                       .scan("aggnull_test", "t1")
                       .aggregation({Max(col("s1"))}, {})
                       .build(context);
    executeAndAssertColumnsEqual(request, {{toNullableVec<String>({"banana"})}});

    request = context
                  .scan("aggnull_test", "t1")
                  .aggregation({}, {col("s1")})
                  .build(context);
    executeAndAssertColumnsEqual(request, {{toNullableVec<String>("s1", {{}, "banana"})}});
}
CATCH

TEST_F(ExecutorAggTestRunner, RepeatedAggregateFunction)
try
{
    std::vector<ASTPtr> functions = {Max(col("s1")), Min(col("s1")), Sum(col("s2"))};
    ColumnsWithTypeAndName functions_result = {toNullableVec<Int64>({3}), toNullableVec<Int64>({1}), toVec<UInt64>({6})};
    auto test_single_function = [&](size_t index) {
        auto request = context
                           .scan("test_db", "test_table")
                           .aggregation({functions[index]}, {})
                           .build(context);
        executeAndAssertColumnsEqual(request, {functions_result[index]});
    };
    for (size_t i = 0; i < functions.size(); ++i)
        test_single_function(i);

    std::vector<ASTPtr> funcs;
    ColumnsWithTypeAndName results;
    for (size_t i = 0; i < functions.size(); ++i)
    {
        funcs.push_back(functions[i]);
        results.push_back(functions_result[i]);
        for (size_t j = 0; j < functions.size(); ++j)
        {
            funcs.push_back(functions[j]);
            results.push_back(functions_result[j]);
            for (size_t k = 0; k < functions.size(); ++k)
            {
                funcs.push_back(functions[k]);
                results.push_back(functions_result[k]);

                auto request = context
                                   .scan("test_db", "test_table")
                                   .aggregation(funcs, {})
                                   .build(context);
                executeAndAssertColumnsEqual(request, results);

                funcs.pop_back();
                results.pop_back();
            }
            funcs.pop_back();
            results.pop_back();
        }
        funcs.pop_back();
        results.pop_back();
    }
}
CATCH

// TODO support more type of min, max, count.
//      support more aggregation functions: sum, forst_row, group_concat

} // namespace tests
} // namespace DB
