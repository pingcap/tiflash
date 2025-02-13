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

#include <Core/BlockUtils.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
namespace FailPoints
{
extern const char force_agg_on_partial_block[];
extern const char force_agg_prefetch[];
extern const char force_agg_two_level_hash_table_before_merge[];
} // namespace FailPoints
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

class AggExecutorTestRunner : public ExecutorTest
{
public:
    ~AggExecutorTestRunner() override = default;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        /// Create table for tests of group by
        context.addMockTable(
            /* name= */ {db_name, table_types},
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
        context.addMockTable(
            /* name= */ {db_name, table_name},
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

        context.addMockTable(
            {"aggnull_test", "t1"},
            {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
            {toNullableVec<String>("s1", {"banana", {}, "banana"}),
             toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "test_table"},
            {{"s1", TiDB::TP::TypeLongLong}, {"s2", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("s1", {1, 2, 3}), toVec<Int64>("s2", {1, 2, 3})});

        context.addMockTable(
            {"test_db", "test_table_not_null"},
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

        /// agg table with 200 rows
        {
            // with 15 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = i % 15;
                value[i] = {fmt::format("val_{}", i)};
            }
            context.addMockTable(
                {"test_db", "big_table_1"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }
        {
            // with 200 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = i;
                value[i] = {fmt::format("val_{}", i)};
            }
            context.addMockTable(
                {"test_db", "big_table_2"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }
        {
            // with 1 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(200);
            std::vector<std::optional<String>> value(200);
            for (size_t i = 0; i < 200; ++i)
            {
                key[i] = 0;
                value[i] = {fmt::format("val_{}", i)};
            }
            context.addMockTable(
                {"test_db", "big_table_3"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }
        {
            // with 1024 types of key.
            std::vector<std::optional<TypeTraits<int>::FieldType>> key(1024);
            std::vector<std::optional<String>> value(1024);
            for (size_t i = 0; i < 1024; ++i)
            {
                key[i] = i;
                value[i] = {fmt::format("val_{}", i)};
            }
            context.addMockTable(
                {"test_db", "big_table_4"},
                {{"key", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeString}},
                {toNullableVec<Int32>("key", key), toNullableVec<String>("value", value)});
        }
    }

    std::shared_ptr<tipb::DAGRequest> buildDAGRequest(
        std::pair<String, String> src,
        MockAstVec agg_funcs,
        MockAstVec group_by_exprs,
        MockColumnNameVec proj)
    {
        /// We can filter the group by column with project operator.
        /// project is applied to get partial aggregation output, so that we can remove redundant outputs and compare results with less handwriting codes.
        return context.scan(src.first, src.second).aggregation(agg_funcs, group_by_exprs).project(proj).build(context);
    }

    const String db_name{"test_db"};

    /// Prepare some data and names for tests of group by
    const String table_types{"types"};
    const std::vector<String> types_col_name{
        "id",
        "decimal_",
        "tinyint_",
        "smallint_",
        "int_",
        "bigint_",
        "float_",
        "double_",
        "date_",
        "datetime_",
        "string_"};
    ColumnWithNullableInt32 col_id{1, 2, 3, 4, 5, 6, 7, 8, 9};
    ColumnWithNullableDecimal
        col_decimal{DT(55, 1), {}, DT(-24, 1), DT(40, 1), DT(-40, 1), DT(40, 1), {}, DT(55, 1), DT(0, 1)};
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

#define WRAP_FOR_AGG_FAILPOINTS_START                                                  \
    std::vector<bool> enables{true, false};                                            \
    for (auto enable : enables)                                                        \
    {                                                                                  \
        if (enable)                                                                    \
        {                                                                              \
            FailPointHelper::enableFailPoint(FailPoints::force_agg_on_partial_block);  \
            FailPointHelper::enableFailPoint(FailPoints::force_agg_prefetch);          \
        }                                                                              \
        else                                                                           \
        {                                                                              \
            FailPointHelper::disableFailPoint(FailPoints::force_agg_on_partial_block); \
            FailPointHelper::disableFailPoint(FailPoints::force_agg_prefetch);         \
        }

#define WRAP_FOR_AGG_FAILPOINTS_END }

/// Guarantee the correctness of group by
TEST_F(AggExecutorTestRunner, GroupBy)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    std::vector<MockAstVec> group_by_exprs;
    std::vector<MockColumnNameVec> projections;
    std::vector<ColumnsWithTypeAndName> expect_cols;
    size_t test_num;

    {
        /// group by single column
        group_by_exprs
            = {{col(types_col_name[2])},
               {col(types_col_name[3])},
               {col(types_col_name[4])},
               {col(types_col_name[5])},
               {col(types_col_name[6])},
               {col(types_col_name[7])},
               {col(types_col_name[8])},
               {col(types_col_name[9])},
               {col(types_col_name[10])}};
        projections
            = {{types_col_name[2]},
               {types_col_name[3]},
               {types_col_name[4]},
               {types_col_name[5]},
               {types_col_name[6]},
               {types_col_name[7]},
               {types_col_name[8]},
               {types_col_name[9]},
               {types_col_name[10]}};
        expect_cols
            = {{toNullableVec<Int8>(
                   types_col_name[2],
                   ColumnWithNullableInt8{
                       -1,
                       2,
                       {},
                       0,
                       1,
                       3,
                       -2})}, /// select tinyint_ from test_db.types group by tinyint_;
               {toNullableVec<Int16>(
                   types_col_name[3],
                   ColumnWithNullableInt16{
                       -1,
                       2,
                       -2,
                       {},
                       0,
                       4,
                       3})}, /// select smallint_ from test_db.types group by smallint_;
               {toNullableVec<Int32>(
                   types_col_name[4],
                   ColumnWithNullableInt32{-1, {}, 4, 0, 123})}, /// select int_ from test_db.types group by int_;
               {toNullableVec<Int64>(
                   types_col_name[5],
                   ColumnWithNullableInt64{2, -1, 0, 123, {}})}, /// select bigint_ from test_db.types group by bigint_;
               {toNullableVec<Float32>(
                   types_col_name[6],
                   ColumnWithNullableFloat32{
                       0,
                       4,
                       3.3,
                       {},
                       5.6,
                       -0.1})}, /// select float_ from test_db.types group by float_;
               {toNullableVec<Float64>(
                   types_col_name[7],
                   ColumnWithNullableFloat64{
                       0,
                       {},
                       -1.2,
                       1.1,
                       1.2,
                       0.1})}, /// select double_ from test_db.types group by double_;
               {toNullableVec<MyDate>(
                   types_col_name[8],
                   ColumnWithNullableMyDate{
                       {},
                       0,
                       300000,
                       1000000,
                       2000000})}, /// select date_ from test_db.types group by date_;
               {toNullableVec<MyDateTime>(
                   types_col_name[9],
                   ColumnWithNullableMyDateTime{
                       {},
                       0,
                       1000000,
                       2000000,
                       3000000})}, /// select datetime_ from test_db.types group by datetime_;
               {toNullableVec<String>(
                   types_col_name[10],
                   ColumnWithNullableString{
                       {},
                       "pingcap",
                       "PingCAP",
                       "PINGCAP",
                       "Shanghai"})}}; /// select string_ from test_db.types group by string_;
        test_num = expect_cols.size();
        ASSERT_EQ(group_by_exprs.size(), test_num);
        ASSERT_EQ(projections.size(), test_num);

        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair(db_name, table_types), {}, group_by_exprs[i], projections[i]);
            for (auto force_two_level : {false, true})
            {
                if (force_two_level)
                    FailPointHelper::enableFailPoint(FailPoints::force_agg_two_level_hash_table_before_merge);
                else
                    FailPointHelper::disableFailPoint(FailPoints::force_agg_two_level_hash_table_before_merge);
                WRAP_FOR_AGG_FAILPOINTS_START
                executeAndAssertColumnsEqual(request, expect_cols[i]);
                WRAP_FOR_AGG_FAILPOINTS_END
            }
        }
    }

    {
        /// group by two columns
        group_by_exprs
            = {COL_GROUP2(2, 6),
               COL_GROUP2(3, 9),
               COL_GROUP2(4, 7),
               COL_GROUP2(5, 10),
               COL_GROUP2(8, 9),
               COL_GROUP2(9, 10)};
        projections
            = {COL_PROJ2(2, 6), COL_PROJ2(3, 9), COL_PROJ2(4, 7), COL_PROJ2(5, 10), COL_PROJ2(8, 9), COL_PROJ2(9, 10)};
        expect_cols
            = {/// select tinyint_, float_ from test_db.types group by tinyint_, float_;
               {toNullableVec<Int8>(types_col_name[2], ColumnWithNullableInt8{1, 2, {}, 3, 0, 0, -1, {}, -2}),
                toNullableVec<Float32>(
                    types_col_name[6],
                    ColumnWithNullableFloat32{3.3, {}, 4, 0, -0.1, 5.6, -0.1, 3.3, {}})},
               /// select smallint_, datetime_ from test_db.types group by smallint_, datetime_;
               {toNullableVec<Int16>(types_col_name[3], ColumnWithNullableInt16{2, 3, {}, {}, 0, -1, -2, 4}),
                toNullableVec<MyDateTime>(
                    types_col_name[9],
                    ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, {}, 0, 2000000})},
               /// select int_, double_ from test_db.types group by int_, double_;
               {toNullableVec<Int32>(types_col_name[4], ColumnWithNullableInt32{{}, 123, -1, 0, {}, 4, 4, 123}),
                toNullableVec<Float64>(
                    types_col_name[7],
                    ColumnWithNullableFloat64{0, -1.2, {}, 1.1, 1.1, -1.2, 0.1, 1.2})},
               /// select bigint_, string_ from test_db.types group by bigint_, string_;
               {toNullableVec<Int64>(types_col_name[5], ColumnWithNullableInt64{-1, 0, 0, 123, 2, {}, -1, 2}),
                toNullableVec<String>(
                    types_col_name[10],
                    ColumnWithNullableString{{}, {}, "Shanghai", "Shanghai", {}, "PingCAP", "PINGCAP", "pingcap"})},
               /// select date_, datetime_ from test_db.types group by date_, datetime_;
               {toNullableVec<MyDate>(
                    types_col_name[8],
                    ColumnWithNullableMyDate{1000000, 2000000, {}, 300000, 1000000, 0, 2000000, {}}),
                toNullableVec<MyDateTime>(
                    types_col_name[9],
                    ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, 0, 2000000, 1000000})},
               /// select datetime_, string_ from test_db.types group by datetime_, string_;
               {toNullableVec<MyDateTime>(
                    types_col_name[9],
                    ColumnWithNullableMyDateTime{2000000, 0, {}, 3000000, 1000000, 0, 2000000, 1000000}),
                toNullableVec<String>(
                    types_col_name[10],
                    ColumnWithNullableString{{}, "pingcap", "PingCAP", {}, "PINGCAP", {}, "Shanghai", "Shanghai"})}};
        test_num = expect_cols.size();
        ASSERT_EQ(group_by_exprs.size(), test_num);
        ASSERT_EQ(projections.size(), test_num);

        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(std::make_pair(db_name, table_types), {}, group_by_exprs[i], projections[i]);
            for (auto force_two_level : {false, true})
            {
                if (force_two_level)
                    FailPointHelper::enableFailPoint(FailPoints::force_agg_two_level_hash_table_before_merge);
                else
                    FailPointHelper::disableFailPoint(FailPoints::force_agg_two_level_hash_table_before_merge);
                WRAP_FOR_AGG_FAILPOINTS_START
                executeAndAssertColumnsEqual(request, expect_cols[i]);
                WRAP_FOR_AGG_FAILPOINTS_END
            }
        }
    }

    /// TODO type: decimal, enum and unsigned numbers
}
CATCH

TEST_F(AggExecutorTestRunner, AggregationMaxAndMin)
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
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, expect_cols[i]);
        WRAP_FOR_AGG_FAILPOINTS_END
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
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, expect_cols[i]);
        WRAP_FOR_AGG_FAILPOINTS_END
    }
}
CATCH

TEST_F(AggExecutorTestRunner, AggregationCount)
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
    std::vector<MockAstVec> agg_funcs
        = {{agg_func0}, {agg_func1}, {agg_func2}, {agg_func3}, {agg_func4}, {agg_func5}, {agg_func6}};

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
    std::vector<MockAstVec> group_by_exprs{
        {group_by_expr0},
        {group_by_expr10, group_by_expr11},
        {},
        {},
        {group_by_expr4},
        {group_by_expr5},
        {group_by_expr6}};
    std::vector<MockColumnNameVec> projections{
        {"count(age)"},
        {"count(gender)"},
        {"count(1)"},
        {"count(NULL)"},
        {"count(1)"},
        {"count(NULL)"},
        {"count(pr)"}};
    size_t test_num = expect_cols.size();

    /// Start to test
    for (size_t i = 0; i < test_num; ++i)
    {
        request
            = buildDAGRequest(std::make_pair(db_name, table_name), {agg_funcs[i]}, group_by_exprs[i], projections[i]);
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, expect_cols[i]);
        WRAP_FOR_AGG_FAILPOINTS_END
    }
}
CATCH

// TODO support more type of min, max, count.
//      support more aggregation functions: sum, forst_row, group_concat
TEST_F(AggExecutorTestRunner, AggregationCountGroupByFastPathMultiKeys)
try
{
    /// Prepare some data
    std::shared_ptr<tipb::DAGRequest> request;
    auto agg_func
        = Count(lit(Field(static_cast<UInt64>(1)))); /// select count(1) from `test_table_not_null` group by ``;
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
            request = buildDAGRequest(
                std::make_pair("test_db", "test_table_not_null"),
                {agg_func},
                group_by_exprs[i],
                projections[i]);
            WRAP_FOR_AGG_FAILPOINTS_START
            executeAndAssertColumnsEqual(request, expect_cols[i]);
            WRAP_FOR_AGG_FAILPOINTS_END
        }
    }
    {
        context.setCollation(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
        for (size_t i = 0; i < test_num; ++i)
        {
            request = buildDAGRequest(
                std::make_pair("test_db", "test_table_not_null"),
                {agg_func},
                group_by_exprs[i],
                projections[i]);
            WRAP_FOR_AGG_FAILPOINTS_START
            executeAndAssertColumnsEqual(request, expect_cols[i]);
            WRAP_FOR_AGG_FAILPOINTS_END
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
            request = buildDAGRequest(
                std::make_pair("test_db", "test_table_not_null"),
                {agg_func},
                group_by_exprs[i],
                projections[i]);
            WRAP_FOR_AGG_FAILPOINTS_START
            executeAndAssertColumnsEqual(request, expect_cols[i]);
            WRAP_FOR_AGG_FAILPOINTS_END
        }
    }
}
CATCH

TEST_F(AggExecutorTestRunner, AggNull)
try
{
    auto request = context.scan("aggnull_test", "t1").aggregation({Max(col("s1"))}, {}).build(context);
    executeAndAssertColumnsEqual(request, {{toNullableVec<String>({"banana"})}});

    request = context.scan("aggnull_test", "t1").aggregation({}, {col("s1")}).build(context);
    WRAP_FOR_AGG_FAILPOINTS_START
    executeAndAssertColumnsEqual(request, {{toNullableVec<String>("s1", {{}, "banana"})}});
    WRAP_FOR_AGG_FAILPOINTS_END
}
CATCH

TEST_F(AggExecutorTestRunner, RepeatedAggregateFunction)
try
{
    std::vector<ASTPtr> functions = {Max(col("s1")), Min(col("s1")), Sum(col("s2"))};
    ColumnsWithTypeAndName functions_result
        = {toNullableVec<Int64>({3}), toNullableVec<Int64>({1}), toVec<UInt64>({6})};
    auto test_single_function = [&](size_t index) {
        auto request = context.scan("test_db", "test_table").aggregation({functions[index]}, {}).build(context);
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, {functions_result[index]});
        WRAP_FOR_AGG_FAILPOINTS_END
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

                auto request = context.scan("test_db", "test_table").aggregation(funcs, {}).build(context);
                WRAP_FOR_AGG_FAILPOINTS_START
                executeAndAssertColumnsEqual(request, results);
                WRAP_FOR_AGG_FAILPOINTS_END

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

TEST_F(AggExecutorTestRunner, AggMerge)
try
{
    std::vector<String> tables{"big_table_1", "big_table_2", "big_table_3", "big_table_4"};
    for (const auto & table : tables)
    {
        std::vector<std::shared_ptr<tipb::DAGRequest>> requests{
            context.scan("test_db", table).aggregation({Max(col("value"))}, {col("key")}).build(context),
            context.scan("test_db", table).aggregation({Max(col("value"))}, {}).build(context),
        };
        for (const auto & request : requests)
        {
            auto expect = executeStreams(request, 1);
            context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
            // 0: use one level merge
            // 1: use two level merge
            std::vector<UInt64> two_level_thresholds{0, 1};
            for (auto two_level_threshold : two_level_thresholds)
            {
                context.context->setSetting(
                    "group_by_two_level_threshold",
                    Field(static_cast<UInt64>(two_level_threshold)));
                WRAP_FOR_AGG_FAILPOINTS_START
                executeAndAssertColumnsEqual(request, expect);
                WRAP_FOR_AGG_FAILPOINTS_END
            }
        }
    }
}
CATCH

TEST_F(AggExecutorTestRunner, SplitAggOutput)
try
{
    std::vector<String> tables{"big_table_1", "big_table_2", "big_table_3", "big_table_4"};
    std::vector<size_t> max_block_sizes{1, 2, 9, 19, 40, DEFAULT_BLOCK_SIZE};
    std::vector<size_t> concurrences{1, 2, 10};
    std::vector<size_t> expect_rows{15, 200, 1, 1024};
    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto request = context.scan("test_db", tables[i]).aggregation({Max(col("value"))}, {col("key")}).build(context);
        context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
        // 0: use one level
        // 1: use two level
        std::vector<UInt64> two_level_thresholds{0, 1};
        for (auto two_level_threshold : two_level_thresholds)
        {
            for (auto block_size : max_block_sizes)
            {
                for (auto concurrency : concurrences)
                {
                    context.context->setSetting(
                        "group_by_two_level_threshold",
                        Field(static_cast<UInt64>(two_level_threshold)));
                    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
                    WRAP_FOR_AGG_FAILPOINTS_START
                    auto blocks = getExecuteStreamsReturnBlocks(request, concurrency);
                    size_t actual_row = 0;
                    for (auto & block : blocks)
                    {
                        ASSERT(block.rows() <= block_size);
                        actual_row += block.rows();
                    }
                    ASSERT_EQ(actual_row, expect_rows[i]);
                    WRAP_FOR_AGG_FAILPOINTS_END
                }
            }
        }
    }
}
CATCH

TEST_F(AggExecutorTestRunner, SplitAggOutputWithSpecialGroupKey)
try
{
    /// prepare data
    size_t unique_rows = 3000;
    DB::MockColumnInfoVec table_column_infos{
        {"key_8", TiDB::TP::TypeTiny, false},
        {"key_16", TiDB::TP::TypeShort, false},
        {"key_32", TiDB::TP::TypeLong, false},
        {"key_64", TiDB::TP::TypeLongLong, false},
        {"key_string_1", TiDB::TP::TypeString, false},
        {"key_string_2", TiDB::TP::TypeString, false},
        {"value", TiDB::TP::TypeLong, false}};
    ColumnsWithTypeAndName table_column_data;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(table_column_infos))
    {
        ColumnGeneratorOpts opts{
            unique_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        table_column_data.push_back(ColumnGenerator::instance().generate(opts));
    }
    for (auto & table_column : table_column_data)
    {
        table_column.column->assumeMutable()->insertRangeFrom(*table_column.column, 0, unique_rows / 2);
    }
    ColumnWithTypeAndName shuffle_column
        = ColumnGenerator::instance().generate({unique_rows + unique_rows / 2, "UInt64", RANDOM});
    IColumn::Permutation perm;
    shuffle_column.column->getPermutation(false, 0, -1, perm);
    for (auto & column : table_column_data)
    {
        column.column = column.column->permute(perm, 0);
    }

    context.addMockTable("test_db", "agg_table_with_special_key", table_column_infos, table_column_data);

    std::vector<size_t> max_block_sizes{1, 8, DEFAULT_BLOCK_SIZE};
    std::vector<size_t> concurrences{1, 8};
    // 0: use one level
    // 1: use two level
    std::vector<UInt64> two_level_thresholds{0, 1};
    std::vector<Int64> collators{TiDB::ITiDBCollator::UTF8MB4_BIN, TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI};
    std::vector<std::vector<String>> group_by_keys{
        /// fast path with one int and one string
        {"key_64", "key_string_1"},
        /// fast path with two string
        {"key_string_1", "key_string_2"},
        /// fast path with one string
        {"key_string_1"},
        /// keys need to be shuffled
        {"key_8", "key_16", "key_32", "key_64"},
    };
    for (auto collator_id : collators)
    {
        for (const auto & keys : group_by_keys)
        {
            context.setCollation(collator_id);
            const auto * current_collator = TiDB::ITiDBCollator::getCollator(collator_id);
            ASSERT_TRUE(current_collator != nullptr);
            SortDescription sd;
            bool has_string_key = false;
            MockAstVec key_vec;
            for (const auto & key : keys)
                key_vec.push_back(col(key));
            auto request = context.scan("test_db", "agg_table_with_special_key")
                               .aggregation({Max(col("value"))}, key_vec)
                               .build(context);
            /// use one level, no block split, no spill as the reference
            context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
            context.context->setSetting("max_bytes_before_external_group_by", Field(static_cast<UInt64>(0)));
            context.context->setSetting("max_block_size", Field(static_cast<UInt64>(unique_rows * 2)));
            auto reference = executeStreams(request);
            if (current_collator->isCI())
            {
                /// for ci collation, need to sort and compare the result manually
                for (const auto & result_col : reference)
                {
                    if (!removeNullable(result_col.type)->isString())
                    {
                        sd.push_back(SortColumnDescription(result_col.name, 1, 1, nullptr));
                    }
                    else
                    {
                        sd.push_back(SortColumnDescription(result_col.name, 1, 1, current_collator));
                        has_string_key = true;
                    }
                }
                /// don't run ci test if there is no string key
                if (!has_string_key)
                    continue;
                Block tmp_block(reference);
                sortBlock(tmp_block, sd);
                reference = tmp_block.getColumnsWithTypeAndName();
            }
            for (auto two_level_threshold : two_level_thresholds)
            {
                for (auto block_size : max_block_sizes)
                {
                    for (auto concurrency : concurrences)
                    {
                        context.context->setSetting(
                            "group_by_two_level_threshold",
                            Field(static_cast<UInt64>(two_level_threshold)));
                        context.context->setSetting("max_block_size", Field(static_cast<UInt64>(block_size)));
                        WRAP_FOR_AGG_FAILPOINTS_START
                        auto blocks = getExecuteStreamsReturnBlocks(request, concurrency);
                        for (auto & block : blocks)
                        {
                            block.checkNumberOfRows();
                            ASSERT(block.rows() <= block_size);
                        }
                        if (current_collator->isCI())
                        {
                            auto merged_block = vstackBlocks(std::move(blocks));
                            sortBlock(merged_block, sd);
                            auto merged_columns = merged_block.getColumnsWithTypeAndName();
                            for (size_t col_index = 0; col_index < reference.size(); col_index++)
                                ASSERT_TRUE(columnEqual(
                                    reference[col_index].column,
                                    merged_columns[col_index].column,
                                    sd[col_index].collator));
                        }
                        else
                        {
                            ASSERT_TRUE(columnsEqual(
                                reference,
                                vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName(),
                                false));
                        }
                        WRAP_FOR_AGG_FAILPOINTS_END
                    }
                }
            }
        }
    }
}
CATCH

TEST_F(AggExecutorTestRunner, Empty)
try
{
    context.addMockTable(
        {"test_db", "empty_table"},
        {{"s1", TiDB::TP::TypeLongLong}, {"s2", TiDB::TP::TypeLongLong}},
        {toVec<Int64>("s1", {}), toVec<Int64>("s2", {})});
    context.addExchangeReceiver(
        "empty_recv",
        {{"s1", TiDB::TP::TypeLongLong}, {"s2", TiDB::TP::TypeLongLong}},
        {toVec<Int64>("s1", {}), toVec<Int64>("s2", {})},
        5,
        {{"s2", TiDB::TP::TypeLongLong}});

    auto request = context.scan("test_db", "empty_table").aggregation({Max(col("s1"))}, {col("s2")}).build(context);
    executeAndAssertColumnsEqual(request, {});

    request = context.receive("empty_recv", 5).aggregation({Max(col("s1"))}, {col("s2")}, 5).build(context);
    {
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, {});
        WRAP_FOR_AGG_FAILPOINTS_END
    }

    request = context.scan("test_db", "empty_table")
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
    {
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(request, {toVec<UInt64>({0})});
        WRAP_FOR_AGG_FAILPOINTS_END
    }
}
CATCH

TEST_F(AggExecutorTestRunner, AggKeyOptimization)
try
{
    const String db_name = "test_db";
    const String tbl_name = "agg_first_row_opt_tbl";
    const auto rows = 1024;
    const auto row_types = 4;
    const auto rows_per_type = rows / row_types;
    DB::MockColumnInfoVec table_column_infos{
        {"col_string_with_collator", TiDB::TP::TypeString, false, Poco::Dynamic::Var("utf8_general_ci")},
        {"col_string_no_collator", TiDB::TP::TypeString, false},
        {"col_int", TiDB::TP::TypeLong, false},
        {"col_tinyint", TiDB::TP::TypeTiny, false}};

    std::vector<String> col_data_string_with_collator(rows);
    std::vector<String> col_data_string_no_collator(rows);
    std::vector<TypeTraits<Int32>::FieldType> col_data_int(rows);
    std::vector<TypeTraits<Int8>::FieldType> col_data_tinyint(rows);
    // rows_per_type "a" 0
    //               "a" 0
    //               ...
    // rows_per_type "b" 1
    //               "b" 1
    //               ...
    // rows_per_type "c" 2
    //               "c" 2
    //               ...
    // rows_per_type "d" 3
    //               "d" 3
    //               ...
    for (size_t i = 0; i < row_types; ++i)
    {
        for (size_t j = 0; j < rows_per_type; ++j)
        {
            char ch = 'a' + i;
            const auto idx = i * rows_per_type + j;
            col_data_string_with_collator[idx] = std::string{ch};
            col_data_string_no_collator[idx] = std::string{ch};
            col_data_int[idx] = i;
            col_data_tinyint[idx] = static_cast<Int64>(static_cast<unsigned char>(i));
        }
    }
    context.addMockTable(
        {db_name, tbl_name},
        table_column_infos,
        {
            toVec<String>("col_string_with_collator", col_data_string_with_collator),
            toVec<String>("col_string_no_collator", col_data_string_no_collator),
            toVec<Int32>("col_int", col_data_int),
            toVec<Int8>("col_tinyint", col_data_tinyint),
        });

    std::random_device rd;
    std::mt19937_64 gen(rd());

    std::vector<size_t> max_block_sizes{1, 2, DEFAULT_BLOCK_SIZE};
    std::vector<UInt64> two_level_thresholds{0, 1};

    std::uniform_int_distribution<size_t> dist(0, max_block_sizes.size());
    size_t random_block_size = max_block_sizes[dist(gen)];

    std::uniform_int_distribution<size_t> dist1(0, two_level_thresholds.size());
    size_t random_two_level_threshold = two_level_thresholds[dist1(gen)];
    LOG_DEBUG(
        Logger::get("AggExecutorTestRunner::AggKeyOptimization"),
        "max_block_size: {}, two_level_threshold: {}",
        random_block_size,
        random_two_level_threshold);

    context.context->setSetting("group_by_two_level_threshold_bytes", Field(static_cast<UInt64>(0)));
#define WRAP_FOR_AGG_CHANGE_SETTINGS                             \
    context.context->setSetting(                                 \
        "group_by_two_level_threshold",                          \
        Field(static_cast<UInt64>(random_two_level_threshold))); \
    context.context->setSetting("max_block_size", Field(static_cast<UInt64>(random_block_size)));

    FailPointHelper::enableFailPoint(FailPoints::force_agg_prefetch);
    {
        // case-1: select count(1), col_tinyint from t group by col_int, col_tinyint
        // agg method: keys64(AggregationMethodKeysFixed)
        // opt: agg_func_ref_key
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_tinyint"))};
        MockAstVec keys{col("col_int"), col("col_tinyint")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected
            = {toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
               toNullableVec<Int8>("first_row(col_tinyint)", ColumnWithNullableInt8{0, 1, 2, 3}),
               toVec<Int32>("col_int", ColumnWithInt32{0, 1, 2, 3}),
               toVec<Int8>("col_tinyint", ColumnWithInt8{0, 1, 2, 3})};
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }

    {
        // case-2: select count(1), col_int from t group by col_int
        // agg method: key32(AggregationMethodOneNumber)
        // opt: agg_func_ref_key
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_int"))};
        MockAstVec keys{col("col_int")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected
            = {toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
               toNullableVec<Int32>("first_row(col_int)", ColumnWithNullableInt32{0, 1, 2, 3}),
               toVec<Int32>("col_int", ColumnWithInt32{0, 1, 2, 3})};
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }

    {
        // case-3: select count(1), col_string_no_collator from t group by col_string_no_collator
        // agg method: key_string(AggregationMethodStringNoCache)
        // opt: agg_func_ref_key
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_string_no_collator"))};
        MockAstVec keys{col("col_string_no_collator")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected = {
            toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
            toNullableVec<String>("first_row(col_string_no_collator)", ColumnWithNullableString{"a", "b", "c", "d"}),
            toVec<String>("col_string_no_collator", ColumnWithString{"a", "b", "c", "d"}),
        };
        executeAndAssertColumnsEqual(request, expected);
    }

    {
        // case-4: select count(1), col_string_with_collator from t group by col_string_with_collator
        // agg method: key_strbin/key_strbinpadding(AggregationMethodOneKeyStringNoCache)
        // opt: key_ref_agg_func
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_string_with_collator"))};
        MockAstVec keys{col("col_string_with_collator")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected = {
            toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
            toNullableVec<String>("first_row(col_string_with_collator)", ColumnWithNullableString{"a", "b", "c", "d"}),
            toVec<String>("col_string_with_collator", ColumnWithString{"a", "b", "c", "d"}),
        };
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }

    {
        // case-4-1: select count(1) from t group by col_string_with_collator
        // Use 'any' agg func instead of first_row
        // agg method: key_strbin/key_strbinpadding(AggregationMethodOneKeyStringNoCache)
        // opt: key_ref_agg_func
        std::vector<ASTPtr> agg_funcs = {
            makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
        };
        MockAstVec keys{col("col_string_with_collator")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected = {
            toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
            toVec<String>("first_row(col_string_with_collator)", ColumnWithString{"a", "b", "c", "d"}),
        };
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }

    // case-5: none
    // agg method: key_fixed_string(AggregationMethodFixedStringNoCache)

    {
        // case-6: select count(1), col_string_with_collator from t group by col_string_with_collator, col_int, col_string_no_collator
        // agg method: serialized(AggregationMethodSerialized)
        // opt: key_ref_agg_func && agg_func_ref_key
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_string_with_collator"))};
        MockAstVec keys{col("col_string_with_collator"), col("col_int"), col("col_string_no_collator")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected = {
            toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
            toNullableVec<String>("first_row(col_string_with_collator)", ColumnWithNullableString{"a", "b", "c", "d"}),
            toVec<String>("col_string_with_collator", ColumnWithString{"a", "b", "c", "d"}),
            toVec<Int32>("col_int", ColumnWithInt32{0, 1, 2, 3}),
            toVec<String>("col_string_no_collator", ColumnWithString{"a", "b", "c", "d"}),
        };
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }

    {
        // case-7: select count(1), col_string_with_collator, col_int from t group by col_string_with_collator, col_int
        // agg method: two_keys_num64_strbin(AggregationMethodFastPathTwoKeyNoCache)
        // opt: key_ref_agg_func && agg_func_ref_key
        std::vector<ASTPtr> agg_funcs
            = {makeASTFunction("count", lit(Field(static_cast<UInt64>(1)))),
               makeASTFunction("first_row", col("col_string_with_collator"))};
        MockAstVec keys{col("col_string_with_collator"), col("col_int")};
        auto request = context.scan(db_name, tbl_name).aggregation(agg_funcs, keys).build(context);
        auto expected = {
            toVec<UInt64>("count(1)", ColumnWithUInt64{rows_per_type, rows_per_type, rows_per_type, rows_per_type}),
            toNullableVec<String>("first_row(col_string_with_collator)", ColumnWithNullableString{"a", "b", "c", "d"}),
            toVec<String>("col_string_with_collator", ColumnWithString{"a", "b", "c", "d"}),
            toVec<Int32>("col_int", ColumnWithInt32{0, 1, 2, 3})};
        WRAP_FOR_AGG_CHANGE_SETTINGS
        executeAndAssertColumnsEqual(request, expected);
    }
    FailPointHelper::disableFailPoint(FailPoints::force_agg_prefetch);
#undef WRAP_FOR_AGG_CHANGE_SETTINGS
}
CATCH

TEST_F(AggExecutorTestRunner, FineGrainedShuffle)
try
{
    DB::MockColumnInfoVec column_infos{{"partition", TiDB::TP::TypeLong}, {"value", TiDB::TP::TypeLong}};
    DB::MockColumnInfoVec partition_column_infos{{"partition", TiDB::TP::TypeLong}};
    ColumnsWithTypeAndName column_data;
    ColumnsWithTypeAndName common_column_data;
    size_t table_rows = 1024;
    for (const auto & column_info : mockColumnInfosToTiDBColumnInfos(column_infos))
    {
        ColumnGeneratorOpts opts{
            table_rows,
            getDataTypeByColumnInfoForComputingLayer(column_info)->getName(),
            RANDOM,
            column_info.name};
        column_data.push_back(ColumnGenerator::instance().generate(opts));
    }
    ColumnWithTypeAndName shuffle_column = ColumnGenerator::instance().generate({table_rows, "UInt64", RANDOM});
    IColumn::Permutation perm;
    shuffle_column.column->getPermutation(false, 0, -1, perm);
    for (auto & column : column_data)
    {
        column.column = column.column->permute(perm, 0);
    }

    context
        .addExchangeReceiver("exchange_receiver_1_concurrency", column_infos, column_data, 1, partition_column_infos);
    context
        .addExchangeReceiver("exchange_receiver_10_concurrency", column_infos, column_data, 10, partition_column_infos);
    std::vector<size_t> exchange_receiver_concurrency = {1, 10};

    auto gen_request = [&](size_t exchange_concurrency) {
        return context
            .receive(fmt::format("exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency)
            .aggregation({Max(col("value"))}, {col("partition")}, exchange_concurrency)
            .build(context);
    };

    auto baseline = executeStreams(gen_request(1), 1);
    for (size_t exchange_concurrency : exchange_receiver_concurrency)
    {
        WRAP_FOR_AGG_FAILPOINTS_START
        executeAndAssertColumnsEqual(gen_request(exchange_concurrency), baseline);
        WRAP_FOR_AGG_FAILPOINTS_END
    }
}
CATCH

#undef WRAP_FOR_AGG_FAILPOINTS_START
#undef WRAP_FOR_AGG_FAILPOINTS_END

} // namespace tests
} // namespace DB
