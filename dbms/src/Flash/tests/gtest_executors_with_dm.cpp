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

#include <Debug/MockStorage.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class ExecutorsWithDMTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.mockStorage()->setUseDeltaMerge(true);
        context.context->getSettingsRef().dt_enable_read_thread = true;
        context.context->getSettingsRef().dt_segment_stable_pack_rows = 1;
        context.context->getSettingsRef().dt_segment_limit_rows = 1;
        context.context->getSettingsRef().dt_segment_delta_cache_limit_rows = 1;
        // note that
        // 1. the first column is pk.
        // 2. The decimal type is not supported.
        context.addMockDeltaMerge(
            {"test_db", "t0"},
            {{"col0", TiDB::TP::TypeLongLong}},
            {{toVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        context.addMockDeltaMerge(
            {"test_db", "t1"},
            {{"col0", TiDB::TP::TypeLongLong}, {"col1", TiDB::TP::TypeString}},
            {{toVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        context.addMockDeltaMerge(
            {"test_db", "t2"},
            {{"col0", TiDB::TP::TypeLongLong},
             {"col1", TiDB::TP::TypeTiny},
             {"col2", TiDB::TP::TypeShort},
             {"col3", TiDB::TP::TypeLong},
             {"col4", TiDB::TP::TypeLongLong},
             {"col5", TiDB::TP::TypeFloat},
             {"col6", TiDB::TP::TypeDouble},
             {"col7", TiDB::TP::TypeDate},
             {"col8", TiDB::TP::TypeDatetime},
             {"col9", TiDB::TP::TypeString}},
            {toVec<Int64>("col0", col_id),
             toNullableVec<Int8>("col1", col_tinyint),
             toNullableVec<Int16>("col2", col_smallint),
             toNullableVec<Int32>("col3", col_int),
             toNullableVec<Int64>("col4", col_bigint),
             toNullableVec<Float32>("col5", col_float),
             toNullableVec<Float64>("col6", col_double),
             toNullableVec<MyDate>("col7", col_mydate),
             toNullableVec<MyDateTime>("col8", col_mydatetime),
             toNullableVec<String>("col9", col_string)});

        // with 200 rows.
        std::vector<TypeTraits<Int64>::FieldType> key(200);
        std::vector<std::optional<String>> value(200);
        for (size_t i = 0; i < 200; ++i)
        {
            key[i] = i % 15;
            value[i] = {fmt::format("val_{}", i)};
        }
        context.addMockDeltaMerge(
            {"test_db", "big_table"},
            {{"key", TiDB::TP::TypeLongLong}, {"value", TiDB::TP::TypeString}},
            {toVec<Int64>("key", key), toNullableVec<String>("value", value)});

        context.addMockDeltaMerge(
            {"test_db", "empty_table"},
            {{"col0", TiDB::TP::TypeLongLong}},
            {toVec<Int32>("col0", {})});
    }

    ColumnWithInt64 col_id{1, 2, 3, 4, 5, 6, 7, 8, 9};
    ColumnWithNullableInt8 col_tinyint{1, 2, 3, {}, {}, 0, 0, -1, -2};
    ColumnWithNullableInt16 col_smallint{2, 3, {}, {}, 0, -1, -2, 4, 0};
    ColumnWithNullableInt32 col_int{4, {}, {}, 0, 123, -1, -1, 123, 4};
    ColumnWithNullableInt64 col_bigint{2, 2, {}, 0, -1, {}, -1, 0, 123};
    ColumnWithNullableFloat32 col_float{3.3, {}, 0, 4.0, 3.3, 5.6, -0.1, -0.1, {}};
    ColumnWithNullableFloat64 col_double{0.1, 0, 1.1, 1.1, 1.2, {}, {}, -1.2, -1.2};
    ColumnWithNullableMyDate col_mydate{1000000, 2000000, {}, 300000, 1000000, {}, 0, 2000000, {}};
    ColumnWithNullableMyDateTime col_mydatetime{2000000, 0, {}, 3000000, 1000000, {}, 0, 2000000, 1000000};
    ColumnWithNullableString col_string{{}, "pingcap", "PingCAP", {}, "PINGCAP", "PingCAP", {}, "Shanghai", "Shanghai"};
};

#define WRAP_FOR_DM_TEST_BEGIN                     \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_DM_TEST_END }

TEST_F(ExecutorsWithDMTestRunner, Basic)
try
{
    std::vector<bool> keep_order_opt{false, true};

    WRAP_FOR_DM_TEST_BEGIN
    for (auto keep_order : keep_order_opt)
    {
        auto request = context.scan("test_db", "t0", keep_order).build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        request = context.scan("test_db", "t1", keep_order).build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        request = context.scan("test_db", "t2", keep_order).build(context);

        executeAndAssertColumnsEqual(
            request,
            {toNullableVec<Int64>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
             toNullableVec<Int8>(col_tinyint),
             toNullableVec<Int16>(col_smallint),
             toNullableVec<Int32>(col_int),
             toNullableVec<Int64>(col_bigint),
             toNullableVec<Float32>(col_float),
             toNullableVec<Float64>(col_double),
             toNullableVec<MyDate>(col_mydate),
             toNullableVec<MyDateTime>(col_mydatetime),
             toNullableVec<String>(col_string)});

        request = context.scan("test_db", "big_table", keep_order).build(context);
        auto expect = executeStreams(request, 1);

        executeAndAssertColumnsEqual(request, expect);

        request = context.scan("test_db", "empty_table", keep_order).build(context);
        executeAndAssertColumnsEqual(request, {});

        // projection
        request = context.scan("test_db", "t1", keep_order).project({col("col0")}).build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3, 4, 5, 6, 7})}});

        request = context.scan("test_db", "t1", keep_order).project({col("col1")}).build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}, "col1-4", {}, "col1-6", "col1-7"})}});

        // filter
        request = context.scan("test_db", "t0", keep_order)
                      .filter(lt(col("col0"), lit(Field(static_cast<Int64>(4)))))
                      .build(context);
        executeAndAssertColumnsEqual(request, {{toNullableVec<Int64>("col0", {0, 1, 2, 3})}});

        request = context.scan("test_db", "t1", keep_order)
                      .filter(lt(col("col0"), lit(Field(static_cast<Int64>(4)))))
                      .build(context);
        executeAndAssertColumnsEqual(
            request,
            {{toNullableVec<Int64>("col0", {0, 1, 2, 3})},
             {toNullableVec<String>("col1", {"col1-0", "col1-1", "col1-2", {}})}});
    }
    WRAP_FOR_DM_TEST_END
}
CATCH

#undef WRAP_FOR_DM_TEST_BEGIN
#undef WRAP_FOR_DM_TEST_END

} // namespace tests
} // namespace DB
