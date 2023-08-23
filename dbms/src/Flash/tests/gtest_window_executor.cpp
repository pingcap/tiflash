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

#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>

namespace DB::tests
{
class WindowExecutorTestRunner : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"test_db", "test_table"},
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})});
        DB::MockColumnInfoVec partition_column_infos{{"partition", TiDB::TP::TypeLongLong}};
        context.addExchangeReceiver(
            "test_recv",
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
            1,
            partition_column_infos);

        context.addMockTable(
            {"test_db", "test_table_string"},
            {{"partition", TiDB::TP::TypeString}, {"order", TiDB::TP::TypeString}},
            {toVec<String>("partition", {"banana", "banana", "banana", "banana", "apple", "apple", "apple", "apple"}),
             toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"})});

        context.addMockTable(
            {"test_db", "test_table_more_cols"},
            {{"partition1", TiDB::TP::TypeLongLong},
             {"partition2", TiDB::TP::TypeLongLong},
             {"order1", TiDB::TP::TypeLongLong},
             {"order2", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
             toVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}),
             toVec<Int64>("order1", {2, 1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1}),
             toVec<Int64>("order2", {2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2, 1})});

        context.addMockTable(
            {"test_db", "test_table_float64"},
            {{"partition", TiDB::TP::TypeDouble}, {"order", TiDB::TP::TypeDouble}},
            {toVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
             toVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})});

        context.addMockTable(
            {"test_db", "test_table_datetime"},
            {{"partition", TiDB::TP::TypeDatetime}, {"order", TiDB::TP::TypeDatetime}});

        context.addMockTable(
            {"test_db", "test_table_for_rank"},
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})});

        context.addMockTable(
            {"test_db", "test_table_for_lead_lag"},
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}, {"value", TiDB::TP::TypeString}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 7, 8}),
             toVec<String>("value", {"a", "b", "c", "d", "e", "f", "g", "h"})});
    }

    void executeWithTableScanAndConcurrency(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const String & db,
        const String & table_name,
        const ColumnsWithTypeAndName & source_columns,
        const ColumnsWithTypeAndName & expect_columns)
    {
        context.updateMockTableColumnData(db, table_name, source_columns);
        executeAndAssertColumnsEqual(request, expect_columns);
    }
};

TEST_F(WindowExecutorTestRunner, testWindowFunctionByPartitionAndOrder)
try
{
    /***** row_number with different types of input *****/
    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    auto request = context.scan("test_db", "test_table")
                       .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                       .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                       .build(context);
    executeAndAssertColumnsEqual(
        request,
        createColumns(
            {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
             toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // null input
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table",
        {toNullableVec<Int64>("partition", {}), toNullableVec<Int64>("order", {})},
        createColumns({}));

    // nullable
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table",
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
         {toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})}},
        createColumns(
            {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}),
             toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    request = context.scan("test_db", "test_table_string")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(
        request,
        createColumns(
            {toNullableVec<String>(
                 "partition",
                 {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
             toNullableVec<String>(
                 "order",
                 {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
             toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_string",
        {toNullableVec<String>(
             "partition",
             {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}),
         toNullableVec<String>(
             "order",
             {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
        createColumns(
            {toNullableVec<String>(
                 "partition",
                 {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
             toNullableVec<String>(
                 "order",
                 {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
             toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // float64 - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_float order by order_float64)
    request = context.scan("test_db", "test_table_float64")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(
        request,
        createColumns(
            {toNullableVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
             toNullableVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
             toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_float64",
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
         toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        createColumns(
            {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
             toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
             toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    request = context.scan("test_db", "test_table_datetime")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_datetime",
        {toNullableDatetimeVec(
             "partition",
             {"20220101010102",
              "20220101010102",
              "20220101010102",
              "20220101010102",
              "20220101010101",
              "20220101010101",
              "20220101010101",
              "20220101010101"},
             0),
         toDatetimeVec(
             "order",
             {"20220101010101",
              "20220101010101",
              "20220101010102",
              "20220101010102",
              "20220101010101",
              "20220101010101",
              "20220101010102",
              "20220101010102"},
             0)},
        createColumns(
            {toNullableDatetimeVec(
                 "partition",
                 {"20220101010101",
                  "20220101010101",
                  "20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102",
                  "20220101010102",
                  "20220101010102"},
                 0),
             toNullableDatetimeVec(
                 "order",
                 {"20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102",
                  "20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102"},
                 0),
             toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_datetime",
        {toNullableDatetimeVec(
             "partition",
             {"20220101010102",
              {},
              "20220101010102",
              "20220101010102",
              "20220101010102",
              "20220101010101",
              "20220101010101",
              "20220101010101",
              "20220101010101"},
             0),
         toNullableDatetimeVec(
             "order",
             {"20220101010101",
              {},
              "20220101010101",
              "20220101010102",
              "20220101010102",
              "20220101010101",
              "20220101010101",
              "20220101010102",
              "20220101010102"},
             0)},
        createColumns(
            {toNullableDatetimeVec(
                 "partition",
                 {{},
                  "20220101010101",
                  "20220101010101",
                  "20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102",
                  "20220101010102",
                  "20220101010102"},
                 0),
             toNullableDatetimeVec(
                 "order",
                 {{},
                  "20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102",
                  "20220101010101",
                  "20220101010101",
                  "20220101010102",
                  "20220101010102"},
                 0),
             toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // 2 partiton key and 2 order key
    // sql : select *, row_number() over w1 from test6 window w1 as (partition by partition_int1, partition_int2 order by order_int1,order_int2)
    request = context.scan("test_db", "test_table_more_cols")
                  .sort({{"partition1", false}, {"partition2", false}, {"order1", false}, {"order2", false}}, true)
                  .window(
                      RowNumber(),
                      {{"order1", false}, {"order2", false}},
                      {{"partition1", false}, {"partition2", false}},
                      buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(
        request,
        createColumns(
            {toNullableVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
             toNullableVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}),
             toNullableVec<Int64>("order1", {1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2}),
             toNullableVec<Int64>("order2", {1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2}),
             toNullableVec<Int64>("row_number", {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})}));

    /***** rank, dense_rank *****/
    request = context.scan("test_db", "test_table_for_rank")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window({Rank(), DenseRank()}, {{"order", false}}, {{"partition", false}}, MockWindowFrame{})
                  .build(context);
    executeAndAssertColumnsEqual(
        request,
        createColumns(
            {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
             toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}),
             toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})}));

    // nullable
    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_for_rank",
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        createColumns(
            {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}),
             toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}),
             toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})}));

    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_for_rank",
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2})},
        createColumns(
            {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
             toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2}),
             toNullableVec<Int64>("rank", {1, 2, 1, 1, 3, 3, 1, 1, 3, 3}),
             toNullableVec<Int64>("dense_rank", {1, 2, 1, 1, 2, 2, 1, 1, 2, 2})}));
}
CATCH

TEST_F(WindowExecutorTestRunner, multiWindow)
try
{
    auto add_source = [&](bool is_table) {
        return is_table ? context.scan("test_db", "test_table") : context.receive("test_recv", 1);
    };

    std::vector<ASTPtr> functions = {DenseRank(), Rank()};
    ColumnsWithTypeAndName functions_result
        = {toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2}),
           toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3})};
    auto test_single_window_function = [&](size_t index) {
        std::vector<bool> bools{true, false};
        for (auto is_table : bools)
        {
            size_t stream_count = is_table ? 0 : 1;
            auto request
                = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(functions[index], {"order", false}, {"partition", false}, MockWindowFrame{}, stream_count)
                      .build(context);
            executeAndAssertColumnsEqual(
                request,
                createColumns(
                    {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
                     toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
                     functions_result[index]}));
        }
    };
    for (size_t i = 0; i < functions.size(); ++i)
        test_single_window_function(i);

    auto gen_test_window_request = [&](const std::vector<ASTPtr> & wfs) {
        std::vector<std::shared_ptr<tipb::DAGRequest>> requests;
        std::vector<bool> bools{true, false};

        // merge window request
        for (auto is_table : bools)
        {
            size_t stream_count = is_table ? 0 : 1;
            requests.push_back(
                add_source(is_table)
                    .sort({{"partition", false}, {"order", false}}, true, stream_count)
                    .window(wfs, {{"order", false}}, {{"partition", false}}, MockWindowFrame(), stream_count)
                    .build(context));
        }

        // spilt window request
        for (auto is_table : bools)
        {
            size_t stream_count = is_table ? 0 : 1;
            auto req = add_source(is_table).sort({{"partition", false}, {"order", false}}, true, stream_count);
            for (const auto & wf : wfs)
                req.window(wf, {"order", false}, {"partition", false}, MockWindowFrame(), stream_count);
            requests.push_back(req.build(context));
        }

        return requests;
    };

    std::vector<ASTPtr> wfs;
    ColumnsWithTypeAndName wfs_result
        = {{toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2})},
           {toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})}};
    for (size_t i = 0; i < functions.size(); ++i)
    {
        wfs.push_back(functions[i]);
        wfs_result.push_back(functions_result[i]);
        for (size_t j = 0; j < functions.size(); ++j)
        {
            wfs.push_back(functions[j]);
            wfs_result.push_back(functions_result[j]);
            for (size_t k = 0; k < functions.size(); ++k)
            {
                wfs.push_back(functions[k]);
                wfs_result.push_back(functions_result[k]);

                auto requests = gen_test_window_request(wfs);
                for (const auto & request : requests)
                    executeAndAssertColumnsEqual(request, wfs_result);

                wfs.pop_back();
                wfs_result.pop_back();
            }
            wfs.pop_back();
            wfs_result.pop_back();
        }
        wfs.pop_back();
        wfs_result.pop_back();
    }
}
CATCH

TEST_F(WindowExecutorTestRunner, multiWindowThenAgg)
try
{
    auto add_source = [&](bool is_table) {
        return is_table ? context.scan("test_db", "test_table") : context.receive("test_recv", 1);
    };
    std::vector<bool> bools{true, false};
    for (auto is_table : bools)
    {
        size_t stream_count = is_table ? 0 : 1;

        /*
        select count(1) from (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order`),
                ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order` DESC)
            FROM `test_db`.`test_table`
        )t1;
        */
        auto request
            = add_source(is_table)
                  .sort({{"partition", false}, {"order", false}}, true, stream_count)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame(), stream_count)
                  .sort({{"partition", false}, {"order", true}}, true, stream_count)
                  .window(RowNumber(), {"order", true}, {"partition", false}, buildDefaultRowsFrame(), stream_count)
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        /*
        select count(1) from (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order`),
                ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order`)
            FROM `test_db`.`test_table`
        )t1;
        */
        request
            = add_source(is_table)
                  .sort({{"partition", false}, {"order", false}}, true, stream_count)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame(), stream_count)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame(), stream_count)
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        request = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(
                          {RowNumber(), RowNumber()},
                          {{"order", false}},
                          {{"partition", false}},
                          buildDefaultRowsFrame(),
                          stream_count)
                      .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        /*
        select count(1) from (
            SELECT
                Rank() OVER (PARTITION BY `partition` ORDER BY  `order`),
                DenseRank() OVER (PARTITION BY `partition` ORDER BY  `order`)
            FROM `test_db`.`test_table`
        )t1;
        */
        request = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(Rank(), {"order", false}, {"partition", false}, MockWindowFrame(), stream_count)
                      .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame(), stream_count)
                      .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        request = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(
                          {Rank(), DenseRank()},
                          {{"order", false}},
                          {{"partition", false}},
                          MockWindowFrame(),
                          stream_count)
                      .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        /*
        select count(1) from (
            SELECT
                DenseRank() OVER (PARTITION BY `partition` ORDER BY  `order`),
                DenseRank() OVER (PARTITION BY `partition` ORDER BY  `order`),
                Rank() OVER (PARTITION BY `partition` ORDER BY  `order`)
            FROM `test_db`.`test_table`
        )t1;
        */
        request = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(
                          {DenseRank(), DenseRank(), Rank()},
                          {{"order", false}},
                          {{"partition", false}},
                          MockWindowFrame(),
                          stream_count)
                      .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

        request = add_source(is_table)
                      .sort({{"partition", false}, {"order", false}}, true, stream_count)
                      .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame(), stream_count)
                      .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame(), stream_count)
                      .window(Rank(), {"order", false}, {"partition", false}, MockWindowFrame(), stream_count)
                      .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                      .build(context);
        executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));
    }
}
CATCH

TEST_F(WindowExecutorTestRunner, functionAsArgument)
try
{
    ColumnsWithTypeAndName result
        = {{toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2})},
           {toNullableVec<Int64>("order", {1, 2, 3, 4, 5, 6, 7, 8})},
           {toNullableVec<String>("value", {"a", "b", "c", "d", "e", "f", "g", "h"})}};
    auto request = context.scan("test_db", "test_table_for_lead_lag")
                       .sort({{"partition", false}, {"order", false}}, true)
                       .window(
                           Lead1(concat(col("value"), col("value"))),
                           {"order", false},
                           {"partition", false},
                           MockWindowFrame())
                       .build(context);
    result.emplace_back(toNullableVec<String>({"bb", "cc", "dd", {}, "ff", "gg", "hh", {}}));
    executeAndAssertColumnsEqual(request, result);
    result.pop_back();

    request = context.scan("test_db", "test_table_for_lead_lag")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(
                      Lag2(concat(col("value"), lit(Field(String("0")))), lit(Field(static_cast<UInt64>(2)))),
                      {"order", false},
                      {"partition", false},
                      MockWindowFrame())
                  .build(context);
    result.emplace_back(toNullableVec<String>({{}, {}, "a0", "b0", {}, {}, "e0", "f0"}));
    executeAndAssertColumnsEqual(request, result);
    result.pop_back();

    request = context.scan("test_db", "test_table_for_lead_lag")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(
                      Lead2(
                          concat(col("value"), concat(lit(Field(String("0"))), col("value"))),
                          lit(Field(static_cast<UInt64>(1)))),
                      {"order", false},
                      {"partition", false},
                      MockWindowFrame())
                  .build(context);
    result.emplace_back(toNullableVec<String>({"b0b", "c0c", "d0d", {}, "f0f", "g0g", "h0h", {}}));
    executeAndAssertColumnsEqual(request, result);
}
CATCH

TEST_F(WindowExecutorTestRunner, fineGrainedShuffle)
try
{
    DB::MockColumnInfoVec column_infos{{"partition", TiDB::TP::TypeLong}, {"order", TiDB::TP::TypeLong}};
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
        .addExchangeReceiver("exchange_receiver_3_concurrency", column_infos, column_data, 3, partition_column_infos);
    context
        .addExchangeReceiver("exchange_receiver_5_concurrency", column_infos, column_data, 5, partition_column_infos);
    context
        .addExchangeReceiver("exchange_receiver_10_concurrency", column_infos, column_data, 10, partition_column_infos);
    std::vector<size_t> exchange_receiver_concurrency = {1, 3, 5, 10};

    auto gen_request = [&](size_t exchange_concurrency) {
        return context
            .receive(fmt::format("exchange_receiver_{}_concurrency", exchange_concurrency), exchange_concurrency)
            .sort({{"partition", false}, {"order", false}}, true, exchange_concurrency)
            .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame(), exchange_concurrency)
            .build(context);
    };

    auto baseline = executeStreams(gen_request(1), 1);
    for (size_t exchange_concurrency : exchange_receiver_concurrency)
    {
        executeAndAssertColumnsEqual(gen_request(exchange_concurrency), baseline);
    }
}
CATCH

} // namespace DB::tests
