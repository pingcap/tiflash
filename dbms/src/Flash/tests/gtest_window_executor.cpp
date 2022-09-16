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

namespace DB::tests
{
class WindowExecutorTestRunner : public DB::tests::ExecutorTest
{
    static const size_t max_concurrency_level = 10;

public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable(
            {"test_db", "test_table"},
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})});
        context.addMockTable(
            {"test_db", "test_table_string"},
            {{"partition", TiDB::TP::TypeString}, {"order", TiDB::TP::TypeString}},
            {toVec<String>("partition", {"banana", "banana", "banana", "banana", "apple", "apple", "apple", "apple"}),
             toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"})});

        context.addMockTable(
            {"test_db", "test_table_more_cols"},
            {{"partition1", TiDB::TP::TypeLongLong}, {"partition2", TiDB::TP::TypeLongLong}, {"order1", TiDB::TP::TypeLongLong}, {"order2", TiDB::TP::TypeLongLong}},
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
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})});

        context.addMockTable(
            {"test_db", "test_table_for_lead_lag"},
            {{"partition", TiDB::TP::TypeLongLong}, {"order", TiDB::TP::TypeLongLong}, {"value", TiDB::TP::TypeString}},
            {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
             toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 7, 8}),
             toVec<String>("value", {"a", "b", "c", "d", "e", "f", "g", "h"})});
    }

    void executeWithTableScanAndConcurrency(const std::shared_ptr<tipb::DAGRequest> & request, const String & db, const String & table_name, const ColumnsWithTypeAndName & source_columns, const ColumnsWithTypeAndName & expect_columns)
    {
        context.addMockTableColumnData(db, table_name, source_columns);
        executeAndAssertColumnsEqual(request, expect_columns);
    }
};

TEST_F(WindowExecutorTestRunner, testWindowFunctionByPartitionAndOrder)
try
{
    /***** row_number with different types of input *****/
    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    auto request = context
                       .scan("test_db", "test_table")
                       .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                       .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                       .build(context);
    executeAndAssertColumnsEqual(
        request,
        createColumns({toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
                       toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
                       toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // null input
    executeWithTableScanAndConcurrency(request,
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
        createColumns({toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
                       toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}),
                       toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    request = context
                  .scan("test_db", "test_table_string")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(request,
                                 createColumns({toNullableVec<String>("partition", {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
                                                toNullableVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
                                                toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(request,
                                       "test_db",
                                       "test_table_string",
                                       {toNullableVec<String>("partition", {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}),
                                        toNullableVec<String>("order", {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
                                       createColumns({toNullableVec<String>("partition", {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
                                                      toNullableVec<String>("order", {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
                                                      toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // float64 - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_float order by order_float64)
    request = context
                  .scan("test_db", "test_table_float64")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(request,
                                 createColumns({toNullableVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
                                                toNullableVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
                                                toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(request,
                                       "test_db",
                                       "test_table_float64",
                                       {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
                                        toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
                                       createColumns({toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
                                                      toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
                                                      toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    request = context
                  .scan("test_db", "test_table_datetime")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);
    executeWithTableScanAndConcurrency(request,
                                       "test_db",
                                       "test_table_datetime",
                                       {toNullableDatetimeVec("partition", {"20220101010102", "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
                                        toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
                                       createColumns({toNullableDatetimeVec("partition", {"20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
                                                      toNullableDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
                                                      toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})}));

    // nullable
    executeWithTableScanAndConcurrency(request,
                                       "test_db",
                                       "test_table_datetime",
                                       {toNullableDatetimeVec("partition", {"20220101010102", {}, "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
                                        toNullableDatetimeVec("order", {"20220101010101", {}, "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
                                       createColumns({toNullableDatetimeVec("partition", {{}, "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
                                                      toNullableDatetimeVec("order", {{}, "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
                                                      toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})}));

    // 2 partiton key and 2 order key
    // sql : select *, row_number() over w1 from test6 window w1 as (partition by partition_int1, partition_int2 order by order_int1,order_int2)
    request = context
                  .scan("test_db", "test_table_more_cols")
                  .sort({{"partition1", false}, {"partition2", false}, {"order1", false}, {"order2", false}}, true)
                  .window(RowNumber(), {{"order1", false}, {"order2", false}}, {{"partition1", false}, {"partition2", false}}, buildDefaultRowsFrame())
                  .build(context);

    executeAndAssertColumnsEqual(request,
                                 createColumns({toNullableVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
                                                toNullableVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}),
                                                toNullableVec<Int64>("order1", {1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2}),
                                                toNullableVec<Int64>("order2", {1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2}),
                                                toNullableVec<Int64>("row_number", {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})}));

    /***** rank, dense_rank *****/
    request = context.scan("test_db", "test_table_for_rank").sort({{"partition", false}, {"order", false}}, true).window({Rank(), DenseRank()}, {{"order", false}}, {{"partition", false}}, MockWindowFrame{}).build(context);
    executeAndAssertColumnsEqual(request,
                                 createColumns({toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
                                                toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
                                                toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}),
                                                toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})}));

    // nullable
    executeWithTableScanAndConcurrency(request,
                                       "test_db",
                                       "test_table_for_rank",
                                       {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
                                        toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
                                       createColumns({toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
                                                      toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}),
                                                      toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}),
                                                      toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})}));

    executeWithTableScanAndConcurrency(
        request,
        "test_db",
        "test_table_for_rank",
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2})},
        createColumns({toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
                       toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2}),
                       toNullableVec<Int64>("rank", {1, 2, 1, 1, 3, 3, 1, 1, 3, 3}),
                       toNullableVec<Int64>("dense_rank", {1, 2, 1, 1, 2, 2, 1, 1, 2, 2})}));
}
CATCH

TEST_F(WindowExecutorTestRunner, multiWindow)
try
{
    std::vector<ASTPtr> functions = {DenseRank(), Rank()};
    ColumnsWithTypeAndName functions_result = {toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3})};
    auto test_single_window_function = [&](size_t index) {
        auto request = context
                           .scan("test_db", "test_table")
                           .sort({{"partition", false}, {"order", false}}, true)
                           .window(functions[index], {"order", false}, {"partition", false}, MockWindowFrame{})
                           .build(context);
        executeAndAssertColumnsEqual(request,
                                     createColumns({toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
                                                    toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
                                                    functions_result[index]}));
    };
    for (size_t i = 0; i < functions.size(); ++i)
        test_single_window_function(i);

    auto gen_merge_window_request = [&](const std::vector<ASTPtr> & wfs) {
        return context
            .scan("test_db", "test_table")
            .sort({{"partition", false}, {"order", false}}, true)
            .window(wfs, {{"order", false}}, {{"partition", false}}, MockWindowFrame())
            .build(context);
    };

    auto gen_split_window_request = [&](const std::vector<ASTPtr> & wfs) {
        auto req = context
                       .scan("test_db", "test_table")
                       .sort({{"partition", false}, {"order", false}}, true);
        for (const auto & wf : wfs)
            req.window(wf, {"order", false}, {"partition", false}, MockWindowFrame());
        return req.build(context);
    };

    std::vector<ASTPtr> wfs;
    ColumnsWithTypeAndName wfs_result = {{toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2})}, {toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})}};
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

                executeAndAssertColumnsEqual(gen_merge_window_request(wfs), wfs_result);
                executeAndAssertColumnsEqual(gen_split_window_request(wfs), wfs_result);

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
    /*
    select count(1) from (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order`),
            ROW_NUMBER() OVER (PARTITION BY `partition` ORDER BY  `order` DESC)
        FROM `test_db`.`test_table`
    )t1;
    */
    auto request = context
                       .scan("test_db", "test_table")
                       .sort({{"partition", false}, {"order", false}}, true)
                       .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                       .sort({{"partition", false}, {"order", true}}, true)
                       .window(RowNumber(), {"order", true}, {"partition", false}, buildDefaultRowsFrame())
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
    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
    executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window({RowNumber(), RowNumber()}, {{"order", false}}, {{"partition", false}}, buildDefaultRowsFrame())
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
    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(Rank(), {"order", false}, {"partition", false}, MockWindowFrame())
                  .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame())
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
    executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window({Rank(), DenseRank()}, {{"order", false}}, {{"partition", false}}, MockWindowFrame())
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
    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window({DenseRank(), DenseRank(), Rank()}, {{"order", false}}, {{"partition", false}}, MockWindowFrame())
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
    executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));

    request = context
                  .scan("test_db", "test_table")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame())
                  .window(DenseRank(), {"order", false}, {"partition", false}, MockWindowFrame())
                  .window(Rank(), {"order", false}, {"partition", false}, MockWindowFrame())
                  .aggregation({Count(lit(Field(static_cast<UInt64>(1))))}, {})
                  .build(context);
    executeAndAssertColumnsEqual(request, createColumns({toVec<UInt64>({8})}));
}
CATCH

TEST_F(WindowExecutorTestRunner, functionAsArgument)
try
{
    ColumnsWithTypeAndName result = {
        {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2})},
        {toNullableVec<Int64>("order", {1, 2, 3, 4, 5, 6, 7, 8})},
        {toNullableVec<String>("value", {"a", "b", "c", "d", "e", "f", "g", "h"})}};
    auto request = context
                       .scan("test_db", "test_table_for_lead_lag")
                       .sort({{"partition", false}, {"order", false}}, true)
                       .window(Lead1(concat(col("value"), col("value"))), {"order", false}, {"partition", false}, MockWindowFrame())
                       .build(context);
    result.emplace_back(toNullableVec<String>({"bb", "cc", "dd", {}, "ff", "gg", "hh", {}}));
    executeAndAssertColumnsEqual(request, result);
    result.pop_back();

    request = context
                  .scan("test_db", "test_table_for_lead_lag")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(Lag2(concat(col("value"), lit(Field(String("0")))), lit(Field(static_cast<UInt64>(2)))), {"order", false}, {"partition", false}, MockWindowFrame())
                  .build(context);
    result.emplace_back(toNullableVec<String>({{}, {}, "a0", "b0", {}, {}, "e0", "f0"}));
    executeAndAssertColumnsEqual(request, result);
    result.pop_back();

    request = context
                  .scan("test_db", "test_table_for_lead_lag")
                  .sort({{"partition", false}, {"order", false}}, true)
                  .window(Lead2(concat(col("value"), concat(lit(Field(String("0"))), col("value"))), lit(Field(static_cast<UInt64>(1)))), {"order", false}, {"partition", false}, MockWindowFrame())
                  .build(context);
    result.emplace_back(toNullableVec<String>({"b0b", "c0c", "d0d", {}, "f0f", "g0g", "h0h", {}}));
    executeAndAssertColumnsEqual(request, result);
}
CATCH

} // namespace DB::tests
