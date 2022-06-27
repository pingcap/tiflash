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
    executeStreams(
        request,
        {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});

    // null input
    executeStreamsWithSingleSource(
        request,
        {toNullableVec<Int64>("partition", {}), toNullableVec<Int64>("order", {})},
        {});

    // nullable
    executeStreamsWithSingleSource(
        request,
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), {toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})}},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})});

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    request = context
                  .scan("test_db", "test_table_string")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeStreams(
        request,
        {toNullableVec<String>("partition", {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
         toNullableVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});

    // nullable
    executeStreamsWithSingleSource(
        request,
        {toNullableVec<String>("partition", {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}),
         toNullableVec<String>("order", {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
        {toNullableVec<String>("partition", {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}),
         toNullableVec<String>("order", {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}),
         toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})});

    // float64 - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_float order by order_float64)
    request = context
                  .scan("test_db", "test_table_float64")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);

    executeStreams(
        request,
        {toNullableVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
         toNullableVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});

    // nullable
    executeStreamsWithSingleSource(
        request,
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
         toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}),
         toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}),
         toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})});

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    request = context
                  .scan("test_db", "test_table_datetime")
                  .sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true)
                  .window(RowNumber(), {"order", false}, {"partition", false}, buildDefaultRowsFrame())
                  .build(context);
    executeStreamsWithSingleSource(
        request,
        {toNullableDatetimeVec("partition", {"20220101010102", "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toNullableDatetimeVec("partition", {"20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toNullableDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})});

    // nullable
    executeStreamsWithSingleSource(
        request,
        {toNullableDatetimeVec("partition", {"20220101010102", {}, "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toNullableDatetimeVec("order", {"20220101010101", {}, "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toNullableDatetimeVec("partition", {{}, "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toNullableDatetimeVec("order", {{}, "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})});

    // 2 partiton key and 2 order key
    // sql : select *, row_number() over w1 from test6 window w1 as (partition by partition_int1, partition_int2 order by order_int1,order_int2)
    request = context
                  .scan("test_db", "test_table_more_cols")
                  .sort({{"partition1", false}, {"partition2", false}, {"order1", false}, {"order2", false}}, true)
                  .window(RowNumber(), {{"order1", false}, {"order2", false}}, {{"partition1", false}, {"partition2", false}}, buildDefaultRowsFrame())
                  .build(context);

    executeStreams(
        request,
        {toNullableVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toNullableVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}),
         toNullableVec<Int64>("order1", {1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2}),
         toNullableVec<Int64>("order2", {1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2}),
         toNullableVec<Int64>("row_number", {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})});

    /***** rank, dense_rank *****/
    request = context.scan("test_db", "test_table_for_rank").sort({{"partition", false}, {"order", false}}, true).window({Rank(), DenseRank()}, {{"order", false}}, {{"partition", false}}, MockWindowFrame{}).build(context);
    executeStreams(
        request,
        {toNullableVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}),
         toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})});

    // nullable
    executeStreamsWithSingleSource(
        request,
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}),
         toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})});

    executeStreamsWithSingleSource(
        request,
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2}),
         toNullableVec<Int64>("rank", {1, 2, 1, 1, 3, 3, 1, 1, 3, 3}),
         toNullableVec<Int64>("dense_rank", {1, 2, 1, 1, 2, 2, 1, 1, 2, 2})});
}
CATCH

} // namespace DB::tests
