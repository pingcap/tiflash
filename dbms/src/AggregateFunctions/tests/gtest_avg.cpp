#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/AggregateDescription.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>

namespace DB::tests
{
class TestAggFuncAvg : public DB::tests::AggregateFunctionTest
{
};

/***
 * +---------+------------+----------+-------------+-------------+-------------+
   | int_col | string_col | real_col | decimal_col | group_col_1 | group_col_2 |
   +---------+------------+----------+-------------+-------------+-------------+
   |       1 | a          |      1.1 |      1.1000 |           1 |           1 |
   |       2 | b          |      2.2 |      2.2200 |           1 |        NULL |
   |       3 | c          |      3.3 |      3.3300 |           2 |           2 |
   |       4 | d          |      4.4 |      4.4400 |           2 |           2 |
   |    NULL | NULL       |     NULL |        NULL |        NULL |        NULL |
   |       6 | NULL       |     NULL |        NULL |           1 |        NULL |
   |    NULL | NULL       |     NULL |        NULL |        NULL |           1 |
   +---------+------------+----------+-------------+-------------+-------------+
 */

using DecimalField32 = DecimalField<Decimal32>;

ColumnWithTypeAndName int_col_without_null_value = createColumn<Nullable<Int64>>({1, 2, 3, 4});
ColumnWithTypeAndName int_col_with_null_value = createColumn<Nullable<Int64>>({1, 2, 3, 4, {}, 6, {}});
ColumnWithTypeAndName string_col_without_null_value = createColumn<Nullable<String>>({"a", "b", "c", "e"});
ColumnWithTypeAndName string_col_with_null_value = createColumn<Nullable<String>>({"a", "b", "c", "e", {}, {}, {}});
ColumnWithTypeAndName real_col_without_null_value = createColumn<Nullable<double>>({1.1, 2.2, 3.3, 4.4});
ColumnWithTypeAndName real_col_with_null_value = createColumn<Nullable<double>>({1.1, 2.2, 3.3, 4.4, {}, {}, {}});
ColumnWithTypeAndName decimal_col_without_null_value = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 4), {DecimalField32(1, 11), DecimalField32(2, 22), DecimalField32(3, 33), DecimalField32(4, 44)});
ColumnWithTypeAndName decimal_col_with_null_value = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 4), {DecimalField32(1, 11), DecimalField32(2, 22), DecimalField32(3, 33), DecimalField32(4, 44), {}, {}, {}});
ColumnWithTypeAndName group_col_1 = createColumn<Nullable<Int64>>({1,1,2,2,{},1,{}});
ColumnWithTypeAndName group_col_2 = createColumn<Nullable<Int64>>({1,{},2,2,{},{},1});

TEST_F(TestAggFuncAvg, aggAvgTest)
try
{
    // select count(col)
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_with_null_value},
                                 ColumnNumbers{}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_with_null_value},
                                 ColumnNumbers{}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_with_null_value},
                                 ColumnNumbers{}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_with_null_value},
                                 ColumnNumbers{}));

    // select count(col) group by col1

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_without_null_value, group_col_1},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({3, 2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_with_null_value, group_col_1},
                                 ColumnNumbers{1}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_without_null_value, group_col_1},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({3, 2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_with_null_value, group_col_1},
                                 ColumnNumbers{1}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_without_null_value, group_col_1},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({3, 2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_with_null_value, group_col_1},
                                 ColumnNumbers{1}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_without_null_value, group_col_1},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({3, 2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_with_null_value, group_col_1},
                                 ColumnNumbers{1}));

    // select count(col) group by col1, col2

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2,1,1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1,1,2,1,2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2,1,1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1,1,2,1,2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2,1,1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1,1,2,1,2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{real_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2,1,1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1,1,2,1,2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    // test empty_result_for_aggregation_by_empty_set is true
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({0}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{createColumn<UInt64>({})},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({{}}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{createColumn<UInt64>({})},
                                 ColumnNumbers{},
                                 true));
}
CATCH

} // namespace DB::tests