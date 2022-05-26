#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
class TestAggFuncCount : public DB::tests::AggregateFunctionTest
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
   |       7 | NULL       |     NULL |        NULL |        NULL |        NULL |
   |       8 | NULL       |     NULL |        NULL |        NULL |        NULL |
   |       9 | NULL       |     NULL |        NULL |        NULL |        NULL |
   |      10 | NULL       |     NULL |        NULL |        NULL |        NULL |
   +---------+------------+----------+-------------+-------------+-------------+
 */

using DecimalField32 = DecimalField<Decimal32>;

ColumnWithTypeAndName int_col_without_null_value = createColumn<Nullable<Int64>>({1, 2, 3, 4});
ColumnWithTypeAndName int_col_with_null_value = createColumn<Nullable<Int64>>({1, 2, 3, 4, {}, 6, {}});
ColumnWithTypeAndName int_col_all_null_value = createColumn<Nullable<Int64>>({{}, {}, {}, {}});

ColumnWithTypeAndName string_col_without_null_value = createColumn<Nullable<String>>({"a", "b", "c", "e"});
ColumnWithTypeAndName string_col_with_null_value = createColumn<Nullable<String>>({"a", "b", "c", "e", {}, {}, {}});
ColumnWithTypeAndName string_col_all_null_value = createColumn<Nullable<String>>({{}, {}, {}, {}});

ColumnWithTypeAndName double_col_without_null_value = createColumn<Nullable<double>>({1.1, 2.2, 3.3, 4.4});
ColumnWithTypeAndName double_col_with_null_value = createColumn<Nullable<double>>({1.1, 2.2, 3.3, 4.4, {}, {}, {}});
ColumnWithTypeAndName double_col_all_null_value = createColumn<Nullable<double>>({{}, {}, {}, {}});

ColumnWithTypeAndName float_col_without_null_value = createColumn<Nullable<float>>({1.1f, 2.2f, 3.3f, 4.4f});
ColumnWithTypeAndName float_col_with_null_value = createColumn<Nullable<float>>({1.1f, 2.2f, 3.3f, 4.4f, {}, {}, {}});
ColumnWithTypeAndName float_col_all_null_value = createColumn<Nullable<float>>({{}, {}, {}, {}});

ColumnWithTypeAndName decimal_col_without_null_value = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 4), {DecimalField32(1, 11), DecimalField32(2, 22), DecimalField32(3, 33), DecimalField32(4, 44)});
ColumnWithTypeAndName decimal_col_with_null_value = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 4), {DecimalField32(1, 11), DecimalField32(2, 22), DecimalField32(3, 33), DecimalField32(4, 44), {}, {}, {}});
ColumnWithTypeAndName decimal_col_all_null_value = createColumn<Nullable<Decimal32>>({{}, {}, {}, {}});

ColumnWithTypeAndName only_null_col = createOnlyNullColumn(5);

ColumnWithTypeAndName group_col_1 = createColumn<Nullable<Int64>>({1, 1, 2, 2, {}, 1, {}});
ColumnWithTypeAndName group_col_2 = createColumn<Nullable<Int64>>({1, {}, 2, 2, {}, {}, 1});
ColumnWithTypeAndName group_col_all_null_value = createColumn<Nullable<Int64>>({{}, {}, {}, {}});

TEST_F(TestAggFuncCount, aggCountTest)
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
                                 ColumnsWithTypeAndName{double_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_with_null_value},
                                 ColumnNumbers{}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_without_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({7}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_with_null_value},
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
                                 ColumnsWithTypeAndName{double_col_without_null_value, group_col_1},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({3, 2, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_with_null_value, group_col_1},
                                 ColumnNumbers{1}));

        ASSERT_COLUMN_EQ(
            createColumn<UInt64>({2, 2}),
            executeAggregateFunction("count",
                                     ColumnsWithTypeAndName{float_col_without_null_value, group_col_1},
                                     ColumnNumbers{1}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt64>({3, 2, 2}),
            executeAggregateFunction("count",
                                     ColumnsWithTypeAndName{float_col_with_null_value, group_col_1},
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
        createColumn<UInt64>({2, 1, 1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1, 1, 2, 1, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 1, 1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1, 1, 2, 1, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 1, 1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1, 1, 2, 1, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 1, 1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1, 1, 2, 1, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({2, 1, 1}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_without_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({1, 1, 2, 1, 2}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_with_null_value, group_col_1, group_col_2},
                                 ColumnNumbers{1, 2}));

    // select count(col) where id >= 7
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_all_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_all_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_all_null_value},
                                 ColumnNumbers{}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_all_null_value},
                                 ColumnNumbers{}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_all_null_value},
                                 ColumnNumbers{}));

    // select count(col) group by group_col_1
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{int_col_all_null_value, group_col_all_null_value},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{string_col_all_null_value, group_col_all_null_value},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{decimal_col_all_null_value, group_col_all_null_value},
                                 ColumnNumbers{1}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{double_col_all_null_value, group_col_all_null_value},
                                 ColumnNumbers{1}));

    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({4}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{float_col_all_null_value, group_col_all_null_value},
                                 ColumnNumbers{1}));

    // test only null columns
    ASSERT_COLUMN_EQ(
        createColumn<UInt64>({5}),
        executeAggregateFunction("count",
                                 ColumnsWithTypeAndName{only_null_col},
                                 ColumnNumbers{}));

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

    {
        // test allow_to_use_two_level_group_by
        std::vector<Int64> data_value, group_by_value;
        std::vector<UInt64> result;
        for (size_t i = 0; i < 200000; i++)
        {
            data_value.push_back(i);
            group_by_value.push_back(i);
            result.push_back(1);
        }
        ColumnWithTypeAndName touch_two_level_threshold_col = createColumn<Int64>(data_value);
        ColumnWithTypeAndName touch_two_level_threshold_group_by = createColumn<Int64>(group_by_value);
        ASSERT_COLUMN_EQ(
            createColumn<UInt64>(result),
            executeAggregateFunction("count",
                                     ColumnsWithTypeAndName{touch_two_level_threshold_col, touch_two_level_threshold_group_by},
                                     ColumnNumbers{1},
                                     false,
                                     false));
//        ASSERT_COLUMN_EQ(
//            createColumn<UInt64>(result),
//            executeAggregateFunction("count",
//                                     ColumnsWithTypeAndName{touch_two_level_threshold_col, touch_two_level_threshold_group_by},
//                                     ColumnNumbers{1},
//                                     false,
//                                     true));
    }
}
CATCH

} // namespace DB::tests