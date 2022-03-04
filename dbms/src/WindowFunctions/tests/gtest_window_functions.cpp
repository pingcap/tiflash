#include <Core/Block.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/MockTableScanBlockInputStream.h>
#include <google/protobuf/util/json_util.h>

namespace DB::tests
{
class WindowFunction : public DB::tests::FunctionTest
{
protected:
    std::shared_ptr<DAGQueryBlockInterpreter> mock_interpreter;
    ColumnsWithTypeAndName mock_general_table_scan_columns;
    ColumnsWithTypeAndName mock_general_table_scan_columns1;

    void SetUp() override
    {
        DB::tests::FunctionTest::SetUp();

        mock_general_table_scan_columns.push_back(toIntVec("empid", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}));
        mock_general_table_scan_columns.push_back(toIntVec("deptid", {10, 10, 20, 20, 40, 40, 40, 50, 50, 10, 10, 20, 20, 40, 40, 50, 50}));
        mock_general_table_scan_columns.push_back(toFloatVec("salary", {5500.00, 4500.00, 1900.00, 4800.00, 6500.00, 14500.00, 44500.00, 6500.00, 7500.00, 3500.00, 2500.00, 3500.00, 2500.00, 3500.00, 2500.00, 3500.00, 2500.00}));

        std::vector<DataTypeMyDateTime::FieldType> date_time_with_fsp_data{
            MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
            MyDateTime(1969, 12, 31, 23, 23, 23, 999999).toPackedUInt(),
            MyDateTime(1970, 1, 1, 0, 0, 0, 1).toPackedUInt(),
            MyDateTime(1970, 1, 01, 0, 0, 1, 1).toPackedUInt(),
            MyDateTime(2020, 10, 10, 11, 11, 11, 123456).toPackedUInt(),
            MyDateTime(2038, 1, 19, 3, 14, 7, 999999).toPackedUInt(),
            MyDateTime(2038, 1, 19, 3, 14, 8, 000000).toPackedUInt(),
        };

        mock_general_table_scan_columns1.push_back(toIntVec("partition_int", {1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1}));
        mock_general_table_scan_columns1.push_back(toStringVec("partition_string", {"apple", "apple", "apple", "apple", "apple", "bear", "bear", "bear", "bear", "bear", "coffe", "coffe", "coffe", "coffe", "coffe", "bear", "apple", "apple", "apple", "apple", "apple"}));
        mock_general_table_scan_columns1.push_back(toFloatVec("partition_decimal", {10.00, 10.00, 10.00, 10.00, 10.00, 20.00, 20.00, 20.00, 20.00, 20.00, 30.00, 30.00, 30.00, 30.00, 30.00, 10.00, 20.00, 10.00, 10.00, 10.00, 10.00}));
        mock_general_table_scan_columns1.push_back(toDatetimeVec("partition_datetime", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt()}));

        mock_general_table_scan_columns1.push_back(toIntVec("order_int", {1, 3, 2, 4, 5, 1, 3, 2, 4, 5, 1, 3, 2, 4, 5, 10, 11, 13, 5, 5, 5}));
        mock_general_table_scan_columns1.push_back(toStringVec("order_string", {"apple", "coffe", "bear", "dest", "echo", "apple", "coffe", "bear", "dest", "echo", "apple", "coffe", "bear", "dest", "echo", "xxxxx", "yyyyy", "zzzzz", "far", "echo", "echo"}));
        mock_general_table_scan_columns1.push_back(toFloatVec("order_decimal", {10.00, 30.00, 20.00, 40.00, 50.00, 10.00, 30.00, 20.00, 40.00, 50.00, 10.00, 30.00, 20.00, 40.00, 50.00, 100.00, 110.00, 120.00, 50.00, 60.00, 50.00}));
        mock_general_table_scan_columns1.push_back(toDatetimeVec("order_datetime", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 6, 0).toPackedUInt()}));

        mock_general_table_scan_columns1.push_back(toIntVec("value_int", {1, 3, 2, 4, 5, 1, 3, 2, 4, 5, 1, 3, 2, 4, 5, 10, 11, 13, 6, 7, 8}));
        mock_general_table_scan_columns1.push_back(toStringVec("value_string", {"apple", "coffe", "bear", "dest", "echo", "apple", "coffe", "bear", "dest", "echo", "apple", "coffe", "bear", "dest", "echo", "hhhhh", "yyyyy", "zzzzz", "far", "good", "hello"}));
        mock_general_table_scan_columns1.push_back(toFloatVec("value_decimal", {10.00, 30.00, 20.00, 40.00, 50.00, 10.00, 30.00, 20.00, 40.00, 50.00, 10.00, 30.00, 20.00, 40.00, 50.00, 100.00, 110.00, 120.00, 60.00, 70.00, 80.00}));
        mock_general_table_scan_columns1.push_back(toDatetimeVec("value_datetime", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 4, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 5, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2023, 1, 1, 1, 1, 3, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 6, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 7, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 8, 0).toPackedUInt()}));
    }

    ColumnWithTypeAndName toDatetimeVec(String name, const std::vector<DataTypeMyDateTime::FieldType> & v)
    {
        return createColumn<MyDateTime>(v, name);
    }

    ColumnWithTypeAndName toNullableDatetimeVec(String name, const std::vector<std::optional<DataTypeMyDateTime::FieldType>> & v)
    {
        return createColumn<Nullable<MyDateTime>>(v, name);
    }

    ColumnWithTypeAndName toNullableStringVec(String name, const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v, name);
    }

    ColumnWithTypeAndName toStringVec(String name, const std::vector<DataTypeString::FieldType> & v)
    {
        return createColumn<String>(v, name);
    }

    ColumnWithTypeAndName toNullableIntVec(String name, const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v, name);
    }

    ColumnWithTypeAndName toIntVec(String name, const std::vector<Int64> & v)
    {
        return createColumn<Int64>(v, name);
    }

    ColumnWithTypeAndName toNullableFloatVec(String name, const std::vector<std::optional<Float64>> & v)
    {
        return createColumn<Nullable<Float64>>(v, name);
    }

    ColumnWithTypeAndName toFloatVec(String name, const std::vector<Float64> & v)
    {
        return createColumn<Float64>(v, name);
    }

    void setMaxBlockSize(int size)
    {
        context.getSettingsRef().max_block_size.set(size);
    }

    Blocks mockBlocks(std::vector<ColumnsWithTypeAndName> vec)
    {
        Blocks blocks;
        for (ColumnsWithTypeAndName cols : vec)
        {
            Block block(cols);
            blocks.push_back(block);
        }
        return blocks;
    }

    void mockInterpreter(std::vector<NameAndTypePair> source_columns, Context context)
    {
        std::vector<BlockInputStreams> mock_input_streams_vec = {};
        DAGQueryBlock mock_query_block(0, static_cast<const google::protobuf::RepeatedPtrField<tipb::Executor>>(nullptr));
        std::vector<SubqueriesForSets> mock_subqueries_for_sets = {};
        mock_interpreter = std::make_shared<DAGQueryBlockInterpreter>(context,
                                                                      mock_input_streams_vec,
                                                                      mock_query_block,
                                                                      1,
                                                                      false,
                                                                      mock_subqueries_for_sets);

        mock_interpreter->analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    }

    void mockExecuteTableScan(DAGPipeline & pipeline, ColumnsWithTypeAndName columns)
    {
        pipeline.streams.push_back(std::make_shared<MockTableScanBlockInputStream>(columns, context.getSettingsRef().max_block_size));
    }


    void mockExecuteWindowOrder(ExpressionActionsChain & chain, DAGPipeline & pipeline, std::string sort_json_str)
    {
        tipb::Sort sort;
        google::protobuf::util::JsonStringToMessage(sort_json_str, &sort);
        std::vector<NameAndTypePair> columns = (*mock_interpreter->analyzer).appendWindowOrderBy(chain, sort);
        mock_interpreter->executeWindowOrder(pipeline, getSortDescription(columns, sort.byitems()));
    }

    void mockExecuteWindow(ExpressionActionsChain & chain, DAGPipeline & pipeline, std::string window_json_str)
    {
        tipb::Window window;
        google::protobuf::util::JsonStringToMessage(window_json_str, &window);
        WindowDescription window_description = (*mock_interpreter->analyzer).appendWindow(chain, window);
        mock_interpreter->executeWindow(pipeline, window_description);
        mock_interpreter->executeExpression(pipeline, window_description.before_window_select);
    }

    void ASSERT_CHECK_BLOCK(const Block & lhs, const Block & rhs)
    {
        size_t columns = rhs.columns();
        ASSERT_TRUE(lhs.columns() == columns);

        for (size_t i = 0; i < columns; ++i)
        {
            const auto & expected = rhs.getByPosition(i);
            const auto & actual = lhs.getByPosition(i);
            std::cout << actual.type->getName() << "---" << expected.type->getName() << std::endl;
            ASSERT_TRUE(actual.type->equals(*expected.type));
            ASSERT_COLUMN_EQ(expected.column, actual.column);
        }
    }

    void ASSERT_CHECK_BLOCKS(Blocks expect_blocks, Blocks actual_blocks)
    {
        ASSERT_EQ(expect_blocks.size(), actual_blocks.size());
        for (size_t i = 0; i < actual_blocks.size(); ++i)
        {
            ASSERT_EQ(expect_blocks[i].rows(), actual_blocks[i].rows());
            ASSERT_CHECK_BLOCK(expect_blocks[i], actual_blocks[i]);
        }
    }

    Block mergeBlocks(Blocks blocks)
    {
        Block sample_block;
        std::vector<MutableColumnPtr> actual_cols;

        for (const auto & block : blocks)
        {
            if (!sample_block)
            {
                sample_block = block;
                for (const auto & column : block.getColumnsWithTypeAndName())
                {
                    actual_cols.push_back(column.type->createColumn());
                }
            }

            for (size_t i = 0; i < block.columns(); ++i)
            {
                for (size_t j = 0; j < block.rows(); j++)
                {
                    actual_cols[i]->insert((*(block.getColumnsWithTypeAndName())[i].column)[j]);
                }
            }
        }

        ColumnsWithTypeAndName actual_columns;

        for (size_t i = 0; i < actual_cols.size(); ++i)
        {
            actual_columns.push_back({std::move(actual_cols[i]), sample_block.getColumnsWithTypeAndName()[i].type, sample_block.getColumnsWithTypeAndName()[i].name, sample_block.getColumnsWithTypeAndName()[i].column_id});
        }
        return Block(actual_columns);
    }

    void testOneWindowFunction(const std::vector<NameAndTypePair> & source_column_types, const ColumnsWithTypeAndName & source_columns, const ColumnsWithTypeAndName & expect_columns, const std::string window_json_str, const std::string sort_json_str)
    {
        mockInterpreter(source_column_types, context);
        DAGPipeline pipeline;
        ExpressionActionsChain chain;
        Block except_block(expect_columns);

        mockExecuteTableScan(pipeline, source_columns);

        mockExecuteWindowOrder(chain, pipeline, sort_json_str);

        mockExecuteWindow(chain, pipeline, window_json_str);

        auto stream = pipeline.firstStream();

        Blocks actual_blocks;
        while (Block block = stream->read())
        {
            actual_blocks.push_back(block);
        }

        Block actual_block = mergeBlocks(actual_blocks);
        ASSERT_EQ(actual_blocks.size(), (actual_block.rows() - 1) / context.getSettingsRef().max_block_size + 1);
        ASSERT_CHECK_BLOCK(except_block, actual_block);
    }
};

TEST_F(WindowFunction, testWindowFunctionByPartitionAndOrder)
{
    setMaxBlockSize(3);

    std::vector<NameAndTypePair> source_column_types;
    ColumnsWithTypeAndName source_columns;
    ColumnsWithTypeAndName expect_columns;
    std::string window_json;
    std::string sort_json;


    /***** row_number with different types of input *****/
    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toIntVec("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toIntVec("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableIntVec("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toIntVec("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toIntVec("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableIntVec("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeString>()), NameAndTypePair("order", std::make_shared<DataTypeString>())},
        {toStringVec("partition", {"banana", "banana", "banana", "banana", "apple", "apple", "apple", "apple"}), toStringVec("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"})},
        {toStringVec("partition", {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toStringVec("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableIntVec("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeString>()), NameAndTypePair("order", std::make_shared<DataTypeString>())},
        {toNullableStringVec("partition", {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}), toNullableStringVec("order", {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
        {toNullableStringVec("partition", {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toNullableStringVec("order", {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableIntVec("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // decimal - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_decimal order by order_decimal)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeDecimal256>()), NameAndTypePair("order", std::make_shared<DataTypeDecimal256>())},
        {toFloatVec("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toFloatVec("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toFloatVec("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toFloatVec("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableIntVec("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toNullableFloatVec("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableFloatVec("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toNullableFloatVec("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableFloatVec("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableIntVec("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeMyDateTime>()), NameAndTypePair("order", std::make_shared<DataTypeMyDateTime>())},
        {toDatetimeVec("partition", {MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt()}),
         toDatetimeVec("order", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()})},
        {toDatetimeVec("partition", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()}),
         toDatetimeVec("order", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()}),
         toNullableIntVec("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeMyDateTime>()), NameAndTypePair("order", std::make_shared<DataTypeMyDateTime>())},
        {toNullableDatetimeVec("partition", {MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), {}, MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt()}),
         toNullableDatetimeVec("order", {MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), {}, MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()})},
        {toNullableDatetimeVec("partition", {{}, MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()}),
         toNullableDatetimeVec("order", {{}, MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 1, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt(), MyDateTime(2022, 1, 1, 1, 1, 2, 0).toPackedUInt()}),
         toNullableIntVec("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);


    // dense_rank, rank
    window_json = "{\"funcDesc\":[{\"tp\":\"Rank\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"DenseRank\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toIntVec("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toIntVec("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableIntVec("rank", {1, 1, 3, 3, 1, 1, 3, 3}), toNullableIntVec("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toIntVec("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toIntVec("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toIntVec("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableIntVec("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableIntVec("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);
}
} // namespace DB::tests
