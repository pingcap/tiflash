#include <AggregateFunctions/registerAggregateFunctions.h>
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

    ColumnWithTypeAndName toNullableStringVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    ColumnWithTypeAndName toStringVec(const std::vector<String> & v)
    {
        return createColumn<String>(v);
    }

    ColumnWithTypeAndName toNullableIntVec(String name, const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v, name);
    }

    ColumnWithTypeAndName toIntVec(String name, const std::vector<Int64> & v)
    {
        return createColumn<Int64>(v, name);
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
};
TEST_F(WindowFunction, allTests)
{
    registerAggregateFunctions();
    setMaxBlockSize(3);

    std::vector<NameAndTypePair> source_columns;
    source_columns.emplace_back(NameAndTypePair("empid", std::make_shared<DataTypeInt64>()));
    source_columns.emplace_back(NameAndTypePair("deptid", std::make_shared<DataTypeInt64>()));
    source_columns.emplace_back(NameAndTypePair("salary", std::make_shared<DataTypeFloat64>()));
    mockInterpreter(source_columns, context);

    DAGPipeline pipeline;
    ExpressionActionsChain chain;

    ColumnsWithTypeAndName mock_table_scan_columns;
    mock_table_scan_columns.push_back(toIntVec("empid", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}));
    mock_table_scan_columns.push_back(toIntVec("deptid", {10, 10, 20, 20, 40, 40, 40, 50, 50, 10, 10, 20, 20, 40, 40, 50, 50}));
    mock_table_scan_columns.push_back(toFloatVec("salary", {5500.00, 4500.00, 1900.00, 4800.00, 6500.00, 14500.00, 44500.00, 6500.00, 7500.00, 3500.00, 2500.00, 3500.00, 2500.00, 3500.00, 2500.00, 3500.00, 2500.00}));

    ColumnsWithTypeAndName expect_columns;
    expect_columns.push_back(toIntVec("empid", {1, 2, 10, 11, 4, 12, 13, 3, 7, 6, 5, 14, 15, 9, 8, 16, 17}));
    expect_columns.push_back(toIntVec("deptid", {10, 10, 10, 10, 20, 20, 20, 20, 40, 40, 40, 40, 40, 50, 50, 50, 50}));
    expect_columns.push_back(toFloatVec("salary", {5500.00, 4500.00, 3500.00, 2500.00, 4800.00, 3500.00, 2500.00, 1900.00, 44500.00, 14500.00, 6500.00, 3500.00, 2500.00, 7500.00, 6500.00, 3500.00, 2500.00}));
    expect_columns.push_back(toNullableIntVec("row_number", {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4}));
    Block except_block(expect_columns);

    mockExecuteTableScan(pipeline, mock_table_scan_columns);

    std::string sort_json_str = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":true},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":true}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkJSa2MT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    mockExecuteWindowOrder(chain, pipeline, sort_json_str);

    std::string window_json_str = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":true}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":true},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":true}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAgLrIs8T+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":10,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
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
} // namespace DB::tests
