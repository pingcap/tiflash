#include <Common/MyTime.h>
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

    void SetUp() override
    {
        DB::tests::FunctionTest::SetUp();
    }

    template <typename T>
    ColumnWithTypeAndName toNullableVec(String name, const std::vector<std::optional<typename TypeTraits<T>::FieldType>> & v)
    {
        return createColumn<Nullable<T>>(v, name);
    }

    template <typename T>
    ColumnWithTypeAndName toVec(String name, const std::vector<typename TypeTraits<T>::FieldType> & v)
    {
        return createColumn<T>(v, name);
    }

    template <typename T>
    ColumnWithTypeAndName toNullableDecimalVec(String name, const std::vector<std::optional<String>> & v, int prec, int scala)
    {
        return createColumn<Nullable<T>>(std::make_tuple(prec, scala), v, name);
    }

    ColumnWithTypeAndName toDatetimeVec(String name, const std::vector<String> & v, int fsp)
    {
        std::vector<typename TypeTraits<MyDateTime>::FieldType> vec;
        for (auto & value_str : v)
        {
            Field value = parseMyDateTime(value_str, fsp);
            vec.push_back(value.template safeGet<UInt64>());
        }
        DataTypePtr data_type = std::make_shared<DataTypeMyDateTime>(fsp);
        return {makeColumn<MyDateTime>(data_type, vec), data_type, name, 0};
    }

    ColumnWithTypeAndName toNullableDatetimeVec(String name, const std::vector<String> & v, int fsp)
    {
        std::vector<std::optional<typename TypeTraits<MyDateTime>::FieldType>> vec;
        for (auto & value_str : v)
        {
            if (!value_str.empty())
            {
                Field value = parseMyDateTime(value_str, fsp);
                vec.push_back(value.template safeGet<UInt64>());
            }
            else
            {
                vec.push_back({});
            }
        }
        DataTypePtr data_type = makeNullable(std::make_shared<DataTypeMyDateTime>(fsp));
        return {makeColumn<Nullable<MyDateTime>>(data_type, vec), data_type, name, 0};
    }

    void setMaxBlockSize(int size)
    {
        context.getSettingsRef().max_block_size.set(size);
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

        NamesWithAliases final_project;
        for (auto column : (*mock_interpreter->analyzer).source_columns)
        {
            final_project.push_back({column.name, ""});
        }
        mockExecuteProject(pipeline, final_project);
    }

    void mockExecuteProject(DAGPipeline & pipeline, NamesWithAliases & final_project)
    {
        mock_interpreter->executeProject(pipeline, final_project);
    }

    void ASSERT_CHECK_BLOCK(const Block & lhs, const Block & rhs)
    {
        size_t columns = rhs.columns();
        //        std::cout << lhs.columns() << "---" << columns << std::endl;
        //
        //        for (size_t i = 0; i < columns; ++i)
        //        {
        //            std::cout << rhs.getByPosition(i).name << std::endl;
        //        }

        ASSERT_TRUE(lhs.columns() == columns);

        for (size_t i = 0; i < columns; ++i)
        {
            const auto & expected = lhs.getByPosition(i);
            const auto & actual = rhs.getByPosition(i);
            std::cout << actual.type->getName() << "---" << expected.type->getName() << std::endl;
            ASSERT_TRUE(actual.type->getName() == expected.type->getName());
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
try
{
    setMaxBlockSize(2);

    std::string window_json;
    std::string sort_json;

    /***** row_number with different types of input *****/
    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeString>()), NameAndTypePair("order", std::make_shared<DataTypeString>())},
        {toVec<String>("partition", {"banana", "banana", "banana", "banana", "apple", "apple", "apple", "apple"}), toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"})},
        {toVec<String>("partition", {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeString>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeString>()))},
        {toNullableVec<String>("partition", {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}), toNullableVec<String>("order", {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
        {toNullableVec<String>("partition", {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toNullableVec<String>("order", {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // decimal - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_float order by order_decimal)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeDecimal256>()), NameAndTypePair("order", std::make_shared<DataTypeDecimal256>())},
        {toVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeMyDateTime>()), NameAndTypePair("order", std::make_shared<DataTypeMyDateTime>())},
        {toDatetimeVec("partition", {"20220101010102", "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toDatetimeVec("partition", {"20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeMyDateTime>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeMyDateTime>()))},
        {toNullableDatetimeVec("partition", {"20220101010102", {}, "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toNullableDatetimeVec("order", {"20220101010101", {}, "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toNullableDatetimeVec("partition", {{}, "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toNullableDatetimeVec("order", {{}, "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // 2 partiton key and 2 order key
    // sql :  select *, row_number() over w1 from test6 window w1 as (partition by partition_int1, partition_int2 order by order_int1,order_int2)
    window_json = "{\"funcDesc\":[{\"tp\":\"RowNumber\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkLXGqNT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAkLXGqNT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition1", std::make_shared<DataTypeInt64>()), NameAndTypePair("partition2", std::make_shared<DataTypeInt64>()), NameAndTypePair("order1", std::make_shared<DataTypeInt64>()), NameAndTypePair("order2", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}), toVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}), toVec<Int64>("order1", {2, 1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1}), toVec<Int64>("order2", {2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2, 1})},
        {toVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}), toVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}), toVec<Int64>("order1", {1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2}), toVec<Int64>("order2", {1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})},
        window_json,
        sort_json);

    /***** rank, dense_rank *****/
    window_json = "{\"funcDesc\":[{\"tp\":\"Rank\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"DenseRank\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":128,\"flen\":21,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);
}
CATCH

TEST_F(WindowFunction, testAggregationFunction)
try
{
    setMaxBlockSize(3);

    std::string window_json;
    std::string sort_json;

    /***** count, sum, avg, max, min *****/
    // default frame.
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1,
    //                 count(value_string) over w1, sum(value_string) over w1, avg(value_string) over w1, max(value_string) over w1, min(value_string) over w1,
    //                 count(value_float) over w1, sum(value_float) over w1, avg(value_float) over w1, max(value_float) over w1, min(value_float) over w1,
    //                 count(value_datetime) over w1, sum(value_datetime) over w1, avg(value_datetime) over w1, max(value_datetime) over w1, min(value_datetime) over w1
    //           from test5 window w1 as (partition by partition_int order by order_int);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false}],\"sig\":\"CastStringAsReal\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false}],\"sig\":\"CastStringAsReal\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAM=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},\"hasDistinct\":false},{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAQ=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAQ=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":28,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":28,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAQ=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAQ=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAQ=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAU=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAU=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastTimeAsReal\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAU=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastTimeAsReal\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAU=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAU=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Ranges\",\"start\":{\"type\":\"Preceding\",\"unbounded\":true,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\",\"calcFuncs\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}]}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoNCatdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAoNCatdT+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":254,\"flag\":0,\"flen\":32,\"decimal\":0,\"collate\":46,\"charset\":\"utf8mb4\"},{\"tp\":246,\"flag\":0,\"flen\":6,\"decimal\":2,\"collate\":63,\"charset\":\"binary\"},{\"tp\":12,\"flag\":128,\"flen\":26,\"decimal\":6,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("string", std::make_shared<DataTypeString>()),
         NameAndTypePair("float", std::make_shared<DataTypeFloat64>()),
         NameAndTypePair("datetime", std::make_shared<DataTypeMyDateTime>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<String>("string", {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "aaa", "bbb", "ccc", "ddd", "eee", "fff"}),
         toVec<Float64>("float", {1.00, 2.00, 3.00, 4.00, 5.00, 6.00, 1.00, 2.00, 3.00, 4.00, 5.00, 6.00}),
         toDatetimeVec("datetime", {"20220101010101", "20220101010102", "20220101010103", "20220101010104", "20220101010105", "20220101010106", "20220101010101", "20220101010102", "20220101010103", "20220101010104", "20220101010105", "20220101010106"}, 0)},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<String>("string", {"aaa", "ccc", "bbb", "ddd", "fff", "eee", "aaa", "ccc", "bbb", "ddd", "fff", "eee"}),
         toVec<Float64>("float", {1.00, 3.00, 2.00, 4.00, 6.00, 5.00, 1.00, 3.00, 2.00, 4.00, 6.00, 5.00}),
         toDatetimeVec("datetime", {"20220101010101", "20220101010103", "20220101010102", "20220101010104", "20220101010106", "20220101010105", "20220101010101", "20220101010103", "20220101010102", "20220101010104", "20220101010106", "20220101010105"}, 0),
         toVec<Int64>("count_int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableDecimalVec<Decimal128>("sum_int", {"1", "4", "6", "10", "16", "21", "1", "4", "6", "10", "16", "21"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000", "1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000"}, 15, 4),
         toNullableVec<Int32>("max_int", {1, 3, 3, 4, 6, 6, 1, 3, 3, 4, 6, 6}),
         toNullableVec<Int32>("min_int", {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
         toVec<Int64>("count_string", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableVec<Float64>("sum_string", {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
         toNullableVec<Float64>("avg_string", {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
         toNullableVec<String>("max_string", {"aaa", "ccc", "ccc", "ddd", "fff", "fff", "aaa", "ccc", "ccc", "ddd", "fff", "fff"}),
         toNullableVec<String>("min_string", {"aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa"}),
         toVec<Int64>("count_decimal", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableDecimalVec<Decimal128>("sum_decimal", {"1.00", "4.00", "6.00", "10.00", "16.00", "21.00", "1.00", "4.00", "6.00", "10.00", "16.00", "21.00"}, 28, 2),
         toNullableDecimalVec<Decimal64>("avg_decimal", {"1.000000", "2.000000", "2.000000", "2.500000", "3.200000", "3.500000", "1.000000", "2.000000", "2.000000", "2.500000", "3.200000", "3.500000"}, 10, 6),
         toNullableDecimalVec<Decimal32>("max_decimal", {"1.00", "3.00", "3.00", "4.00", "6.00", "6.00", "1.00", "3.00", "3.00", "4.00", "6.00", "6.00"}, 6, 2),
         toNullableDecimalVec<Decimal32>("min_decimal", {"1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00"}, 6, 2),
         toVec<Int64>("count_datetime", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableVec<Float64>("sum_datetime", {20220101010101, 40440202020204, 60660303030306, 80880404040410, 101100505050516, 121320606060621, 20220101010101, 40440202020204, 60660303030306, 80880404040410, 101100505050516, 121320606060621}),
         toNullableVec<Float64>("avg_datetime", {20220101010101.0000, 20220101010102.0000, 20220101010102.0000, 20220101010102.5000, 20220101010103.1992, 20220101010103.5000, 20220101010101.0000, 20220101010102.0000, 20220101010102.0000, 20220101010102.5000, 20220101010103.1992, 20220101010103.5000}),
         toNullableDatetimeVec("datetime", {"20220101010101", "20220101010103", "20220101010103", "20220101010104", "20220101010106", "20220101010106", "20220101010101", "20220101010103", "20220101010103", "20220101010104", "20220101010106", "20220101010106"}, 6),
         toNullableDatetimeVec("datetime", {"20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 6)

        },
        window_json,
        sort_json);

    // nullbale
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())),
         NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>())),
         NameAndTypePair("int", makeNullable(std::make_shared<DataTypeInt64>())),
         NameAndTypePair("string", makeNullable(std::make_shared<DataTypeString>())),
         NameAndTypePair("float", makeNullable(std::make_shared<DataTypeFloat64>())),
         NameAndTypePair("datetime", makeNullable(std::make_shared<DataTypeMyDateTime>()))},
        {toNullableVec<Int64>("partition", {{}, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 8, 1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toNullableVec<Int64>("int", {{}, {}, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableVec<String>("string", {{}, {}, "aaa", "bbb", "ccc", "ddd", "eee", "fff", "aaa", "bbb", "ccc", "ddd", "eee", "fff"}),
         toNullableVec<Float64>("float", {{}, {}, 1.00, 2.00, 3.00, 4.00, 5.00, 6.00, 1.00, 2.00, 3.00, 4.00, 5.00, 6.00}),
         toNullableDatetimeVec("datetime", {{}, {}, "20220101010101", "20220101010102", "20220101010103", "20220101010104", "20220101010105", "20220101010106", "20220101010101", "20220101010102", "20220101010103", "20220101010104", "20220101010105", "20220101010106"}, 0)},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2}),
         toNullableVec<Int64>("order", {{}, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 8}),
         toNullableVec<Int64>("int", {{}, 1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5, {}}),
         toNullableVec<String>("string", {{}, "aaa", "ccc", "bbb", "ddd", "fff", "eee", "aaa", "ccc", "bbb", "ddd", "fff", "eee", {}}),
         toNullableVec<Float64>("float", {{}, 1.00, 3.00, 2.00, 4.00, 6.00, 5.00, 1.00, 3.00, 2.00, 4.00, 6.00, 5.00, {}}),
         toNullableDatetimeVec("datetime", {{}, "20220101010101", "20220101010103", "20220101010102", "20220101010104", "20220101010106", "20220101010105", "20220101010101", "20220101010103", "20220101010102", "20220101010104", "20220101010106", "20220101010105", {}}, 0),
         toVec<Int64>("count_int", {0, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 6}),
         toNullableDecimalVec<Decimal128>("sum_int", {{}, "1", "4", "6", "10", "16", "21", "1", "4", "6", "10", "16", "21", "21"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {{}, "1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000", "1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000", "3.5000"}, 15, 4),
         toNullableVec<Int32>("max_int", {{}, 1, 3, 3, 4, 6, 6, 1, 3, 3, 4, 6, 6, 6}),
         toNullableVec<Int32>("min_int", {{}, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
         toVec<Int64>("count_string", {0, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 6}),
         toNullableVec<Float64>("sum_string", {{}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
         toNullableVec<Float64>("avg_string", {{}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
         toNullableVec<String>("max_string", {{}, "aaa", "ccc", "ccc", "ddd", "fff", "fff", "aaa", "ccc", "ccc", "ddd", "fff", "fff", "fff"}),
         toNullableVec<String>("min_string", {{}, "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa", "aaa"}),
         toVec<Int64>("count_decimal", {0, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 6}),
         toNullableDecimalVec<Decimal128>("sum_decimal", {{}, "1.00", "4.00", "6.00", "10.00", "16.00", "21.00", "1.00", "4.00", "6.00", "10.00", "16.00", "21.00", "21.00"}, 28, 2),
         toNullableDecimalVec<Decimal64>("avg_decimal", {{}, "1.000000", "2.000000", "2.000000", "2.500000", "3.200000", "3.500000", "1.000000", "2.000000", "2.000000", "2.500000", "3.200000", "3.500000", "3.500000"}, 10, 6),
         toNullableDecimalVec<Decimal32>("max_decimal", {{}, "1.00", "3.00", "3.00", "4.00", "6.00", "6.00", "1.00", "3.00", "3.00", "4.00", "6.00", "6.00", "6.00"}, 6, 2),
         toNullableDecimalVec<Decimal32>("min_decimal", {{}, "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00"}, 6, 2),
         toVec<Int64>("count_datetime", {0, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 6}),
         toNullableVec<Float64>("sum_datetime", {{}, 20220101010101, 40440202020204, 60660303030306, 80880404040410, 101100505050516, 121320606060621, 20220101010101, 40440202020204, 60660303030306, 80880404040410, 101100505050516, 121320606060621, 121320606060621}),
         toNullableVec<Float64>("avg_datetime", {{}, 20220101010101.0000, 20220101010102.0000, 20220101010102.0000, 20220101010102.5000, 20220101010103.1992, 20220101010103.5000, 20220101010101.0000, 20220101010102.0000, 20220101010102.0000, 20220101010102.5000, 20220101010103.1992, 20220101010103.5000, 20220101010103.5000}),
         toNullableDatetimeVec("datetime", {{}, "20220101010101", "20220101010103", "20220101010103", "20220101010104", "20220101010106", "20220101010106", "20220101010101", "20220101010103", "20220101010103", "20220101010104", "20220101010106", "20220101010106", "20220101010106"}, 6),
         toNullableDatetimeVec("datetime", {{}, "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 6)},
        window_json,
        sort_json);
}
CATCH

TEST_F(WindowFunction, testFrame)
try
{
    setMaxBlockSize(2);

    std::string window_json;
    std::string sort_json;

    // frame type : rows

    // between offset and offset in within the boundary
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between 2 preceding and 2 following);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"Preceding\",\"unbounded\":false,\"offset\":\"2\"},\"end\":{\"type\":\"Following\",\"unbounded\":false,\"offset\":\"2\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAgPK6sej+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAgPK6sej+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("count_int", {3, 4, 5, 5, 4, 3, 3, 4, 5, 5, 4, 3}),
         toNullableDecimalVec<Decimal128>("sum_int", {"6", "10", "16", "20", "17", "15", "6", "10", "16", "20", "17", "15"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"2.0000", "2.5000", "3.2000", "4.0000", "4.2500", "5.0000", "2.0000", "2.5000", "3.2000", "4.0000", "4.2500", "5.0000"}, 15, 4),
         toNullableVec<Int32>("max_int", {3, 4, 6, 6, 6, 6, 3, 4, 6, 6, 6, 6}),
         toNullableVec<Int32>("min_int", {1, 1, 1, 2, 2, 4, 1, 1, 1, 2, 2, 4})},
        window_json,
        sort_json);

    // between offset and offset out of the boundary
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between 2 following and 4 following);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"Following\",\"unbounded\":false,\"offset\":\"2\"},\"end\":{\"type\":\"Following\",\"unbounded\":false,\"offset\":\"4\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIOAgITu0+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIOAgITu0+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("count_int", {3, 3, 2, 1, 0, 0, 3, 3, 2, 1, 0, 0}),
         toNullableDecimalVec<Decimal128>("sum_int", {"12", "15", "11", "5", {}, {}, "12", "15", "11", "5", {}, {}}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"4.0000", "5.0000", "5.5000", "5.0000", {}, {}, "4.0000", "5.0000", "5.5000", "5.0000", {}, {}}, 15, 4),
         toNullableVec<Int32>("max_int", {6, 6, 6, 5, 0, 0, 6, 6, 6, 5, 0, 0}),
         toNullableVec<Int32>("min_int", {2, 4, 5, 5, 0, 0, 2, 4, 5, 5, 0, 0})},
        window_json,
        sort_json);

    // between offset and offset contains the boundary
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between 20 preceding and 20 following);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"Preceding\",\"unbounded\":false,\"offset\":\"20\"},\"end\":{\"type\":\"Following\",\"unbounded\":false,\"offset\":\"20\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAgJaG0+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAgJaG0+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("count_int", {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}),
         toNullableDecimalVec<Decimal128>("sum_int", {"21", "21", "21", "21", "21", "21", "21", "21", "21", "21", "21", "21"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000", "3.5000"}, 15, 4),
         toNullableVec<Int32>("max_int", {6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}),
         toNullableVec<Int32>("min_int", {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})},
        window_json,
        sort_json);

    // no boundary, have some problem
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between unbounded preceding and unbounded following);
    //    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA0MOGtOj+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    //    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGA0MOGtOj+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";
    //
    //    testOneWindowFunction(
    //        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
    //         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
    //         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
    //        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
    //         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
    //         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
    //        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
    //         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
    //         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
    //         toVec<Int64>("count_int", {6,6,6,6,6,6,6,6,6,6,6,6}),
    //         toNullableDecimalVec<Decimal128>("sum_int", {"21","21","21","21","21","21","21","21","21","21","21","21",}, 32, 0),
    //         toNullableDecimalVec<Decimal64>("avg_int", {"3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000","3.5000",}, 15, 4),
    //         toNullableVec<Int32>("max_int", {6,6,6,6,6,6,6,6,6,6,6,6}),
    //         toNullableVec<Int32>("min_int", {1,1,1,1,1,1,1,1,1,1,1,1})},
    //        window_json,
    //        sort_json
    //    );

    // between current row and unbounded
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between current row and unbounded following);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"},\"end\":{\"type\":\"Following\",\"unbounded\":true,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIOAgN+Jw+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIOAgN+Jw+j+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("count_int", {6, 5, 4, 3, 2, 1, 6, 5, 4, 3, 2, 1}),
         toNullableDecimalVec<Decimal128>("sum_int", {"21", "20", "17", "15", "11", "5", "21", "20", "17", "15", "11", "5"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"3.5000", "4.0000", "4.2500", "5.0000", "5.5000", "5.0000", "3.5000", "4.0000", "4.2500", "5.0000", "5.5000", "5.0000"}, 15, 4),
         toNullableVec<Int32>("max_int", {6, 6, 6, 6, 6, 5, 6, 6, 6, 6, 6, 5}),
         toNullableVec<Int32>("min_int", {1, 2, 2, 4, 5, 5, 1, 2, 2, 4, 5, 5})},
        window_json,
        sort_json);

    // between unbounded and current row
    // sql : select *, count(value_int) over w1, sum(value_int) over w1, avg(value_int) over w1, max(value_int) over w1, min(value_int) over w1 from test7
    // window w1 as (partition by partition_int order by order_int rows between unbounded preceding and current row);
    window_json = "{\"funcDesc\":[{\"tp\":\"Count\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":8,\"flag\":129,\"flen\":21,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Sum\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":10,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":32,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Avg\",\"children\":[{\"tp\":\"ScalarFunc\",\"val\":\"CAA=\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"CastIntAsDecimal\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":246,\"flag\":128,\"flen\":15,\"decimal\":4,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Max\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},{\"tp\":\"Min\",\"children\":[{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAI=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false}],\"partitionBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"orderBy\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"frame\":{\"type\":\"Rows\",\"start\":{\"type\":\"Preceding\",\"unbounded\":true,\"offset\":\"0\"},\"end\":{\"type\":\"CurrentRow\",\"unbounded\":false,\"offset\":\"0\"}},\"child\":{\"tp\":\"TypeSort\",\"executorId\":\"Sort_12\",\"sort\":{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsKCpxuj+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}}}";
    sort_json = "{\"byItems\":[{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAA=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false},{\"expr\":{\"tp\":\"ColumnRef\",\"val\":\"gAAAAAAAAAE=\",\"sig\":\"Unspecified\",\"fieldType\":{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},\"hasDistinct\":false},\"desc\":false}],\"isPartialSort\":true,\"child\":{\"tp\":\"TypeExchangeReceiver\",\"exchangeReceiver\":{\"encodedTaskMeta\":[\"CIGAsKCpxuj+BRABIg4xMjcuMC4wLjE6MzkzMA==\"],\"fieldTypes\":[{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"},{\"tp\":3,\"flag\":0,\"flen\":11,\"decimal\":0,\"collate\":63,\"charset\":\"binary\"}]},\"executorId\":\"ExchangeReceiver_11\"}}";

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("order", std::make_shared<DataTypeInt64>()),
         NameAndTypePair("int", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}),
         toVec<Int64>("order", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toVec<Int64>("int", {1, 3, 2, 4, 6, 5, 1, 3, 2, 4, 6, 5}),
         toVec<Int64>("count_int", {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}),
         toNullableDecimalVec<Decimal128>("sum_int", {"1", "4", "6", "10", "16", "21", "1", "4", "6", "10", "16", "21"}, 32, 0),
         toNullableDecimalVec<Decimal64>("avg_int", {"1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000", "1.0000", "2.0000", "2.0000", "2.5000", "3.2000", "3.5000"}, 15, 4),
         toNullableVec<Int32>("max_int", {1, 3, 3, 4, 6, 6, 1, 3, 3, 4, 6, 6}),
         toNullableVec<Int32>("min_int", {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})},
        window_json,
        sort_json);
}
CATCH

} // namespace DB::tests
