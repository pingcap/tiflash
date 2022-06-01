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

#include <Common/MyTime.h>
#include <Core/Block.h>
#include <DataStreams/MockTableScanBlockInputStream.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <WindowFunctions/registerWindowFunctions.h>
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
        DB::registerWindowFunctions();
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
    static ColumnWithTypeAndName toConst(const T s)
    {
        return createConstColumn<T>(1, s);
    }

    static ColumnWithTypeAndName toDatetimeVec(String name, const std::vector<String> & v, int fsp)
    {
        std::vector<typename TypeTraits<MyDateTime>::FieldType> vec;
        for (const auto & value_str : v)
        {
            Field value = parseMyDateTime(value_str, fsp);
            vec.push_back(value.template safeGet<UInt64>());
        }
        DataTypePtr data_type = std::make_shared<DataTypeMyDateTime>(fsp);
        return {makeColumn<MyDateTime>(data_type, vec), data_type, name, 0};
    }

    static ColumnWithTypeAndName toNullableDatetimeVec(String name, const std::vector<String> & v, int fsp)
    {
        std::vector<std::optional<typename TypeTraits<MyDateTime>::FieldType>> vec;
        for (const auto & value_str : v)
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
                                                                      1);

        mock_interpreter->analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    }

    void mockExecuteTableScan(DAGPipeline & pipeline, ColumnsWithTypeAndName columns)
    {
        pipeline.streams.push_back(std::make_shared<MockTableScanBlockInputStream>(columns, context.getSettingsRef().max_block_size));
        mock_interpreter->input_streams_vec.push_back(pipeline.streams);
    }

    void mockExecuteProject(DAGPipeline & pipeline, NamesWithAliases & final_project)
    {
        mock_interpreter->executeProject(pipeline, final_project);
    }

    static Block mergeBlocks(Blocks blocks)
    {
        if (blocks.empty())
        {
            return {};
        }
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
                for (size_t j = 0; j < block.rows(); ++j)
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

    // TODO: This is a temporary method.
    void testOneWindowFunction(const std::vector<NameAndTypePair> & source_column_types, const ColumnsWithTypeAndName & source_columns, const ColumnsWithTypeAndName & expect_columns, const tipb::Window & window, const tipb::Sort & sort)
    {
        mockInterpreter(source_column_types, context);
        DAGPipeline pipeline;
        ExpressionActionsChain chain;
        Block except_block(expect_columns);

        mockExecuteTableScan(pipeline, source_columns);

        mock_interpreter->handleWindowOrder(pipeline, sort);
        mock_interpreter->input_streams_vec[0] = pipeline.streams;
        NamesWithAliases final_project;
        for (const auto & column : (*mock_interpreter->analyzer).source_columns)
        {
            final_project.push_back({column.name, ""});
        }
        mockExecuteProject(pipeline, final_project);

        mock_interpreter->handleWindow(pipeline, window);
        mock_interpreter->input_streams_vec[0] = pipeline.streams;
        NamesWithAliases final_project_1;
        for (const auto & column : (*mock_interpreter->analyzer).source_columns)
        {
            final_project_1.push_back({column.name, ""});
        }
        mockExecuteProject(pipeline, final_project_1);

        auto stream = pipeline.firstStream();

        Blocks actual_blocks;
        while (Block block = stream->read())
        {
            actual_blocks.push_back(block);
        }

        Block actual_block = mergeBlocks(actual_blocks);

        if (actual_block)
        {
            // Check that input columns is properly split to many blocks
            ASSERT_EQ(actual_blocks.size(), (actual_block.rows() - 1) / context.getSettingsRef().max_block_size + 1);
        }
        ASSERT_BLOCK_EQ(except_block, actual_block);
    }
};

TEST_F(WindowFunction, testWindowFunctionByPartitionAndOrder)
try
{
    setMaxBlockSize(3);
    MockDAGRequestContext mock_context(context);

    /***** row_number with different types of input *****/
    // TODO: wrap it into a function
    MockWindowFrame frame;
    frame.type = tipb::WindowFrameType::Rows;
    frame.end = {tipb::WindowBoundType::CurrentRow, false, 0};
    frame.start = {tipb::WindowBoundType::CurrentRow, false, 0};

    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    mock_context.addMockTable({"test_db", "test_table"}, {{"partition", TiDB::TP::TypeLong}, {"order", TiDB::TP::TypeLong}});
    auto request_window_int = mock_context.scan("test_db", "test_table").window(RowNumber(), {"order", false}, {"partition", false}, frame).build(mock_context);
    auto request_sort_int = mock_context.scan("test_db", "test_table").sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true).build(mock_context);

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_int->root_executor().window(),
        request_sort_int->root_executor().sort());

    // null input
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {}), toNullableVec<Int64>("order", {})},
        {},
        request_window_int->root_executor().window(),
        request_sort_int->root_executor().sort());

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_int->root_executor().window(),
        request_sort_int->root_executor().sort());

    // string - sql : select *, row_number() over w1 from test2 window w1 as (partition by partition_string order by order_string)
    mock_context.addMockTable({"test_db", "test_table_string"}, {{"partition", TiDB::TP::TypeString}, {"order", TiDB::TP::TypeString}});
    auto request_window_string = mock_context.scan("test_db", "test_table_string").window(RowNumber(), {"order", false}, {"partition", false}, frame).build(mock_context);
    auto request_sort_string = mock_context.scan("test_db", "test_table_string").sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true).build(mock_context);
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeString>()), NameAndTypePair("order", std::make_shared<DataTypeString>())},
        {toVec<String>("partition", {"banana", "banana", "banana", "banana", "apple", "apple", "apple", "apple"}), toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"})},
        {toVec<String>("partition", {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toVec<String>("order", {"apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_string->root_executor().window(),
        request_sort_string->root_executor().sort());

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeString>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeString>()))},
        {toNullableVec<String>("partition", {"banana", "banana", "banana", "banana", {}, "apple", "apple", "apple", "apple"}), toNullableVec<String>("order", {"apple", "apple", "banana", "banana", {}, "apple", "apple", "banana", "banana"})},
        {toNullableVec<String>("partition", {{}, "apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana"}), toNullableVec<String>("order", {{}, "apple", "apple", "banana", "banana", "apple", "apple", "banana", "banana"}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_string->root_executor().window(),
        request_sort_string->root_executor().sort());

    // decimal - sql : select *, row_number() over w1 from test3 window w1 as (partition by partition_float order by order_decimal)
    mock_context.addMockTable({"test_db", "test_table_decimal"}, {{"partition", TiDB::TP::TypeDecimal}, {"order", TiDB::TP::TypeDecimal}});
    auto request_window_decimal = mock_context.scan("test_db", "test_table_decimal").window(RowNumber(), {"order", false}, {"partition", false}, frame).build(mock_context);
    auto request_sort_decimal = mock_context.scan("test_db", "test_table_decimal").sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true).build(mock_context);

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeDecimal256>()), NameAndTypePair("order", std::make_shared<DataTypeDecimal256>())},
        {toVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toVec<Float64>("partition", {1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toVec<Float64>("order", {1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_decimal->root_executor().window(),
        request_sort_decimal->root_executor().sort());

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00})},
        {toNullableVec<Float64>("partition", {{}, 1.00, 1.00, 1.00, 1.00, 2.00, 2.00, 2.00, 2.00}), toNullableVec<Float64>("order", {{}, 1.00, 1.00, 2.00, 2.00, 1.00, 1.00, 2.00, 2.00}), toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_decimal->root_executor().window(),
        request_sort_decimal->root_executor().sort());

    // datetime - select *, row_number() over w1 from test4 window w1 as (partition by partition_datetime order by order_datetime);
    mock_context.addMockTable({"test_db", "test_table_datetime"}, {{"partition", TiDB::TP::TypeDatetime}, {"order", TiDB::TP::TypeDatetime}});
    auto request_window_datetime = mock_context.scan("test_db", "test_table_datetime").window(RowNumber(), {"order", false}, {"partition", false}, frame).build(mock_context);
    auto request_sort_datetime = mock_context.scan("test_db", "test_table_datetime").sort({{"partition", false}, {"order", false}, {"partition", false}, {"order", false}}, true).build(mock_context);

    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeMyDateTime>()), NameAndTypePair("order", std::make_shared<DataTypeMyDateTime>())},
        {toDatetimeVec("partition", {"20220101010102", "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toDatetimeVec("partition", {"20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toDatetimeVec("order", {"20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_datetime->root_executor().window(),
        request_sort_datetime->root_executor().sort());

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeMyDateTime>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeMyDateTime>()))},
        {toNullableDatetimeVec("partition", {"20220101010102", {}, "20220101010102", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010101", "20220101010101"}, 0),
         toNullableDatetimeVec("order", {"20220101010101", {}, "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0)},
        {toNullableDatetimeVec("partition", {{}, "20220101010101", "20220101010101", "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010102", "20220101010102"}, 0),
         toNullableDatetimeVec("order", {{}, "20220101010101", "20220101010101", "20220101010102", "20220101010102", "20220101010101", "20220101010101", "20220101010102", "20220101010102"}, 0),
         toNullableVec<Int64>("row_number", {1, 1, 2, 3, 4, 1, 2, 3, 4})},
        request_window_datetime->root_executor().window(),
        request_sort_datetime->root_executor().sort());

    /***** rank, dense_rank *****/
    MockWindowFrame new_frame;
    mock_context.addMockTable({"test_db", "test_table1"}, {{"partition", TiDB::TP::TypeLong}, {"order", TiDB::TP::TypeLong}});
    auto request_window_rank_int = mock_context.scan("test_db", "test_table1").window({Rank(), DenseRank()}, {{"order", false}}, {{"partition", false}}, new_frame).build(mock_context);
    auto request_sort_rank_int = mock_context.scan("test_db", "test_table1").sort({{"partition", false}, {"order", false}}, true).build(mock_context);
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})},
        request_window_rank_int->root_executor().window(),
        request_sort_rank_int->root_executor().sort());

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})},
        request_window_rank_int->root_executor().window(),
        request_sort_rank_int->root_executor().sort());

    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 2, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 2, 1, 1, 2, 2, 1, 1, 2, 2})},
        request_window_rank_int->root_executor().window(),
        request_sort_rank_int->root_executor().sort());
}
CATCH

} // namespace DB::tests
