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

#include <Common/MyTime.h>
#include <Core/Block.h>
#include <DataStreams/MockTableScanBlockInputStream.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <TestUtils/FunctionTestUtils.h>
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

    void mockExecuteWindowOrder(DAGPipeline & pipeline, std::string sort_json_str)
    {
        tipb::Sort sort;
        google::protobuf::util::JsonStringToMessage(sort_json_str, &sort);
        mock_interpreter->handleWindowOrder(pipeline, sort);
        mock_interpreter->input_streams_vec[0] = pipeline.streams;
        NamesWithAliases final_project;
        for (const auto & column : (*mock_interpreter->analyzer).source_columns)
        {
            final_project.push_back({column.name, ""});
        }
        mockExecuteProject(pipeline, final_project);
    }

    void mockExecuteWindow(DAGPipeline & pipeline, std::string window_json_str)
    {
        tipb::Window window;
        google::protobuf::util::JsonStringToMessage(window_json_str, &window);
        mock_interpreter->handleWindow(pipeline, window);
        mock_interpreter->input_streams_vec[0] = pipeline.streams;
        NamesWithAliases final_project;
        for (const auto & column : (*mock_interpreter->analyzer).source_columns)
        {
            final_project.push_back({column.name, ""});
        }
        mockExecuteProject(pipeline, final_project);
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

    void testOneWindowFunction(const std::vector<NameAndTypePair> & source_column_types, const ColumnsWithTypeAndName & source_columns, const ColumnsWithTypeAndName & expect_columns, const std::string window_json_str, const std::string sort_json_str)
    {
        mockInterpreter(source_column_types, context);
        DAGPipeline pipeline;
        ExpressionActionsChain chain;
        Block except_block(expect_columns);

        mockExecuteTableScan(pipeline, source_columns);

        mockExecuteWindowOrder(pipeline, sort_json_str);

        mockExecuteWindow(pipeline, window_json_str);

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

    std::string window_json;
    std::string sort_json;

    /***** row_number with different types of input *****/
    // int - sql : select *, row_number() over w1 from test1 window w1 as (partition by partition_int order by order_int)
    window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAkMCV6NP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}})";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 2, 3, 4, 1, 2, 3, 4})},
        window_json,
        sort_json);

    // null input
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {}), toNullableVec<Int64>("order", {})},
        {},
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
    window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGA8Nz57tP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"},{"tp":254,"flag":0,"flen":32,"decimal":0,"collate":46,"charset":"utf8mb4"}]},"executorId":"ExchangeReceiver_11"}})";
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
    window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAoN3M99P+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"},{"tp":246,"flag":0,"flen":6,"decimal":2,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}})";
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
    window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAsNmBhdT+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"},{"tp":12,"flag":128,"flen":26,"decimal":6,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}})";
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
    // sql : select *, row_number() over w1 from test6 window w1 as (partition by partition_int1, partition_int2 order by order_int1,order_int2)
    window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIKA0Img1If/BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAM=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIKA0Img1If/BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}})";
    testOneWindowFunction(
        {NameAndTypePair("partition1", std::make_shared<DataTypeInt64>()), NameAndTypePair("partition2", std::make_shared<DataTypeInt64>()), NameAndTypePair("order1", std::make_shared<DataTypeInt64>()), NameAndTypePair("order2", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}), toVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}), toVec<Int64>("order1", {2, 1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1}), toVec<Int64>("order2", {2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2, 1})},
        {toVec<Int64>("partition1", {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2}), toVec<Int64>("partition2", {1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2}), toVec<Int64>("order1", {1, 1, 2, 1, 1, 2, 1, 1, 2, 1, 1, 2}), toVec<Int64>("order2", {1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2, 2}), toNullableVec<Int64>("row_number", {1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})},
        window_json,
        sort_json);

    /***** rank, dense_rank *****/
    window_json = R"({"funcDesc":[{"tp":"Rank","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false},{"tp":"DenseRank","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"child":{"tp":"TypeSort","executorId":"Sort_12","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}}}})";
    sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAsOnl3NP+BRABIg4xMjcuMC4wLjE6MzkzMA=="],"fieldTypes":[{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"},{"tp":3,"flag":0,"flen":11,"decimal":0,"collate":63,"charset":"binary"}]},"executorId":"ExchangeReceiver_11"}})";
    testOneWindowFunction(
        {NameAndTypePair("partition", std::make_shared<DataTypeInt64>()), NameAndTypePair("order", std::make_shared<DataTypeInt64>())},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2})},
        {toVec<Int64>("partition", {1, 1, 1, 1, 2, 2, 2, 2}), toVec<Int64>("order", {1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);

    // nullable
    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);

    testOneWindowFunction(
        {NameAndTypePair("partition", makeNullable(std::make_shared<DataTypeInt64>())), NameAndTypePair("order", makeNullable(std::make_shared<DataTypeInt64>()))},
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2})},
        {toNullableVec<Int64>("partition", {{}, {}, 1, 1, 1, 1, 2, 2, 2, 2}), toNullableVec<Int64>("order", {{}, 1, 1, 1, 2, 2, 1, 1, 2, 2}), toNullableVec<Int64>("rank", {1, 2, 1, 1, 3, 3, 1, 1, 3, 3}), toNullableVec<Int64>("dense_rank", {1, 2, 1, 1, 2, 2, 1, 1, 2, 2})},
        window_json,
        sort_json);
}
CATCH
} // namespace DB::tests
