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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/InterpreterTestUtils.h>
#include <TestUtils/executorSerializer.h>
namespace DB::tests
{
DAGContext & InterpreterTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void InterpreterTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
    dag_context_ptr->log = Logger::get("interpreterTest");
}

void InterpreterTest::SetUpTestCase()
{
    try
    {
        DB::registerFunctions();
        DB::registerAggregateFunctions();
    }
    catch (DB::Exception &)
    {
        // Maybe another test has already registered, ignore exception here.
    }
}

void InterpreterTest::initializeClientInfo()
{
    context.context.setCurrentQueryId("test");
    ClientInfo & client_info = context.context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
}

void InterpreterTest::executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "interpreter_test", concurrency);
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in interpreter tests.
    DAGQuerySource dag(context.context);
    auto res = executeQuery(dag, context.context, false, QueryProcessingStage::Complete);
    FmtBuffer fb;
    res.in->dumpTree(fb);
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(fb.toString()));
}

static Block mergeBlocks(Blocks blocks)
{
    if (blocks.empty())
        return {};

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
        actual_columns.push_back({std::move(actual_cols[i]), sample_block.getColumnsWithTypeAndName()[i].type, sample_block.getColumnsWithTypeAndName()[i].name, sample_block.getColumnsWithTypeAndName()[i].column_id});
    return Block(actual_columns);
}

void InterpreterTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, std::unordered_map<String, ColumnsWithTypeAndName> & source_columns_map, const ColumnsWithTypeAndName & expect_columns)
{
    DAGContext dag_context(*request, "interpreter_test", 1);
    dag_context.setColumnsForTest(source_columns_map);
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in tests.
    DAGQuerySource dag(context.context);
    auto res = executeQuery(dag, context.context, false, QueryProcessingStage::Complete);
    auto stream = res.in;
    Blocks actual_blocks;
    Block except_block(expect_columns);
    while (Block block = stream->read())
    {
        actual_blocks.push_back(block);
    }

    Block actual_block = mergeBlocks(actual_blocks);
    if (actual_block)
    {
        // Check that input columns is properly split to many blocks
        ASSERT_EQ(actual_blocks.size(), (actual_block.rows() - 1) / context.context.getSettingsRef().max_block_size + 1);
    }
    ASSERT_BLOCK_EQ(except_block, actual_block);
}

void InterpreterTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns)
{
    executeStreams(request, context.executorIdColumnsMap(), expect_columns);
}

void InterpreterTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
