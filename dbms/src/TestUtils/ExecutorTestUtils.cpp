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
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/executorSerializer.h>
namespace DB::tests
{
DAGContext & ExecutorTest::getDAGContext()
{
    assert(dag_context_ptr != nullptr);
    return *dag_context_ptr;
}

void ExecutorTest::initializeContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context = MockDAGRequestContext(TiFlashTestEnv::getContext());
    dag_context_ptr->log = Logger::get("executorTest");
}

void ExecutorTest::SetUpTestCase()
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

void ExecutorTest::initializeClientInfo()
{
    context.context.setCurrentQueryId("test");
    ClientInfo & client_info = context.context.getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;
}

void ExecutorTest::executeInterpreter(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
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

namespace
{
Block mergeBlocks(Blocks blocks)
{
    if (blocks.empty())
        return {};

    Block sample_block = blocks.back();
    std::vector<MutableColumnPtr> actual_cols;
    for (const auto & column : sample_block.getColumnsWithTypeAndName())
    {
        actual_cols.push_back(column.type->createColumn());
    }
    for (const auto & block : blocks)
    {
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

void readBlock(BlockInputStreamPtr stream, const ColumnsWithTypeAndName & expect_columns)
{
    Blocks actual_blocks;
    Block except_block(expect_columns);
    stream->readPrefix();
    while (auto block = stream->read())
    {
        actual_blocks.push_back(block);
    }
    stream->readSuffix();
    Block actual_block = mergeBlocks(actual_blocks);
    ASSERT_BLOCK_EQ(except_block, actual_block);
}
} // namespace

void ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, std::unordered_map<String, ColumnsWithTypeAndName> & source_columns_map, const ColumnsWithTypeAndName & expect_columns, size_t concurrency)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    dag_context.setColumnsForTest(source_columns_map);
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in tests.
    DAGQuerySource dag(context.context);
    readBlock(executeQuery(dag, context.context, false, QueryProcessingStage::Complete).in, expect_columns);
}

void ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & expect_columns, size_t concurrency)
{
    executeStreams(request, context.executorIdColumnsMap(), expect_columns, concurrency);
}

void ExecutorTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
