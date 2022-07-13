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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/executeQuery.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/executorSerializer.h>

#include <functional>

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
    auto register_func = [](std::function<void()> func) {
        try
        {
            func();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    };

    register_func(DB::registerFunctions);
    register_func(DB::registerAggregateFunctions);
    register_func(DB::registerWindowFunctions);
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

DB::ColumnsWithTypeAndName readBlock(BlockInputStreamPtr stream)
{
    Blocks actual_blocks;
    stream->readPrefix();
    while (auto block = stream->read())
    {
        actual_blocks.push_back(block);
    }
    stream->readSuffix();
    return mergeBlocks(actual_blocks).getColumnsWithTypeAndName();
}
} // namespace

DB::ColumnsWithTypeAndName ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, std::unordered_map<String, ColumnsWithTypeAndName> & source_columns_map, size_t concurrency)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    dag_context.setColumnsForTest(source_columns_map);
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in tests.
    DAGQuerySource dag(context.context);
    return readBlock(executeQuery(dag, context.context, false, QueryProcessingStage::Complete).in);
}

DB::ColumnsWithTypeAndName ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    return executeStreams(request, context.executorIdColumnsMap(), concurrency);
}

DB::ColumnsWithTypeAndName ExecutorTest::executeStreamsWithSingleSource(const std::shared_ptr<tipb::DAGRequest> & request, const ColumnsWithTypeAndName & source_columns, SourceType type, size_t concurrency)
{
    std::unordered_map<String, ColumnsWithTypeAndName> source_columns_map;
    source_columns_map[getSourceName(type)] = source_columns;
    return executeStreams(request, source_columns_map, concurrency);
}

void ExecutorTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
