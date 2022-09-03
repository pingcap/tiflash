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
#include <Debug/MockComputeServerManager.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/executeQuery.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/executorSerializer.h>

#include <functional>

namespace DB::tests
{
TiDB::TP dataTypeToTP(const DataTypePtr & type)
{
    // TODO support more types.
    switch (removeNullable(type)->getTypeId())
    {
    case TypeIndex::UInt8:
    case TypeIndex::Int8:
        return TiDB::TP::TypeTiny;
    case TypeIndex::UInt16:
    case TypeIndex::Int16:
        return TiDB::TP::TypeShort;
    case TypeIndex::UInt32:
    case TypeIndex::Int32:
        return TiDB::TP::TypeLong;
    case TypeIndex::UInt64:
    case TypeIndex::Int64:
        return TiDB::TP::TypeLongLong;
    case TypeIndex::String:
        return TiDB::TP::TypeString;
    case TypeIndex::Float32:
        return TiDB::TP::TypeFloat;
    case TypeIndex::Float64:
        return TiDB::TP::TypeDouble;
    default:
        throw Exception("Unsupport type");
    }
}

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
    TiFlashTestEnv::getGlobalContext().setExecutorTest();
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
    context.context.setExecutorTest();
    // Currently, don't care about regions information in interpreter tests.
    auto res = executeQuery(context.context);
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
} // namespace

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

void ExecutorTest::enablePlanner(bool is_enable)
{
    context.context.setSetting("enable_planner", is_enable ? "true" : "false");
}

DB::ColumnsWithTypeAndName ExecutorTest::executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency)
{
    DAGContext dag_context(*request, "executor_test", concurrency);
    context.context.setExecutorTest();
    context.context.setMockStorage(context.mockStorage());
    context.context.setDAGContext(&dag_context);
    // Currently, don't care about regions information in tests.
    return readBlock(executeQuery(context.context).in);
}

DB::ColumnsWithTypeAndName ExecutorTest::executeMPPTasks(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    auto res = executeMPPQuery(context.context, properties, tasks, server_config_map);
    return readBlock(res);
}

void ExecutorTest::dagRequestEqual(const String & expected_string, const std::shared_ptr<tipb::DAGRequest> & actual)
{
    ASSERT_EQ(Poco::trim(expected_string), Poco::trim(ExecutorSerializer().serialize(actual.get())));
}

} // namespace DB::tests
