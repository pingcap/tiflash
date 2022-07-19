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

#include <Server/MockExecutionClient.h>
#include <Server/MockExecutionServer.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/mockExecutor.h>

#include <thread>

#include "TestUtils/FunctionTestUtils.h"
#include "TestUtils/executorSerializer.h"
#include "mpp.pb.h"

namespace DB
{
namespace tests
{
class ServerRunner : public DB::tests::ExecutorTest
{
public:
    std::shared_ptr<tipb::DAGRequest> dag_request;
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        context.addMockTable({"test_db", "test_table"},
                             {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}},
                             {toNullableVec<String>("s1", {"banana", {}, "banana"}),
                              toNullableVec<String>("s2", {"apple", {}, "banana"})});

        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana"}), toNullableVec<String>("s3", {"apple", {}, "banana"})});


        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});

        // dag_request = context
        //                   .scan("test_db", "test_table")
        //                   .filter(eq(col("s1"), col("s2")))
        //                   .build(context);
    }
};


TEST_F(ServerRunner, runCoprocessor)
try
{
    DB::MockExecutionServer app(TiFlashTestEnv::global_context, context.executorIdColumnsMap());
    std::vector<std::string> args;
    args.push_back("--no");
    auto run_server = [&] {
        app.run(args);
    };
    std::thread thd(run_server);
    thd.detach();

    std::this_thread::sleep_for(std::chrono::seconds(15));

    // Ywq todo: wrap it up.
    // coprocessor::Request request;
    // String dag_string;
    // dag_request->SerializeToString(&dag_string);
    // request.set_data(dag_string);

    // coprocessor::Response response;
    MockExecutionClient client(
        grpc::CreateChannel(Debug::LOCAL_HOST, grpc::InsecureChannelCredentials()));
    // std::string reply = client.runCoprocessor(request, response);
    mpp::DispatchTaskRequest request;
    mpp::DispatchTaskResponse response;
    auto reply = client.runDispatchMPP(request, response);
    response.PrintDebugString(); // ywq todo find a way to show response...
}
CATCH


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


TEST_F(ServerRunner, runTasks)
try
{
    // // coprocessor::Response response;
    // MockExecutionClient client(
    //     grpc::CreateChannel(Debug::LOCAL_HOST, grpc::InsecureChannelCredentials()));
    // // std::string reply = client.runCoprocessor(request, response);
    // // mpp::DispatchTaskRequest request;
    // // mpp::DispatchTaskResponse response;
    // may be no need a client

    auto tasks = context.scan("test_db", "test_table_1")
                       .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                       .project({"max(s1)"}).buildMPPTasks(context);

    size_t task_size = tasks.size();
    for (size_t i = 0; i < task_size; ++i)
    {
        std::cout << ExecutorSerializer().serialize(tasks[i].dag_request.get()) << std::endl;

    }

    auto request = context.scan("test_db", "test_table_1")
                       .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                       .project({"max(s1)"});

    DB::MockExecutionServer app(TiFlashTestEnv::global_context, context.executorIdColumnsMap());
    std::vector<std::string> args;
    args.push_back("--no");
    auto run_server = [&] {
        app.run(args);
    };
    std::thread thd(run_server);
    thd.detach();

    std::this_thread::sleep_for(std::chrono::seconds(5));

    auto res = request.testExecute(context);

    auto read_res = [&] {
        std::cout << "start read block...." << std::endl;
        auto columns = readBlock(res);
        std::cout << "after read block???" << std::endl;
        std::cout << "columns size: " << columns.size() << std::endl;
        std::cout << getColumnsContent(columns) << std::endl;
    };
    // std::this_thread::sleep_for(std::chrono::seconds(5));

    std::thread read_thd(read_res);
    read_thd.detach();
    std::this_thread::sleep_for(std::chrono::seconds(50));

    // auto reply = client.runDispatchMPP(request, response);
    // response.PrintDebugString();
}
CATCH
} // namespace tests
} // namespace DB
