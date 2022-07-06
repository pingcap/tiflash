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
#include <TestUtils/mockExecutor.h>

#include <filesystem>
#include <iostream>

#include "Debug/astToExecutor.h"
#include "Interpreters/Context.h"
#include "TestUtils/TiFlashTestEnv.h"
#include "gtest/gtest.h"

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

        dag_request = context
                                                            .scan("test_db", "test_table")
                                                            .filter(eq(col("s1"), col("s2")))
                                                            .build(context);
    }
};


TEST_F(ServerRunner, runServer)
try
{
    std::cout << "ywq test columns map" << std::endl;
    for (auto kv : context.executorIdColumnsMap()) {
        std::cout << kv.first << std::endl;
    }
    // dag_context_ptr->setColumnsForTest(context.executorIdColumnsMap()); // ywq todo make it clear..
    // TiFlashTestEnv::global_context->setDAGContext(dag_context_ptr.get());
    DB::MockExecutionServer app(TiFlashTestEnv::global_context, context.executorIdColumnsMap());

    std::vector<std::string> args;
    args.push_back("--deamon");
    try
    {
        std::cout << "Current path is " << std::filesystem::current_path() << '\n';
        app.run(args);
        // ywq todo figure out how to run a deamon server....
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        std::cerr << code << "\n";
    }
}
CATCH


TEST_F(ServerRunner, runDispatchMPPTask)
try
{
    std::cout << "ywq test reach client run dispatchmpptask..." << std::endl;

    MockExecutionClient client(
        grpc::CreateChannel(Debug::LOCAL_HOST, grpc::InsecureChannelCredentials()));
    std::string reply = client.runDispatchMPPTask();
    std::cout << "runDispatchMPPTask received: " << reply << std::endl;
}
CATCH

TEST_F(ServerRunner, runCoprocessor)
try
{
    std::cout << "ywq test reach client run coprocessor..." << std::endl;
    coprocessor::Request request;
    request.set_tp(103);
    request.set_start_ts(1);
    String dag_string;
    dag_request->SerializeToString(&dag_string);
    request.set_data(dag_string);
    std::cout << "dag string: " << dag_string << std::endl;
    coprocessor::Response response;

    MockExecutionClient client(
        grpc::CreateChannel(Debug::LOCAL_HOST, grpc::InsecureChannelCredentials()));
    std::string reply = client.runCoprocessor(request, response);
    std::cout << "coprocessor received: " << reply << std::endl;
}
CATCH

} // namespace tests
} // namespace DB
