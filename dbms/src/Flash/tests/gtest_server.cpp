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

        dag_request = context
                          .scan("test_db", "test_table")
                          .filter(eq(col("s1"), col("s2")))
                          .build(context);
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
    response.PrintDebugString();
}
CATCH
} // namespace tests
} // namespace DB
