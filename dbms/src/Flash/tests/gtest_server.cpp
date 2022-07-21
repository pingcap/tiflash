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

#include <Server/MockExecutionServer.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/mockExecutor.h>
#include <thread>


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
    }
};


TEST_F(ServerRunner, runTasks)
try
{
    auto tasks = context.scan("test_db", "test_table_1")
                     .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                     .project({"max(s1)"})
                     .buildMPPTasks(context);

    size_t task_size = tasks.size();
    for (size_t i = 0; i < task_size; ++i)
    {
        std::cout << ExecutorSerializer().serialize(tasks[i].dag_request.get()) << std::endl;
    }


    MockExecutionServer app(TiFlashTestEnv::global_context, context.executorIdColumnsMap());
    std::vector<std::string> args;
    args.push_back("--no");
    auto run_server = [&] {
        app.run(args);
    };
    std::thread thd(run_server);
    thd.detach();

    // ywq todo figure out how to remove sleep_for...
    std::this_thread::sleep_for(std::chrono::seconds(5));
    executeMPPTasks(tasks);
}
CATCH
} // namespace tests
} // namespace DB
