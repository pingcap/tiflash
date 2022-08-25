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
#include <TestUtils/MPPTaskTestUtils.h>

namespace DB::tests
{
DAGProperties getDAGPropertiesForTest(int server_num)
{
    DAGProperties properties;
    // enable mpp
    properties.is_mpp_query = true;
    properties.mpp_partition_num = server_num;
    properties.start_ts = MockTimeStampGenerator::instance().nextTs();
    return properties;
}

void MPPTaskTestUtils::SetUpTestCase()
{
    ExecutorTest::SetUpTestCase();
    log_ptr = Logger::get("compute_test");
    server_num = 1;
}

void MPPTaskTestUtils::TearDownTestCase() // NOLINT(readability-identifier-naming))
{
    MockComputeServerManager::instance().reset();
}

void MPPTaskTestUtils::startServers()
{
    startServers(server_num);
}

void MPPTaskTestUtils::startServers(size_t server_num_)
{
    server_num = server_num_;
    test_meta.server_num = server_num_;
    test_meta.context_idx = TiFlashTestEnv::globalContextSize();
    MockComputeServerManager::instance().reset();
    auto size = std::thread::hardware_concurrency();
    GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
    std::cout << "ywq test globalContext size: " << TiFlashTestEnv::globalContextSize() << std::endl;
    for (size_t i = 0; i < test_meta.server_num; ++i)
    {
        MockComputeServerManager::instance().addServer(MockServerAddrGenerator::instance().nextAddr());
        TiFlashTestEnv::addGlobalContext();
        TiFlashTestEnv::getGlobalContext(i + test_meta.context_idx).setMPPTest();
    }

    MockComputeServerManager::instance().startServers(log_ptr, test_meta.context_idx);
    std::cout << "ywq test globalContext size: " << TiFlashTestEnv::globalContextSize() << std::endl;
    MockServerAddrGenerator::instance().reset();
}

size_t MPPTaskTestUtils::serverNum()
{
    return server_num;
}

void MPPTaskTestUtils::injectCancel(DAGRequestBuilder builder)
{
    auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
    auto tasks = builder.buildMPPTasks(context, properties);
    for (int i = 0; i < TiFlashTestEnv::globalContextSize(); i++)
        TiFlashTestEnv::getGlobalContext(i).setMPPTest();
    MockComputeServerManager::instance().setMockStorage(context.mockStorage());
    executeMPPTasksForCancel(tasks, properties, MockComputeServerManager::instance().getServerConfigMap());
    // ywq todo wait time so long, change it.
}

DB::ColumnsWithTypeAndName MPPTaskTestUtils::executeMPPTasks(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    auto res = executeMPPQuery(TiFlashTestEnv::getGlobalContext(test_meta.context_idx), properties, tasks, server_config_map);
    return readBlock(res);
}

BlockInputStreamPtr MPPTaskTestUtils::executeMPPTasksForCancel(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    auto res = executeMPPQuery(TiFlashTestEnv::getGlobalContext(test_meta.context_idx), properties, tasks, server_config_map);
    return res;
}
} // namespace DB::tests