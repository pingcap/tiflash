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
#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Debug/dbgQueryExecutor.h>
#include <Server/FlashGrpcServerHolder.h>
#include <Server/MockComputeClient.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <TestUtils/MPPTaskTestUtils.h>
#include <fmt/core.h>

namespace DB::tests
{
DAGProperties getDAGPropertiesForTest(int server_num)
{
    DAGProperties properties;
    // enable mpp
    properties.is_mpp_query = true;
    properties.mpp_partition_num = server_num;
    properties.start_ts = MockTimeStampGenerator::instance().nextTs();
    properties.local_query_id = properties.start_ts;
    return properties;
}

void MPPTaskTestUtils::SetUpTestCase()
{
    ExecutorTest::SetUpTestCase();
    log_ptr = Logger::get();
    server_num = 1;
}

void MPPTaskTestUtils::TearDown()
{
    MockComputeServerManager::instance().reset();
    ExecutorTest::TearDown();
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
    for (size_t i = 0; i < test_meta.server_num; ++i)
    {
        MockComputeServerManager::instance().addServer(MockServerAddrGenerator::instance().nextAddr());
        // Currently, we simply add a context and don't care about destruct it.
        TiFlashTestEnv::addGlobalContext(context.context->getSettings());
        TiFlashTestEnv::getGlobalContext(i + test_meta.context_idx).setMPPTest();
    }

    MockComputeServerManager::instance().startServers(log_ptr, test_meta.context_idx);
    MockServerAddrGenerator::instance().reset();
}

size_t MPPTaskTestUtils::serverNum()
{
    return server_num;
}

std::tuple<MPPQueryId, std::vector<BlockInputStreamPtr>> MPPTaskTestUtils::prepareMPPStreams(DAGRequestBuilder builder)
{
    auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
    auto tasks = builder.buildMPPTasks(context, properties);
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
        TiFlashTestEnv::getGlobalContext(i).setCancelTest();
    MockComputeServerManager::instance().setMockStorage(context.mockStorage());
    auto res = executeMPPQueryWithMultipleContext(properties, tasks, MockComputeServerManager::instance().getServerConfigMap());
    return {MPPQueryId(properties.query_ts, properties.local_query_id, properties.server_id, properties.start_ts), res};
}

ColumnsWithTypeAndName MPPTaskTestUtils::executeMPPTasks(QueryTasks & tasks, const DAGProperties & properties, std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    auto res = executeMPPQueryWithMultipleContext(properties, tasks, server_config_map);
    return readBlocks(res);
}

ColumnsWithTypeAndName extractColumns(Context & context, const std::shared_ptr<tipb::SelectResponse> & dag_response)
{
    auto codec = getCodec(dag_response->encode_type());
    Blocks blocks;
    auto schema = getSelectSchema(context);
    for (const auto & chunk : dag_response->chunks())
        blocks.emplace_back(codec->decode(chunk.rows_data(), schema));
    return vstackBlocks(std::move(blocks)).getColumnsWithTypeAndName();
}

ColumnsWithTypeAndName MPPTaskTestUtils::executeCoprocessorTask(std::shared_ptr<tipb::DAGRequest> & dag_request)
{
    assert(server_num == 1);
    auto req = std::make_shared<coprocessor::Request>();
    req->set_tp(103); // 103 is COP_REQ_TYPE_DAG
    auto * data = req->mutable_data();
    dag_request->AppendToString(data);

    DAGContext dag_context(*dag_request, {}, NullspaceID, "", false, Logger::get());

    TiFlashTestEnv::getGlobalContext(test_meta.context_idx).setDAGContext(&dag_context);
    TiFlashTestEnv::getGlobalContext(test_meta.context_idx).setCopTest();

    MockComputeServerManager::instance().setMockStorage(context.mockStorage());

    auto addr = MockComputeServerManager::instance().getServerConfigMap()[0].addr; // Since we only have started 1 server currently.
    MockComputeClient client(
        grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    auto resp = client.runCoprocessor(req);
    auto resp_ptr = std::make_shared<tipb::SelectResponse>();
    if (unlikely(!resp_ptr->ParseFromString(resp.data())))
    {
        throw Exception("Incorrect json response data from Coprocessor.", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        return extractColumns(TiFlashTestEnv::getGlobalContext(test_meta.context_idx), resp_ptr);
    }
}

String MPPTaskTestUtils::queryInfo(size_t server_id)
{
    FmtBuffer buf;
    buf.fmtAppend("server id: {}, tasks: ", server_id);
    buf.fmtAppend(fmt::runtime(TiFlashTestEnv::getGlobalContext(server_id).getTMTContext().getMPPTaskManager()->toString()));
    return buf.toString();
}

::testing::AssertionResult MPPTaskTestUtils::assertQueryCancelled(const MPPQueryId & query_id)
{
    auto seconds = std::chrono::seconds(1);
    auto retry_times = 0;
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        // wait until the task is empty for <query:start_ts>
        while (TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getQueryTaskSet(query_id) != nullptr)
        {
            std::this_thread::sleep_for(seconds);
            retry_times++;
            // Currenly we wait for 20 times to ensure all tasks are cancelled.
            if (retry_times > 20)
            {
                return ::testing::AssertionFailure() << "Query not cancelled, " << queryInfo(i) << std::endl;
            }
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult MPPTaskTestUtils::assertQueryActive(const MPPQueryId & query_id)
{
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        if (TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getQueryTaskSet(query_id) == nullptr)
        {
            return ::testing::AssertionFailure() << "Query " << query_id.toString() << "not active" << std::endl;
        }
    }
    return ::testing::AssertionSuccess();
}

ColumnsWithTypeAndName MPPTaskTestUtils::buildAndExecuteMPPTasks(DAGRequestBuilder builder)
{
    auto properties = DB::tests::getDAGPropertiesForTest(serverNum());
    for (int i = 0; i < TiFlashTestEnv::globalContextSize(); ++i)
        TiFlashTestEnv::getGlobalContext(i).setMPPTest();
    auto tasks = (builder).buildMPPTasks(context, properties);
    MockComputeServerManager::instance().resetMockMPPServerInfo(serverNum());
    MockComputeServerManager::instance().setMockStorage(context.mockStorage());
    return executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap());
}
} // namespace DB::tests
