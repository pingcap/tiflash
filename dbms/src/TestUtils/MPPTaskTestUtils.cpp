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
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <TestUtils/MPPTaskTestUtils.h>
#include <fmt/core.h>

namespace DB::tests
{
DAGProperties getDAGPropertiesForTest(
    int server_num,
    int local_query_id,
    int tidb_server_id,
    int query_ts,
    int gather_id)
{
    DAGProperties properties;
    // enable mpp
    properties.is_mpp_query = true;
    properties.mpp_partition_num = server_num;
    properties.start_ts = MockTimeStampGenerator::instance().nextTs();
    if (query_ts > 0)
        properties.query_ts = query_ts;
    if (local_query_id >= 0)
        properties.local_query_id = local_query_id;
    else
        properties.local_query_id = properties.start_ts;
    if (tidb_server_id >= 0)
        properties.server_id = tidb_server_id;
    if (gather_id > 0)
        properties.gather_id = gather_id;
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
    MockComputeServerManager::instance().setMockStorage(context.mockStorage());
    MockServerAddrGenerator::instance().reset();
}

size_t MPPTaskTestUtils::serverNum()
{
    return server_num;
}

void MPPTaskTestUtils::setCancelTest()
{
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
        TiFlashTestEnv::getGlobalContext(i).setCancelTest();
}

BlockInputStreamPtr MPPTaskTestUtils::prepareMPPStreams(DAGRequestBuilder builder, const DAGProperties & properties)
{
    auto tasks = prepareMPPTasks(builder, properties);
    return executeMPPQueryWithMultipleContext(
        properties,
        tasks,
        MockComputeServerManager::instance().getServerConfigMap());
}

std::vector<QueryTask> MPPTaskTestUtils::prepareMPPTasks(DAGRequestBuilder builder, const DAGProperties & properties)
{
    std::lock_guard lock(mu);
    auto tasks = builder.buildMPPTasks(context, properties);
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
        TiFlashTestEnv::getGlobalContext(i).setCancelTest();
    return tasks;
}

std::vector<QueryTask> MPPTaskTestUtils::prepareMPPTasks(
    std::function<DAGRequestBuilder()> & gen_builder,
    const DAGProperties & properties)
{
    std::lock_guard lock(mu);
    auto tasks = gen_builder().buildMPPTasks(context, properties);
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
        TiFlashTestEnv::getGlobalContext(i).setCancelTest();
    return tasks;
}

ColumnsWithTypeAndName MPPTaskTestUtils::executeProblematicMPPTasks(
    QueryTasks & tasks,
    const DAGProperties & properties,
    BlockInputStreamPtr & stream)
{
    stream = executeMPPQueryWithMultipleContext(
        properties,
        tasks,
        MockComputeServerManager::instance().getServerConfigMap());
    return readBlock(stream);
}

ColumnsWithTypeAndName MPPTaskTestUtils::executeMPPTasks(QueryTasks & tasks, const DAGProperties & properties)
{
    auto res = executeMPPQueryWithMultipleContext(
        properties,
        tasks,
        MockComputeServerManager::instance().getServerConfigMap());
    return readBlock(res);
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

    DAGContext dag_context(*dag_request, {}, NullspaceID, "", DAGRequestKind::Cop, "", 0, "", Logger::get());

    TiFlashTestEnv::getGlobalContext(test_meta.context_idx).setDAGContext(&dag_context);
    TiFlashTestEnv::getGlobalContext(test_meta.context_idx).setCopTest();

    MockComputeServerManager::instance().setMockStorage(context.mockStorage());

    auto addr = MockComputeServerManager::instance()
                    .getServerConfigMap()[0]
                    .addr; // Since we only have started 1 server currently.
    MockComputeClient client(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
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
    buf.fmtAppend(
        fmt::runtime(TiFlashTestEnv::getGlobalContext(server_id).getTMTContext().getMPPTaskManager()->toString()));
    return buf.toString();
}

::testing::AssertionResult MPPTaskTestUtils::assertQueryCancelled(const MPPQueryId & query_id)
{
    auto seconds = std::chrono::seconds(1);
    auto retry_times = 0;
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        // wait until the task is empty for <query:start_ts>
        while (TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getMPPQuery(query_id)
               != nullptr)
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

::testing::AssertionResult MPPTaskTestUtils::assertGatherCancelled(const MPPGatherId & gather_id)
{
    auto seconds = std::chrono::seconds(1);
    auto retry_times = 0;
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        // wait until the task is empty for <query:start_ts>
        while (
            TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getGatherTaskSet(gather_id).first
            != nullptr)
        {
            std::this_thread::sleep_for(seconds);
            ++retry_times;
            // Currenly we wait for 20 times to ensure all tasks are cancelled.
            if (retry_times > 20)
            {
                return ::testing::AssertionFailure() << "Gather not cancelled, " << queryInfo(i) << std::endl;
            }
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult MPPTaskTestUtils::assertQueryActive(const MPPQueryId & query_id)
{
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        if (TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getMPPQuery(query_id) == nullptr)
        {
            return ::testing::AssertionFailure() << "Query " << query_id.toString() << "not active" << std::endl;
        }
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult MPPTaskTestUtils::assertGatherActive(const MPPGatherId & gather_id)
{
    for (int i = test_meta.context_idx; i < TiFlashTestEnv::globalContextSize(); ++i)
    {
        if (TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->getGatherTaskSet(gather_id).first
            == nullptr)
        {
            return ::testing::AssertionFailure() << "Gather " << gather_id.toString() << "not active" << std::endl;
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
    return executeMPPTasks(tasks, properties);
}
} // namespace DB::tests
