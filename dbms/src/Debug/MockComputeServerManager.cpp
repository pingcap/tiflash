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

#include <Common/FmtUtils.h>
#include <Debug/MockComputeServerManager.h>
#include <Debug/MockStorage.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <chrono>
#include <thread>
namespace DB
{
namespace ErrorCodes
{
extern const int IP_ADDRESS_NOT_ALLOWED;
} // namespace ErrorCodes
namespace tests
{
void MockComputeServerManager::addServer(const String & addr)
{
    MockServerConfig config;
    for (const auto & server : server_config_map)
    {
        RUNTIME_CHECK_MSG(server.second.addr != addr, "Already register mock compute server with addr = {}", addr);
    }
    config.partition_id = server_config_map.size();
    config.addr = addr;
    server_config_map[config.partition_id] = config;
}

void MockComputeServerManager::startServers(const LoggerPtr & log_ptr, Context & global_context)
{
    global_context.setMPPTest();
    for (const auto & server_config : server_config_map)
    {
        TiFlashRaftConfig raft_config;
        raft_config.flash_server_addr = server_config.second.addr;
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
        addServer(
            server_config.first,
            std::make_unique<FlashGrpcServerHolder>(global_context, *config, raft_config, log_ptr));
    }

    prepareMockMPPServerInfo();
}

void MockComputeServerManager::startServers(const LoggerPtr & log_ptr, int start_idx)
{
    for (const auto & server_config : server_config_map)
    {
        TiFlashRaftConfig raft_config;
        raft_config.flash_server_addr = server_config.second.addr;
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
        auto & context = TiFlashTestEnv::getGlobalContext(start_idx++);
        context.setMPPTest();
        addServer(server_config.first, std::make_unique<FlashGrpcServerHolder>(context, *config, raft_config, log_ptr));
    }

    prepareMockMPPServerInfo();
}

void MockComputeServerManager::setMockStorage(MockStorage * mock_storage)
{
    for (const auto & server : server_map)
    {
        server.second->setMockStorage(mock_storage);
    }
}

void MockComputeServerManager::reset()
{
    server_map.clear();
    server_config_map.clear();
}

MockMPPServerInfo MockComputeServerManager::getMockMPPServerInfo(size_t partition_id)
{
    return {server_config_map[partition_id].partition_id, server_config_map.size()};
}

std::unordered_map<size_t, MockServerConfig> & MockComputeServerManager::getServerConfigMap()
{
    return server_config_map;
}

void MockComputeServerManager::prepareMockMPPServerInfo()
{
    for (const auto & server : server_map)
    {
        server.second->setMockMPPServerInfo(getMockMPPServerInfo(server.first));
    }
}

void MockComputeServerManager::resetMockMPPServerInfo(size_t partition_num)
{
    size_t i = 0;
    for (const auto & server : server_map)
    {
        server.second->setMockMPPServerInfo({i++, partition_num});
    }
}

void MockComputeServerManager::addServer(size_t partition_id, std::unique_ptr<FlashGrpcServerHolder> server)
{
    server_map[partition_id] = std::move(server);
}

void MockComputeServerManager::cancelGather(const MPPGatherId & gather_id)
{
    mpp::CancelTaskRequest req;
    auto * meta = req.mutable_meta();
    meta->set_query_ts(gather_id.query_id.query_ts);
    meta->set_local_query_id(gather_id.query_id.local_query_id);
    meta->set_server_id(gather_id.query_id.server_id);
    meta->set_start_ts(gather_id.query_id.start_ts);
    meta->set_gather_id(gather_id.gather_id);
    mpp::CancelTaskResponse response;
    for (const auto & server : server_map)
        server.second->flashService()->cancelMPPTaskForTest(&req, &response);
}

String MockComputeServerManager::queryInfo()
{
    FmtBuffer buf;
    for (int i = 0; i < TiFlashTestEnv::globalContextSize(); ++i)
        buf.append(TiFlashTestEnv::getGlobalContext(i).getTMTContext().getMPPTaskManager()->toString());
    return buf.toString();
}
} // namespace tests
} // namespace DB
