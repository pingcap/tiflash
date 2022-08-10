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
#include <Debug/MockComputeServerManager.h>

namespace DB::tests
{
void MockComputeServerManager::addServer(String addr)
{
    MockServerConfig config;
    config.partition_id = server_config_map.size();
    config.addr = addr;
    server_config_map[config.partition_id] = config;
}

void MockComputeServerManager::startServers(const LoggerPtr & log_ptr, Context & global_context)
{
    for (const auto & server_config : server_config_map)
    {
        TiFlashSecurityConfig security_config;
        TiFlashRaftConfig raft_config;
        raft_config.flash_server_addr = server_config.second.addr;
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
        addServer(server_config.first, std::make_unique<FlashGrpcServerHolder>(global_context, *config, security_config, raft_config, log_ptr));
    }

    prepareMockMPPServerInfo();
}

void MockComputeServerManager::setMockStorage(MockStorage & mock_storage)
{
    for (const auto & server : server_map)
    {
        server.second->setMockStorage(mock_storage);
    }
}

void MockComputeServerManager::reset()
{
    server_map.clear();
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

void MockComputeServerManager::addServer(size_t partition_id, std::unique_ptr<FlashGrpcServerHolder> server)
{
    server_map[partition_id] = std::move(server);
}

} // namespace DB::tests