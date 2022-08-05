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
#include <TestUtils/MockComputeServerManager.h>

namespace DB::tests
{
void MockComputeServerManager::addServerConfig(MockServerConfig config)
{
    config.partition_id = server_config_map.size();
    server_config_map[config.name] = config;
}

void MockComputeServerManager::startAllServer(const LoggerPtr & log_ptr)
{
    for (const auto & kv : server_config_map)
    {
        TiFlashSecurityConfig security_config;
        TiFlashRaftConfig raft_config;
        raft_config.flash_server_addr = kv.second.addr;
        Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration;
        addServer(kv.first, std::make_unique<FlashGrpcServerHolder>(TiFlashTestEnv::getGlobalContext(), *config, security_config, raft_config, log_ptr));
    }
}

void MockComputeServerManager::setMockStorage(MockStorage & mock_storage)
{
    for (const auto & kv : server_map)
    {
        kv.second->setMockStorage(mock_storage);
    }
}

void MockComputeServerManager::reset()
{
    server_map.clear();
}

MPPTestInfo MockComputeServerManager::getMPPTestInfo(String name)
{
    return {server_config_map[name].partition_id, server_config_map.size()};
}

void MockComputeServerManager::setMPPTestInfo()
{
    for (const auto & kv : server_map)
    {
        kv.second->setMPPTestInfo(getMPPTestInfo(kv.first));
    }
}

void MockComputeServerManager::addServer(String name, std::unique_ptr<FlashGrpcServerHolder> server)
{
    server_map[name] = std::move(server);
}

} // namespace DB::tests