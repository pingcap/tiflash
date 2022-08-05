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

#pragma once

#include <Server/FlashGrpcServerHolder.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{

struct MockServerConfig
{
    String name;
    String addr;
};
class MockComputeServerManager
{
public:
    void addServerConfig(MockServerConfig config)
    {
        server_config_map[config.name] = config;
    }

    void startAllServer(const LoggerPtr & log_ptr)
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

    void setMockStorage(MockStorage & mock_storage)
    {
        for (const auto & kv : server_map)
        {
            kv.second->setMockStorage(mock_storage);
        }
    }

    void reset()
    {
        server_map.clear();
    }

private:
    void addServer(String name, std::unique_ptr<FlashGrpcServerHolder> server)
    {
        server_map[name] = std::move(server);
    }

private:
    std::unordered_map<String, std::unique_ptr<FlashGrpcServerHolder>> server_map;
    std::unordered_map<String, MockServerConfig> server_config_map;
};
} // namespace DB::tests