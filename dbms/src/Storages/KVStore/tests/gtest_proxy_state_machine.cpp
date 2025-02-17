// Copyright 2025 PingCAP, Inc.
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

#include <Storages/KVStore/ProxyStateMachine.h>
#include <Interpreters/Settings.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <Common/FailPoint.h>

// TODO: Move ServerInfo into KVStore, to make it more conhensive.
namespace DB
{
namespace FailPoints
{
extern const char force_set_proxy_state_machine_cpu_cores[];
} // namespace FailPoints

namespace tests {
TEST(ProxyStateMachineTest, SetLogicalCores)
{
    {
        FailPointHelper::enableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores);
        Settings settings;
        ServerInfo server_info;
        ProxyStateMachine proxy_machine{DB::Logger::get(), TiFlashProxyConfig::genForTest()};
        proxy_machine.getServerInfo(server_info, settings);
        FailPointHelper::disableFailPoint(FailPoints::force_set_proxy_state_machine_cpu_cores);
        ASSERT_EQ(settings.max_threads.get(), 12345);
    }
    {
        Settings settings;
        ServerInfo server_info;
        ProxyStateMachine proxy_machine{DB::Logger::get(), TiFlashProxyConfig::genForTest()};
        proxy_machine.getServerInfo(server_info, settings);
        ASSERT_EQ(settings.max_threads.get(), std::thread::hardware_concurrency());
    }
}
}
}
