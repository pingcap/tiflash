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
#include <TestUtils/MockServerInfo.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <unordered_map>

namespace DB::tests
{
class MockComputeServerManager : public ext::Singleton<MockComputeServerManager>
{
public:
    void addServer(String addr);

    void startServers(const LoggerPtr & log_ptr);

    void setMockStorage(MockStorage & mock_storage);

    void reset();

    MPPTestInfo getMPPTestInfo(size_t partition_id);

    std::unordered_map<size_t, MockServerConfig> & getServerConfigMap();

private:
    void addServer(size_t partition_id, std::unique_ptr<FlashGrpcServerHolder> server);
    void prepareMPPTestInfo();

private:
    std::unordered_map<size_t, std::unique_ptr<FlashGrpcServerHolder>> server_map;
    std::unordered_map<size_t, MockServerConfig> server_config_map;
};
} // namespace DB::tests