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

#pragma once

#include <Flash/Mpp/MPPTaskId.h>
#include <Server/FlashGrpcServerHolder.h>

namespace DB
{
class MockStorage;
namespace tests
{
/** Hold Mock Compute Server to manage the lifetime of them.
  * Maintains Mock Compute Server info.
  */
class MockComputeServerManager : public ext::Singleton<MockComputeServerManager>
{
public:
    /// register an server to run.
    void addServer(const String & addr);

    /// call startServers to run all servers in current test.
    void startServers(const LoggerPtr & log_ptr, Context & global_context);

    void startServers(const LoggerPtr & log_ptr, int start_idx);

    /// set MockStorage for Compute Server in order to mock input columns.
    void setMockStorage(MockStorage * mock_storage);

    /// stop all servers.
    void reset();

    MockMPPServerInfo getMockMPPServerInfo(size_t partition_id);

    std::unordered_map<size_t, MockServerConfig> & getServerConfigMap();

    void resetMockMPPServerInfo(size_t partition_num);

    void cancelGather(const MPPGatherId & gather_id);

    static String queryInfo();

private:
    void addServer(size_t partition_id, std::unique_ptr<FlashGrpcServerHolder> server);
    void prepareMockMPPServerInfo();

private:
    std::unordered_map<size_t, std::unique_ptr<FlashGrpcServerHolder>> server_map;
    std::unordered_map<size_t, MockServerConfig> server_config_map;
};
} // namespace tests
} // namespace DB
