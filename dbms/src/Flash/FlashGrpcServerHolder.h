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

#include <Common/BackgroundTask.h>
#include <Common/ThreadManager.h>
#include <Common/assert_cast.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Server/RaftConfigParser.h>
#include <Storages/KVStore/TiKVHelpers/PDTiKVClient.h>
#include <grpc/grpc_security.h>


namespace DB
{
using MockMPPServerInfo = tests::MockMPPServerInfo;

class FlashGrpcServerHolder
{
public:
    FlashGrpcServerHolder(
        Context & context,
        Poco::Util::LayeredConfiguration & config_,
        const TiFlashRaftConfig & raft_config,
        const LoggerPtr & log_);
    ~FlashGrpcServerHolder();

    void setMockStorage(MockStorage * mock_storage);
    void setMockMPPServerInfo(MockMPPServerInfo info);

    std::unique_ptr<FlashService> & flashService();

private:
    const LoggerPtr & log;
    std::shared_ptr<std::atomic<bool>> is_shutdown;
    std::unique_ptr<FlashService> flash_service = nullptr;
    std::unique_ptr<DiagnosticsService> diagnostics_service = nullptr;
    std::unique_ptr<grpc::Server> flash_grpc_server = nullptr;
    // cqs and notify_cqs are used for processing async grpc events (currently only EstablishMPPConnection).
    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs;
    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> notify_cqs;
    std::vector<std::thread> cq_workers;
    std::vector<std::thread> notify_cq_workers;
    CollectProcInfoBackgroundTask background_task;
};

} // namespace DB
