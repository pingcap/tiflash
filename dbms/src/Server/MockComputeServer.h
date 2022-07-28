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

#include <Common/TiFlashMetrics.h>
#include <Debug/astToExecutor.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/FlashService.h>

namespace DB
{
namespace ErrorCodes
{
extern const int IP_ADDRESS_NOT_ALLOWED;
} // namespace ErrorCodes

// TODO: support AsyncFlashService.
class MockComputeServer
{
public:
    MockComputeServer(Context & global_context, Poco::Logger * log_)
        : log(log_)
        , is_shutdown(std::make_shared<std::atomic<bool>>(false))
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Debug::LOCAL_HOST, grpc::InsecureServerCredentials()); // TODO: Port as a parameter.

        // Init and register flash service.
        TiFlashSecurityConfig security_config;
        flash_service = std::make_unique<FlashService>(security_config, global_context);
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5 * 1000));
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10 * 1000));
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1));
        // number of grpc thread pool's non-temporary threads, better tune it up to avoid frequent creation/destruction of threads
        auto max_grpc_pollers = global_context.getSettingsRef().max_grpc_pollers;
        if (max_grpc_pollers > 0 && max_grpc_pollers <= std::numeric_limits<int>::max())
            builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, max_grpc_pollers);
        builder.RegisterService(flash_service.get());
        LOG_FMT_DEBUG(log, "Flash service registered");

        // Kick off grpc server.
        builder.SetMaxReceiveMessageSize(-1);
        builder.SetMaxSendMessageSize(-1);
        flash_grpc_server = builder.BuildAndStart();
        if (!flash_grpc_server)
        {
            throw Exception("Exception happens when start grpc server, the flash.service_addr may be invalid, flash.service_addr is " + Debug::LOCAL_HOST, ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
        }
        LOG_FMT_DEBUG(log, "Flash grpc server listening on [{}]", Debug::LOCAL_HOST);
    }

    ~MockComputeServer()
    {
        try
        {
            // Shut down grpc server.
            LOG_FMT_DEBUG(log, "Begin to shut down flash grpc server");
            flash_grpc_server->Shutdown();
            *is_shutdown = true;
            // Port from Server.cpp
            // Wait all existed MPPTunnels done to prevent crash.
            // If all existed MPPTunnels are done, almost in all cases it means all existed MPPTasks and ExchangeReceivers are also done.
            const int max_wait_cnt = 300;
            int wait_cnt = 0;

            while (GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Value() >= 1 && (wait_cnt++ < max_wait_cnt))
                std::this_thread::sleep_for(std::chrono::seconds(1));
            flash_grpc_server->Wait();
            flash_grpc_server.reset();
            LOG_FMT_DEBUG(log, "Shut down flash grpc server");
            /// Close flash service.
            LOG_FMT_DEBUG(log, "Begin to shut down flash service");
            flash_service.reset();
            LOG_FMT_DEBUG(log, "Shut down flash service");
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_FMT_FATAL(log, "Exception happens in destructor of MockComputeServer with message: {}", message);
            std::terminate();
        }
    }

private:
    Poco::Logger * log;
    std::shared_ptr<std::atomic<bool>> is_shutdown;
    std::unique_ptr<FlashService> flash_service = nullptr;
    std::unique_ptr<grpc::Server> flash_grpc_server = nullptr;
};


} // namespace DB
