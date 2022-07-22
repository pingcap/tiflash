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
#include <Server/IServer.h>
#include <Server/ServerInfo.h>
#include <daemon/BaseDaemon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int IP_ADDRESS_NOT_ALLOWED;
} // namespace ErrorCodes

Poco::Logger * grpc_log = nullptr;
class DAGContext;

static std::string getCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return path;
}
class MockExecutionServer : public BaseDaemon
    , public IServer
{
public:
    explicit MockExecutionServer(std::unique_ptr<Context> & global_context_, std::unordered_map<String, ColumnsWithTypeAndName> executor_id_columns_map_)
        : global_context(global_context_)
        , executor_id_columns_map(executor_id_columns_map_)
    {}
    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    const TiFlashSecurityConfig & securityConfig() const override { return security_config; };

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    Context & context() const override
    {
        return *global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

    std::unordered_map<String, ColumnsWithTypeAndName> getColumns() override
    {
        return executor_id_columns_map;
    }

protected:
    void initialize(Application & self) override
    {
        BaseDaemon::initialize(self);
        logger().information("starting up");
    }

    void uninitialize() override
    {
        logger().information("shutting down");
        BaseDaemon::uninitialize();
    }

    int main(const std::vector<std::string> &) override
    {
        grpc_log = &Poco::Logger::get("grpc");
        Poco::Logger * log = &logger();
        FlashGrpcServerHolder flash_grpc_server_holder(*this, log);
        LOG_FMT_INFO(log, "Start to wait for terminal signal");
        waitForTerminationRequest();
        return Application::EXIT_OK;
    }

    std::string getDefaultCorePath() const override
    {
        return getCanonicalPath(config().getString("path")) + "cores";
    }

private:
    std::unique_ptr<Context> & global_context;
    std::unordered_map<String, ColumnsWithTypeAndName> executor_id_columns_map;

    TiFlashSecurityConfig security_config;

    ServerInfo server_info;

    class FlashGrpcServerHolder
    {
    public:
        FlashGrpcServerHolder(MockExecutionServer & server, Poco::Logger * log_)
            : log(log_)
            , is_shutdown(std::make_shared<std::atomic<bool>>(false))
        {
            grpc::ServerBuilder builder;
            builder.AddListeningPort(Debug::LOCAL_HOST, grpc::InsecureServerCredentials()); // TODO: Port as a parameter.

            /// Init and register flash service.
            flash_service = std::make_unique<FlashService>(server);
            builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5 * 1000));
            builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10 * 1000));
            builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1));
            // number of grpc thread pool's non-temporary threads, better tune it up to avoid frequent creation/destruction of threads
            auto max_grpc_pollers = server.context().getSettingsRef().max_grpc_pollers;
            if (max_grpc_pollers > 0 && max_grpc_pollers <= std::numeric_limits<int>::max())
                builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, max_grpc_pollers);
            builder.RegisterService(flash_service.get());
            LOG_FMT_INFO(log, "Flash service registered");

            /// Kick off grpc server.
            builder.SetMaxReceiveMessageSize(-1);
            builder.SetMaxSendMessageSize(-1);
            flash_grpc_server = builder.BuildAndStart();
            if (!flash_grpc_server)
            {
                throw Exception("Exception happens when start grpc server, the flash.service_addr may be invalid, flash.service_addr is " + Debug::LOCAL_HOST, ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
            }
            LOG_FMT_INFO(log, "Flash grpc server listening on [{}]", Debug::LOCAL_HOST);
        }

        ~FlashGrpcServerHolder()
        {
            try
            {
                /// Shut down grpc server.
                LOG_FMT_INFO(log, "Begin to shut down flash grpc server");
                flash_grpc_server->Shutdown();
                *is_shutdown = true;
                // Wait all existed MPPTunnels done to prevent crash.
                // If all existed MPPTunnels are done, almost in all cases it means all existed MPPTasks and ExchangeReceivers are also done.
                const int max_wait_cnt = 300;
                int wait_cnt = 0;
                while (GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Value() >= 1 && (wait_cnt++ < max_wait_cnt))
                    std::this_thread::sleep_for(std::chrono::seconds(1));

                flash_grpc_server->Wait();
                flash_grpc_server.reset();
                LOG_FMT_INFO(log, "Shut down flash grpc server");
                /// Close flash service.
                LOG_FMT_INFO(log, "Begin to shut down flash service");
                flash_service.reset();
                LOG_FMT_INFO(log, "Shut down flash service");
            }
            catch (...)
            {
                auto message = getCurrentExceptionMessage(false);
                LOG_FMT_FATAL(log, "Exception happens in destructor of FlashGrpcServerHolder with message: {}", message);
                std::terminate();
            }
        }

    private:
        Poco::Logger * log;
        std::shared_ptr<std::atomic<bool>> is_shutdown;
        std::unique_ptr<FlashService> flash_service = nullptr;
        std::unique_ptr<grpc::Server> flash_grpc_server = nullptr;
    };
};


} // namespace DB
