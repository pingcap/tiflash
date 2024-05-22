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

#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Flash/EstablishCall.h>
#include <Interpreters/Context.h>
#include <Server/FlashGrpcServerHolder.h>

// In order to include grpc::SecureServerCredentials which used in
// sslServerCredentialsWithFetcher()
// We implement sslServerCredentialsWithFetcher() to set config fetcher
// to auto reload sslServerCredentials
#include "../../contrib/grpc/src/cpp/server/secure_server_credentials.h"

namespace DB
{
namespace ErrorCodes
{
extern const int IP_ADDRESS_NOT_ALLOWED;
} // namespace ErrorCodes
namespace
{
void handleRpcs(grpc::ServerCompletionQueue * curcq, const LoggerPtr & log)
{
    GET_METRIC(tiflash_thread_count, type_total_rpc_async_worker).Increment();
    SCOPE_EXIT({ GET_METRIC(tiflash_thread_count, type_total_rpc_async_worker).Decrement(); });
    void * tag = nullptr; // uniquely identifies a request.
    bool ok = false;
    while (true)
    {
        String err_msg;
        try
        {
            // Block waiting to read the next event from the completion queue. The
            // event is uniquely identified by its tag, which in this case is the
            // memory address of a EstablishCallData instance.
            // The return value of Next should always be checked. This return value
            // tells us whether there is any kind of event or cq is shutting down.
            if (!curcq->Next(&tag, &ok))
            {
                LOG_INFO(log, "CQ is fully drained and shut down");
                break;
            }
            GET_METRIC(tiflash_thread_count, type_active_rpc_async_worker).Increment();
            SCOPE_EXIT({ GET_METRIC(tiflash_thread_count, type_active_rpc_async_worker).Decrement(); });
            // If ok is false, it means server is shutdown.
            // We need not log all not ok events, since the volumn is large which will pollute the content of log.
            reinterpret_cast<GRPCKickTag *>(tag)->execute(ok);
        }
        catch (Exception & e)
        {
            err_msg = e.displayText();
            LOG_ERROR(log, "handleRpcs meets error: {} Stack Trace : {}", err_msg, e.getStackTrace().toString());
        }
        catch (pingcap::Exception & e)
        {
            err_msg = e.message();
            LOG_ERROR(log, "handleRpcs meets error: {}", err_msg);
        }
        catch (std::exception & e)
        {
            err_msg = e.what();
            LOG_ERROR(log, "handleRpcs meets error: {}", err_msg);
        }
        catch (...)
        {
            err_msg = "unrecovered error";
            LOG_ERROR(log, "handleRpcs meets error: {}", err_msg);
            throw;
        }
    }
}
} // namespace

static grpc_ssl_certificate_config_reload_status sslServerCertificateConfigCallback(
    void * arg,
    grpc_ssl_server_certificate_config ** config)
{
    if (config == nullptr)
    {
        return GRPC_SSL_CERTIFICATE_CONFIG_RELOAD_FAIL;
    }
    auto * context = static_cast<Context *>(arg);
    auto options = context->getSecurityConfig()->readAndCacheSslCredentialOptions();
    if (options.has_value())
    {
        grpc_ssl_pem_key_cert_pair pem_key_cert_pair
            = {options.value().pem_private_key.c_str(), options.value().pem_cert_chain.c_str()};
        *config
            = grpc_ssl_server_certificate_config_create(options.value().pem_root_certs.c_str(), &pem_key_cert_pair, 1);
        return GRPC_SSL_CERTIFICATE_CONFIG_RELOAD_NEW;
    }
    return GRPC_SSL_CERTIFICATE_CONFIG_RELOAD_UNCHANGED;
}

grpc_server_credentials * grpcSslServerCredentialsCreateWithFetcher(
    grpc_ssl_client_certificate_request_type client_certificate_request,
    Context * context)
{
    grpc_ssl_server_credentials_options * options = grpc_ssl_server_credentials_create_options_using_config_fetcher(
        client_certificate_request,
        sslServerCertificateConfigCallback,
        reinterpret_cast<void *>(context));
    return grpc_ssl_server_credentials_create_with_options(options);
}

std::shared_ptr<grpc::ServerCredentials> sslServerCredentialsWithFetcher(Context & context)
{
    grpc_server_credentials * c_creds = grpcSslServerCredentialsCreateWithFetcher(
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY,
        &context);
    return std::shared_ptr<grpc::ServerCredentials>(new grpc::SecureServerCredentials(c_creds));
}

FlashGrpcServerHolder::FlashGrpcServerHolder(
    Context & context,
    Poco::Util::LayeredConfiguration & config_,
    const TiFlashRaftConfig & raft_config,
    const LoggerPtr & log_)
    : log(log_)
    , is_shutdown(std::make_shared<std::atomic<bool>>(false))
{
    grpc::ServerBuilder builder;

    if (!context.isTest() && context.getSecurityConfig()->hasTlsConfig())
    {
        builder.AddListeningPort(raft_config.flash_server_addr, sslServerCredentialsWithFetcher(context));
    }
    else
    {
        builder.AddListeningPort(raft_config.flash_server_addr, grpc::InsecureServerCredentials());
    }

    /// Init and register flash service.
    bool enable_async_server = context.getSettingsRef().enable_async_server;
    if (enable_async_server)
        flash_service = std::make_unique<AsyncFlashService>();
    else
        flash_service = std::make_unique<FlashService>();
    flash_service->init(context);

    diagnostics_service = std::make_unique<DiagnosticsService>(context, config_);
    builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5 * 1000));
    builder.SetOption(
        grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10 * 1000));
    // number of grpc thread pool's non-temporary threads, better tune it up to avoid frequent creation/destruction of threads
    auto max_grpc_pollers = context.getSettingsRef().max_grpc_pollers;
    if (max_grpc_pollers > 0 && max_grpc_pollers <= std::numeric_limits<int>::max())
        builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, max_grpc_pollers);
    builder.RegisterService(flash_service.get());
    LOG_INFO(log, "Flash service registered");
    builder.RegisterService(diagnostics_service.get());
    LOG_INFO(log, "Diagnostics service registered");

    /// Kick off grpc server.
    // Prevent TiKV from throwing "Received message larger than max (4404462 vs. 4194304)" error.
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    int async_cq_num = context.getSettingsRef().async_cqs;
    if (enable_async_server)
    {
        for (int i = 0; i < async_cq_num; ++i)
        {
            cqs.emplace_back(builder.AddCompletionQueue());
            notify_cqs.emplace_back(builder.AddCompletionQueue());
        }
    }
    flash_grpc_server = builder.BuildAndStart();
    if (!flash_grpc_server)
    {
        throw Exception(
            "Exception happens when start grpc server, the flash.service_addr may be invalid, flash.service_addr is "
                + raft_config.flash_server_addr,
            ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
    }
    LOG_INFO(log, "Flash grpc server listening on [{}]", raft_config.flash_server_addr);
    Debug::setServiceAddr(raft_config.flash_server_addr);
    if (enable_async_server)
    {
        int preallocated_request_count_per_poller = context.getSettingsRef().preallocated_request_count_per_poller;
        int pollers_per_cq = context.getSettingsRef().async_pollers_per_cq;
        for (int i = 0; i < async_cq_num * pollers_per_cq; ++i)
        {
            auto * cq = cqs[i / pollers_per_cq].get();
            auto * notify_cq = notify_cqs[i / pollers_per_cq].get();
            for (int j = 0; j < preallocated_request_count_per_poller; ++j)
            {
                // EstablishCallData will handle its lifecycle by itself.
                EstablishCallData::spawn(
                    assert_cast<AsyncFlashService *>(flash_service.get()),
                    cq,
                    notify_cq,
                    is_shutdown);
            }
            cq_workers.emplace_back(
                ThreadFactory::newThread(false, "async_poller", [cq, this] { handleRpcs(cq, log); }));
            notify_cq_workers.emplace_back(
                ThreadFactory::newThread(false, "async_poller", [notify_cq, this] { handleRpcs(notify_cq, log); }));
        }
    }
}

FlashGrpcServerHolder::~FlashGrpcServerHolder()
{
    try
    {
        /// Shut down grpc server.
        LOG_INFO(log, "Begin to shut down flash grpc server");
        flash_grpc_server->Shutdown();
        *is_shutdown = true;
        // Wait all existed MPPTunnels done to prevent crash.
        // If all existed MPPTunnels are done, almost in all cases it means all existed MPPTasks and ExchangeReceivers are also done.
        const int max_wait_cnt = 300;
        int wait_cnt = 0;
        while (GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Value() >= 1 && (wait_cnt++ < max_wait_cnt))
            std::this_thread::sleep_for(std::chrono::seconds(1));
        if (GET_METRIC(tiflash_object_count, type_count_of_mpptunnel).Value() >= 1)
            LOG_WARNING(
                log,
                "Wait {} seconds for mpp tunnels shutdown, still some mpp tunnels are alive, potential resource leak",
                wait_cnt);
        else
            LOG_INFO(log, "Wait {} seconds for mpp tunnels shutdown, all finished", wait_cnt);

        for (auto & cq : cqs)
            cq->Shutdown();
        for (auto & cq : notify_cqs)
            cq->Shutdown();

        for (auto & worker : cq_workers)
            worker.join();
        for (auto & worker : notify_cq_workers)
            worker.join();

        flash_grpc_server->Wait();
        flash_grpc_server.reset();
        if (GRPCCompletionQueuePool::global_instance)
            GRPCCompletionQueuePool::global_instance->markShutdown();

        GRPCCompletionQueuePool::global_instance = nullptr;
        LOG_INFO(log, "Shut down flash grpc server");

        /// Close flash service.
        LOG_INFO(log, "Begin to shut down flash service");
        flash_service.reset();
        LOG_INFO(log, "Shut down flash service");
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_FATAL(log, "Exception happens in destructor of FlashGrpcServerHolder with message: {}", message);
        std::terminate();
    }
}

void FlashGrpcServerHolder::setMockStorage(MockStorage * mock_storage)
{
    flash_service->setMockStorage(mock_storage);
}

void FlashGrpcServerHolder::setMockMPPServerInfo(MockMPPServerInfo info)
{
    flash_service->setMockMPPServerInfo(info);
}

std::unique_ptr<FlashService> & FlashGrpcServerHolder::flashService()
{
    return flash_service;
}
} // namespace DB
