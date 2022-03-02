#include <Common/CPUAffinityManager.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <Core/Types.h>
#include <Flash/BatchCommandsHandler.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/Utils.h>
#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/server_builder.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

constexpr char tls_err_msg[] = "common name check is failed";

FlashService::FlashService(IServer & server_)
    : server(server_)
    , security_config(server_.securityConfig())
    , log(&Poco::Logger::get("FlashService"))
{
    auto settings = server_.context().getSettingsRef();
    const size_t default_size = 2 * getNumberOfPhysicalCPUCores();

    size_t cop_pool_size = static_cast<size_t>(settings.cop_pool_size);
    cop_pool_size = cop_pool_size ? cop_pool_size : default_size;
    LOG_FMT_INFO(log, "Use a thread pool with {} threads to handle cop requests.", cop_pool_size);
    cop_pool = std::make_unique<ThreadPool>(cop_pool_size, [] { setThreadName("cop-pool"); });

    size_t batch_cop_pool_size = static_cast<size_t>(settings.batch_cop_pool_size);
    batch_cop_pool_size = batch_cop_pool_size ? batch_cop_pool_size : default_size;
    LOG_FMT_INFO(log, "Use a thread pool with {} threads to handle batch cop requests.", batch_cop_pool_size);
    batch_cop_pool = std::make_unique<ThreadPool>(batch_cop_pool_size, [] { setThreadName("batch-cop-pool"); });
}

// Use executeInThreadPool to submit job to thread pool which return grpc::Status.
grpc::Status executeInThreadPool(const std::unique_ptr<ThreadPool> & pool, std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    pool->schedule([&task] { task(); });
    return future.get();
}

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context,
    const coprocessor::Request * request,
    coprocessor::Response * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "{}: Handling coprocessor request: {}", __PRETTY_FUNCTION__, request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_cop).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    grpc::Status ret = executeInThreadPool(cop_pool, [&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);
        return cop_handler.execute();
    });

    LOG_FMT_DEBUG(log, "{}: Handle coprocessor request done: {}, {}", __PRETTY_FUNCTION__, ret.error_code(), ret.error_message());
    return ret;
}

::grpc::Status FlashService::BatchCoprocessor(::grpc::ServerContext * grpc_context, const ::coprocessor::BatchRequest * request, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "{}: Handling coprocessor request: {}", __PRETTY_FUNCTION__, request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_super_batch).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_super_batch).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    grpc::Status ret = executeInThreadPool(batch_cop_pool, [&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*context, request->context(), *grpc_context);
        BatchCoprocessorHandler cop_handler(cop_context, request, writer);
        return cop_handler.execute();
    });

    LOG_FMT_DEBUG(log, "{}: Handle coprocessor request done: {}, {}", __PRETTY_FUNCTION__, ret.error_code(), ret.error_message());
    return ret;
}

::grpc::Status FlashService::DispatchMPPTask(
    ::grpc::ServerContext * grpc_context,
    const ::mpp::DispatchTaskRequest * request,
    ::mpp::DispatchTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_FMT_DEBUG(log, "{}: Handling mpp dispatch request: {}", __PRETTY_FUNCTION__, request->DebugString());
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_dispatch_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    MPPHandler mpp_handler(*request);
    return mpp_handler.execute(context, response);
}

::grpc::Status FlashService::IsAlive(::grpc::ServerContext * grpc_context [[maybe_unused]],
                                     const ::mpp::IsAliveRequest * request [[maybe_unused]],
                                     ::mpp::IsAliveResponse * response [[maybe_unused]])
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    auto & tmt_context = context->getTMTContext();
    response->set_available(tmt_context.checkRunning());
    return ::grpc::Status::OK;
}

::grpc::Status FlashService::EstablishMPPConnection0(::grpc::ServerContext * grpc_context,
                                                     const ::mpp::EstablishMPPConnectionRequest * request,
                                                     ::grpc::ServerWriter<::mpp::MPPDataPacket> * sync_writer,
                                                     EstablishCallData * calldata)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes has registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_FMT_DEBUG(log, "{}: Handling establish mpp connection request: {}", __PRETTY_FUNCTION__, request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        auto status = grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
        if (calldata)
        {
            calldata->WriteDone(status);
        }
        return status;
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        if (calldata)
            calldata->WriteDone(status);
        return status;
    }

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    std::chrono::seconds timeout(10);
    std::string err_msg;
    MPPTunnelPtr tunnel = nullptr;
    {
        MPPTaskPtr sender_task = task_manager->findTaskWithTimeout(request->sender_meta(), timeout, err_msg);
        if (sender_task != nullptr)
        {
            std::tie(tunnel, err_msg) = sender_task->getTunnel(request);
        }
        if (tunnel == nullptr)
        {
            if (calldata)
            {
                LOG_ERROR(log, err_msg);
                calldata->WriteErr(getPacketWithError(err_msg));
                return grpc::Status::OK;
            }
            else
            {
                LOG_ERROR(log, err_msg);
                if (sync_writer->Write(getPacketWithError(err_msg)))
                {
                    return grpc::Status::OK;
                }
                else
                {
                    LOG_FMT_DEBUG(log, "{}: Write error message failed for unknown reason.", __PRETTY_FUNCTION__);
                    return grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
                }
            }
        }
    }
    Stopwatch stopwatch;
    if (calldata)
    {
        tunnel->is_async = true;
        calldata->attachTunnel(tunnel);
        tunnel->connect(calldata);
        LOG_FMT_DEBUG(tunnel->getLogger(), "connect tunnel successfully in async way");
    }
    else
    {
        SyncPacketWriter writer(sync_writer);
        tunnel->connect(&writer);
        LOG_FMT_DEBUG(tunnel->getLogger(), "connect tunnel successfully and begin to wait");
        tunnel->waitForFinish();
        LOG_FMT_INFO(tunnel->getLogger(), "connection for {} cost {} ms.", tunnel->id(), stopwatch.elapsedMilliseconds());
    }

    // TODO: Check if there are errors in task.

    return grpc::Status::OK;
}

::grpc::Status FlashService::CancelMPPTask(
    ::grpc::ServerContext * grpc_context,
    const ::mpp::CancelTaskRequest * request,
    ::mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_FMT_DEBUG(log, "{}: cancel mpp task request: {}", __PRETTY_FUNCTION__, request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_cancel_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cancel_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        auto err = std::make_unique<mpp::Error>();
        err->set_msg("error status");
        response->set_allocated_error(err.release());
        return status;
    }
    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->cancelMPPQuery(request->meta().start_ts(), "Receive cancel request from TiDB");
    return grpc::Status::OK;
}

// This function is deprecated.
grpc::Status FlashService::BatchCommands(
    grpc::ServerContext * grpc_context,
    grpc::ServerReaderWriter<::tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    auto [context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }

    tikvpb::BatchCommandsRequest request;
    while (stream->Read(&request))
    {
        tikvpb::BatchCommandsResponse response;
        GET_METRIC(tiflash_coprocessor_request_count, type_batch).Increment();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Increment();
        SCOPE_EXIT({ GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Decrement(); });
        auto start_time = std::chrono::system_clock::now();
        SCOPE_EXIT({
            std::chrono::duration<double> duration_sec = std::chrono::system_clock::now() - start_time;
            GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_batch).Observe(duration_sec.count());
            GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response.ByteSizeLong());
        });

        LOG_FMT_DEBUG(log, "{}: Handling batch commands: {}", __PRETTY_FUNCTION__, request.DebugString());

        BatchCommandsContext batch_commands_context(
            *context,
            [this](const grpc::ServerContext * grpc_server_context) { return createDBContext(grpc_server_context); },
            *grpc_context);
        BatchCommandsHandler batch_commands_handler(batch_commands_context, request, response);
        auto ret = batch_commands_handler.execute();
        if (!ret.ok())
        {
            LOG_FMT_DEBUG(
                log,
                "{}: Handle batch commands request done: {}, {}",
                __PRETTY_FUNCTION__,
                ret.error_code(),
                ret.error_message());
            return ret;
        }

        if (!stream->Write(response))
        {
            LOG_FMT_DEBUG(log, "{}: Write response failed for unknown reason.", __PRETTY_FUNCTION__);
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }

        LOG_FMT_DEBUG(log, "{}: Handle batch commands request done: {}, {}", __PRETTY_FUNCTION__, ret.error_code(), ret.error_message());
    }

    return grpc::Status::OK;
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return it->second.data();
    return default_val;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    try
    {
        /// Create DB context.
        auto context = std::make_shared<Context>(server.context());
        context->setGlobalContext(server.context());

        /// Set a bunch of client information.
        std::string user = getClientMetaVarWithDefault(grpc_context, "user", "default");
        std::string password = getClientMetaVarWithDefault(grpc_context, "password", "");
        std::string quota_key = getClientMetaVarWithDefault(grpc_context, "quota_key", "");
        std::string peer = grpc_context->peer();
        Int64 pos = peer.find(':');
        if (pos == -1)
        {
            return std::make_tuple(context, ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer));
        }
        std::string client_ip = peer.substr(pos + 1);
        Poco::Net::SocketAddress client_address(client_ip);

        context->setUser(user, password, client_address, quota_key);

        String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        context->setCurrentQueryId(query_id);

        ClientInfo & client_info = context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        /// Set DAG parameters.
        std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
        if (!dag_records_per_chunk_str.empty())
        {
            context->setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
        }

        return std::make_tuple(context, grpc::Status::OK);
    }
    catch (Exception & e)
    {
        LOG_FMT_ERROR(log, "{}: DB Exception: {}", __PRETTY_FUNCTION__, e.message());
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message()));
    }
    catch (const std::exception & e)
    {
        LOG_FMT_ERROR(log, "{}: std exception: {}", __PRETTY_FUNCTION__, e.what());
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
    }
    catch (...)
    {
        LOG_FMT_ERROR(log, "{}: other exception", __PRETTY_FUNCTION__);
        return std::make_tuple(std::make_shared<Context>(server.context()), grpc::Status(grpc::StatusCode::INTERNAL, "other exception"));
    }
}

} // namespace DB
