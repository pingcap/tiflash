#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Core/Types.h>
#include <Flash/BatchCommandsHandler.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/MPPHandler.h>
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
    : server(server_),
      metrics(server.context().getTiFlashMetrics()),
      security_config(server_.securityConfig()),
      log(&Logger::get("FlashService"))
{
    size_t threads = static_cast<size_t>(server_.context().getSettingsRef().coprocessor_thread_pool_size);
    threads = threads ? threads : 4 * getNumberOfPhysicalCPUCores();
    LOG_INFO(log, "Use a thread pool with " << threads << " threads to handling coprocessor requests.");
    cop_thread_pool = std::make_unique<ThreadPool>(threads);
}

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context, const coprocessor::Request * request, coprocessor::Response * response)
{
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling coprocessor request: " << request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(metrics, tiflash_coprocessor_request_count, type_cop).Increment();
    GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_cop).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_cop).Decrement();
        GET_METRIC(metrics, tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds());
        GET_METRIC(metrics, tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());
    });

    grpc::Status ret = execute_in_thread_pool([&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);
        return cop_handler.execute();
    });

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle coprocessor request done: " << ret.error_code() << ", " << ret.error_message());
    return ret;
}

::grpc::Status FlashService::BatchCoprocessor(::grpc::ServerContext * grpc_context, const ::coprocessor::BatchRequest * request,
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer)
{
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling coprocessor request: " << request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }

    GET_METRIC(metrics, tiflash_coprocessor_request_count, type_super_batch).Increment();
    GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_super_batch).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_super_batch).Decrement();
        GET_METRIC(metrics, tiflash_coprocessor_request_duration_seconds, type_super_batch).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    grpc::Status ret = execute_in_thread_pool([&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(context, request->context(), *grpc_context);
        BatchCoprocessorHandler cop_handler(cop_context, request, writer);
        return cop_handler.execute();
    });

    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle coprocessor request done: " << ret.error_code() << ", " << ret.error_message());
    return ret;
}

::grpc::Status FlashService::DispatchMPPTask(
    ::grpc::ServerContext * grpc_context, const ::mpp::DispatchTaskRequest * request, ::mpp::DispatchTaskResponse * response)
{
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling mpp dispatch request: " << request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    // TODO: Add metric.

    grpc::Status ret = execute_in_thread_pool([&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        MPPHandler mpp_handler(context, *request);
        return mpp_handler.execute(response);
    });

    return ret;
}

::grpc::Status FlashService::EstablishMPPConnection(::grpc::ServerContext * grpc_context,
    const ::mpp::EstablishMPPConnectionRequest * request, ::grpc::ServerWriter<::mpp::MPPDataPacket> * writer)
{
    // Establish a pipe for data transferring. The pipes has registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling establish mpp connection request: " << request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
    }
    // TODO: Add metric.

    grpc::Status ret = execute_in_thread_pool([&] {
        auto [context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }

        auto & tmt_context = context.getTMTContext();
        auto task_manager = tmt_context.getMPPTaskManager();
        std::chrono::seconds timeout(10);
        MPPTaskPtr sender_task = task_manager->findTaskWithTimeout(request->sender_meta(), timeout);
        if (sender_task == nullptr)
        {
            auto errMsg = "can't find task [" + toString(request->sender_meta().start_ts()) + "," + toString(request->sender_meta().task_id())
                + "] within " + toString(timeout.count()) + " s";
            LOG_ERROR(log, errMsg);
            mpp::MPPDataPacket packet;
            auto err = new mpp::Error();
            err->set_msg(errMsg);
            packet.set_allocated_error(err);
            writer->Write(packet);
            return grpc::Status::OK;
        }
        MPPTunnelPtr tunnel = sender_task->getTunnelWithTimeout(request->receiver_meta(), timeout);
        if (tunnel == nullptr)
        {
            auto errMsg = "can't find tunnel ( " + toString(request->receiver_meta().task_id()) + " + "
                + toString(request->sender_meta().task_id()) + " ) within " + toString(timeout.count()) + " s";
            LOG_ERROR(log, errMsg);
            mpp::MPPDataPacket packet;
            auto err = new mpp::Error();
            err->set_msg(errMsg);
            packet.set_allocated_error(err);
            writer->Write(packet);
            return grpc::Status::OK;
        }
        Stopwatch stopwatch;
        tunnel->connect(writer);
        LOG_DEBUG(log, "connect tunnel successfully and begin to wait");
        tunnel->waitForFinish();
        LOG_INFO(log, "connection for " << tunnel->tunnel_id << " cost " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms.");
        // TODO: Check if there are errors in task.

        return grpc::Status::OK;
    });

    return ret;
}

// This function is deprecated.
grpc::Status FlashService::BatchCommands(
    grpc::ServerContext * grpc_context, grpc::ServerReaderWriter<::tikvpb::BatchCommandsResponse, tikvpb::BatchCommandsRequest> * stream)
{
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
        GET_METRIC(metrics, tiflash_coprocessor_request_count, type_batch).Increment();
        GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_batch).Increment();
        SCOPE_EXIT({ GET_METRIC(metrics, tiflash_coprocessor_handling_request_count, type_batch).Decrement(); });
        auto start_time = std::chrono::system_clock::now();
        SCOPE_EXIT({
            std::chrono::duration<double> duration_sec = std::chrono::system_clock::now() - start_time;
            GET_METRIC(metrics, tiflash_coprocessor_request_duration_seconds, type_batch).Observe(duration_sec.count());
            GET_METRIC(metrics, tiflash_coprocessor_response_bytes).Increment(response.ByteSizeLong());
        });

        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling batch commands: " << request.DebugString());

        BatchCommandsContext batch_commands_context(
            context, [this](const grpc::ServerContext * grpc_server_context) { return createDBContext(grpc_server_context); },
            *grpc_context);
        BatchCommandsHandler batch_commands_handler(batch_commands_context, request, response);
        auto ret = batch_commands_handler.execute();
        if (!ret.ok())
        {
            LOG_DEBUG(
                log, __PRETTY_FUNCTION__ << ": Handle batch commands request done: " << ret.error_code() << ", " << ret.error_message());
            return ret;
        }

        if (!stream->Write(response))
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Write response failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }

        LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle batch commands request done: " << ret.error_code() << ", " << ret.error_message());
    }

    return grpc::Status::OK;
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return it->second.data();
    return default_val;
}

grpc::Status FlashService::execute_in_thread_pool(std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    cop_thread_pool->schedule([&task] { task(); });
    return future.get();
}

std::tuple<Context, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    try
    {
        /// Create DB context.
        Context context = server.context();
        context.setGlobalContext(server.context());

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

        context.setUser(user, password, client_address, quota_key);

        String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        context.setCurrentQueryId(query_id);

        ClientInfo & client_info = context.getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        /// Set DAG parameters.
        std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
        if (!dag_records_per_chunk_str.empty())
        {
            context.setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
        }

        return std::make_tuple(context, grpc::Status::OK);
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": DB Exception: " << e.message());
        return std::make_tuple(server.context(), grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message()));
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
        return std::make_tuple(server.context(), grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
    }
    catch (...)
    {
        LOG_ERROR(log, __PRETTY_FUNCTION__ << ": other exception");
        return std::make_tuple(server.context(), grpc::Status(grpc::StatusCode::INTERNAL, "other exception"));
    }
}

} // namespace DB
