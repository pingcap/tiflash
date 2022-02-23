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


const int tunnel_sender_cap = 40;
const int rpc_exe_cap = 200;
const int rpc_reg_cap = 10;

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

constexpr char tls_err_msg[] = "common name check is failed";

FlashService::FlashService(IServer & server_)
    : calldata_to_reg_queue(1000)
    , server(server_)
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
    std::shared_ptr<ThreadManager> thd_manager = newThreadManager();
    for (int i = 0; i < rpc_reg_cap; i++)
    {
        thd_manager->scheduleThenDetach(false, "calldata_reg", [this] {
            CallDataReg reg;
            while (calldata_to_reg_queue.pop(reg))
            {
                new EstablishCallData(reg.service, reg.cq, reg.notify_cq);
            }
        });
    }
    for (int i = 0; i < rpc_exe_cap; i++)
    {
        calldata_proc_queues.emplace_back(std::make_shared<MPMCQueue<EstablishCallData *>>(100));
        auto queue = calldata_proc_queues.back();
        thd_manager->scheduleThenDetach(false, "rpc_exec", [queue] {
            EstablishCallData * calldata;
            while (queue->pop(calldata))
            {
                calldata->Proceed0();
            }
        });
    }
    for (int i = 0; i < tunnel_sender_cap; i++)
    {
        tunnel_send_op_queues.emplace_back(std::make_shared<MPMCQueue<std::shared_ptr<DB::MPPTunnel>>>(100));
        auto queue = tunnel_send_op_queues[i];
        thd_manager->scheduleThenDetach(false, "tunnel_sender", [queue] {
            std::shared_ptr<DB::MPPTunnel> obj;
            while (queue->pop(obj))
            {
                obj->sendJob();
            }
        });
    }
}

void SubmitTunnelSendOp(FlashService * srv, const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    int idx = srv->tunnel_send_idx++;
    idx = idx % tunnel_sender_cap;
    if (idx < 0)
        idx += tunnel_sender_cap;
    srv->tunnel_send_op_queues[idx]->push(mpptunnel);
}

void SubmitCalldataProcTask(FlashService * srv, EstablishCallData * cd)
{
    int idx = srv->rpc_exe_idx++;
    idx = idx % rpc_exe_cap;
    if (idx < 0)
        idx += rpc_exe_cap;
    srv->calldata_proc_queues[idx]->push(cd);
}

FlashService::~FlashService()
{
    for (auto & q : tunnel_send_op_queues)
        q->finish();
    for (auto & q : calldata_proc_queues)
        q->finish();
    calldata_to_reg_queue.finish();
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
    LOG_FMT_ERROR(log, "wwwoody! dispatch the task [{},{}]", request->meta().start_ts(), request->meta().task_id());
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

bool FlashService::EstablishMPPConnection4Async(::grpc::ServerContext * grpc_context, const ::mpp::EstablishMPPConnectionRequest * request, EstablishCallData * calldata)
{
    // Establish a pipe for data transferring. The pipes has registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    if (!request->has_sender_meta())
    {
        calldata->WriteDone(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + grpc_context->peer()));
        return true;
    }
    LOG_FMT_DEBUG(log, "{}: Handling establish mpp connection request: {}", __PRETTY_FUNCTION__, request->DebugString());

    if (!security_config.checkGrpcContext(grpc_context))
    {
        calldata->WriteDone(grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg));
        return true;
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
        calldata->WriteDone(status);
        return true;
    }

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    std::chrono::seconds timeout(10);
    std::string err_msg;
    MPPTunnelPtr tunnel = nullptr;
    {
        MPPTaskPtr sender_task = task_manager->findTaskWithTimeout(request->sender_meta(), timeout, err_msg, calldata);
        if (sender_task != nullptr)
        {
            std::tie(tunnel, err_msg) = sender_task->getTunnel(request);
        }
        if (tunnel == nullptr)
        {
            if (err_msg == "pending")
            {
                return true;
            }
            else
            {
                LOG_ERROR(log, err_msg);

                calldata->WriteErr(getPacketWithError(err_msg));
                return true;
            }
        }
    }
    tunnel->is_async = true;
    calldata->attachTunnel(tunnel);
    tunnel->connect(calldata);
    LOG_FMT_DEBUG(tunnel->getLogger(), "connect tunnel successfully and begin to wait");
    return true;
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

grpc::Status FlashService::executeInThreadPool(const std::unique_ptr<ThreadPool> & pool, std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    pool->schedule([&task] { task(); });
    return future.get();
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

std::string DeriveErrWhat(std::exception_ptr eptr) // passing by value is ok
{
    try
    {
        if (eptr)
        {
            std::rethrow_exception(eptr);
        }
    }
    catch (const std::exception & e)
    {
        return std::string(e.what());
    }
    return "";
}

EstablishCallData::EstablishCallData(FlashService * service, grpc::ServerCompletionQueue * cq, grpc::ServerCompletionQueue * notify_cq)
    : service_(service)
    , cq_(cq)
    , notify_cq_(notify_cq)
    , responder_(&ctx_)
    , state_(CREATE)
    , send_queue_(nullptr)
{
    // Invoke the serving logic right away.
    Proceed();
}

//tunnel has been attached
void EstablishCallData::ContinueFromPending()
{
    state_ = EstablishCallData::CallStatus::JOIN;
    mpptunnel_->is_async = true;
    //    attachTunnel(tunnel);
    try
    {
        mpptunnel_->connect(this);
    }
    catch (...)
    {
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), "has connected");
        WriteDone(status, false);
    }
}

bool EstablishCallData::TryWrite()
{
    //check whether has valid msg to write
    {
        std::unique_lock lk(mu);
        if (ready && (send_queue_ && (send_queue_->size() || send_queue_->isFinished()) && mpptunnel_)) //not ready or no packet
            ready = false;
        else
            return false;
    }
    //has valid msg, submit write task
    SubmitTunnelSendOp(service_, mpptunnel_);
    return true;
}

void EstablishCallData::AsyncRpcInitOp()
{
    std::exception_ptr eptr = nullptr;
    try
    {
        service_->EstablishMPPConnection4Async(&ctx_, &request_, this);
    }
    catch (...)
    {
        eptr = std::current_exception();
    }
    if (eptr)
    {
        state_ = FINISH;
        grpc::Status status(static_cast<grpc::StatusCode>(GRPC_STATUS_UNKNOWN), DeriveErrWhat(eptr));
        responder_.Finish(status, this);
        return;
    }
}

void EstablishCallData::Pending()
{
    state_ = PENDING;
}

bool EstablishCallData::Write(const mpp::MPPDataPacket & packet, bool need_wait)
{
    // The actual processing.
    try
    {
        if (need_wait)
        {
            std::unique_lock lk(mu);
            cv.wait(lk, [&] { return ready.load(); });
            ready = false;
        }
        responder_.Write(packet, this);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void EstablishCallData::WriteErr(const mpp::MPPDataPacket & packet)
{
    state_ = ERR_HANDLE;
    bool ret = Write(packet);
    if (ret)
        status4err = grpc::Status::OK;
    else
        status4err = grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
}

void EstablishCallData::WriteDone(const ::grpc::Status & status, bool need_wait)
{
    // And we are done! Let the gRPC runtime know we've finished, using the
    // memory address of this instance as the uniquely identifying tag for
    // the event.
    if (need_wait)
    {
        std::unique_lock lk(mu);
        cv.wait(lk, [&] { return ready.load(); });
        ready = false;
    }
    state_ = FINISH;
    responder_.Finish(status, this);
}

void EstablishCallData::notifyReady()
{
    //    std::unique_lock lk(mu);
    ready = true;
    cv.notify_one();
}

void EstablishCallData::Proceed()
{
    if (state_ == CREATE)
    {
        // Make this instance progress to the PROCESS state.
        state_ = PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different EstablishCallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this EstablishCallData instance.
        service_->RequestEstablishMPPConnection(&ctx_, &request_, &responder_, cq_, notify_cq_, this);
    }
    else
    {
        SubmitCalldataProcTask(service_, this);
    }
}


void EstablishCallData::Proceed0()
{
    if (state_ == PROCESS)
    {
        state_ = JOIN;
        // Spawn a new EstablishCallData instance to serve new clients while we process
        // the one for this EstablishCallData. The instance will deallocate itself as
        // part of its FINISH state.
        service_->calldata_to_reg_queue.push(CallDataReg{service_, cq_, notify_cq_});

        {
            std::unique_lock lk(mu);
            notifyReady();
        }
        AsyncRpcInitOp();
    }
    else if (state_ == PENDING)
    {
        ContinueFromPending();
    }
    else if (state_ == JOIN)
    {
        {
            std::unique_lock lk(mu);
            if (send_queue_ && (send_queue_->size() || send_queue_->isFinished()) && mpptunnel_)
            {
                ready = false;
                lk.unlock();
                SubmitTunnelSendOp(service_, mpptunnel_);
            }
            else
            {
                notifyReady();
            }
        }
    }
    else if (state_ == ERR_HANDLE)
    {
        state_ = FINISH;
        WriteDone(status4err, false);
    }
    else
    {
        GPR_ASSERT(state_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (EstablishCallData).
        delete this;
        return;
    }
}

void EstablishCallData::attachQueue(MPMCQueue<std::shared_ptr<mpp::MPPDataPacket>> * send_queue)
{
    this->send_queue_ = send_queue;
}

void EstablishCallData::attachTunnel(const std::shared_ptr<DB::MPPTunnel> & mpptunnel)
{
    this->mpptunnel_ = mpptunnel;
}

} // namespace DB
