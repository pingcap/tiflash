// Copyright 2023 PingCAP, Ltd.
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

#include <Common/CPUAffinityManager.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadMetricUtil.h>
#include <Common/TiFlashMetrics.h>
#include <Common/VariantOp.h>
#include <Common/getNumberOfCPUCores.h>
#include <Common/setThreadName.h>
#include <Debug/MockStorage.h>
#include <Flash/BatchCoprocessorHandler.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Disaggregated/S3LockService.h>
#include <Flash/Disaggregated/WNEstablishDisaggTaskHandler.h>
#include <Flash/Disaggregated/WNFetchPagesStreamWriter.h>
#include <Flash/EstablishCall.h>
#include <Flash/FlashService.h>
#include <Flash/Management/ManualCompact.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/Utils.h>
#include <Flash/ServiceUtils.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Interpreters/executeQuery.h>
#include <Server/IServer.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/IManageableStorage.h>
#include <Storages/S3/S3Common.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/status_code_enum.h>
#include <kvproto/disaggregated.pb.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_EXCEPTION;
extern const int DISAGG_ESTABLISH_RETRYABLE_ERROR;
} // namespace ErrorCodes

#define CATCH_FLASHSERVICE_EXCEPTION                                                                                                        \
    catch (Exception & e)                                                                                                                   \
    {                                                                                                                                       \
        LOG_ERROR(log, "DB Exception: {}", e.message());                                                                                    \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message())); \
    }                                                                                                                                       \
    catch (const std::exception & e)                                                                                                        \
    {                                                                                                                                       \
        LOG_ERROR(log, "std exception: {}", e.what());                                                                                      \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(grpc::StatusCode::INTERNAL, e.what()));                    \
    }                                                                                                                                       \
    catch (...)                                                                                                                             \
    {                                                                                                                                       \
        LOG_ERROR(log, "other exception");                                                                                                  \
        return std::make_tuple(std::make_shared<Context>(*context), grpc::Status(grpc::StatusCode::INTERNAL, "other exception"));           \
    }

constexpr char tls_err_msg[] = "common name check is failed";

FlashService::FlashService() = default;

void FlashService::init(Context & context_)
{
    context = &context_;
    log = Logger::get("FlashService");
    manual_compact_manager = std::make_unique<Management::ManualCompactManager>(
        context->getGlobalContext(),
        context->getGlobalContext().getSettingsRef());

    // Only when the s3 storage is enabled on write node, provide the lock service interfaces
    if (!context->getSharedContextDisagg()->isDisaggregatedComputeMode() && S3::ClientFactory::instance().isEnabled())
        s3_lock_service = std::make_unique<S3::S3LockService>(*context);

    auto settings = context->getSettingsRef();
    enable_local_tunnel = settings.enable_local_tunnel;
    enable_async_grpc_client = settings.enable_async_grpc_client;
    const size_t default_size = getNumberOfLogicalCPUCores();

    auto cop_pool_size = static_cast<size_t>(settings.cop_pool_size);
    cop_pool_size = cop_pool_size ? cop_pool_size : default_size;
    LOG_INFO(log, "Use a thread pool with {} threads to handle cop requests.", cop_pool_size);
    cop_pool = std::make_unique<legacy::ThreadPool>(cop_pool_size, [] { setThreadName("cop-pool"); });

    auto batch_cop_pool_size = static_cast<size_t>(settings.batch_cop_pool_size);
    batch_cop_pool_size = batch_cop_pool_size ? batch_cop_pool_size : default_size;
    LOG_INFO(log, "Use a thread pool with {} threads to handle batch cop requests.", batch_cop_pool_size);
    batch_cop_pool = std::make_unique<legacy::ThreadPool>(batch_cop_pool_size, [] { setThreadName("batch-cop-pool"); });
}

FlashService::~FlashService() = default;

namespace
{
// Use executeInThreadPool to submit job to thread pool which return grpc::Status.
grpc::Status executeInThreadPool(legacy::ThreadPool & pool, std::function<grpc::Status()> job)
{
    std::packaged_task<grpc::Status()> task(job);
    std::future<grpc::Status> future = task.get_future();
    pool.schedule([&task] { task(); });
    return future.get();
}

String getClientMetaVarWithDefault(const grpc::ServerContext * grpc_context, const String & name, const String & default_val)
{
    if (auto it = grpc_context->client_metadata().find(name); it != grpc_context->client_metadata().end())
        return String(it->second.data(), it->second.size());

    return default_val;
}

void updateSettingsFromTiDB(const grpc::ServerContext * grpc_context, ContextPtr & context, LoggerPtr log)
{
    const static std::vector<std::pair<String, String>> tidb_varname_to_tiflash_varname = {
        std::make_pair("tidb_max_tiflash_threads", "max_threads"),
        std::make_pair("tidb_max_bytes_before_tiflash_external_join", "max_bytes_before_external_join"),
        std::make_pair("tidb_max_bytes_before_tiflash_external_group_by", "max_bytes_before_external_group_by"),
        std::make_pair("tidb_max_bytes_before_tiflash_external_sort", "max_bytes_before_external_sort"),
    };
    for (const auto & names : tidb_varname_to_tiflash_varname)
    {
        String value_from_tidb = getClientMetaVarWithDefault(grpc_context, names.first, "");
        if (!value_from_tidb.empty())
        {
            context->setSetting(names.second, value_from_tidb);
            LOG_DEBUG(log, "set context setting {} to {}", names.second, value_from_tidb);
        }
    }
}
} // namespace

grpc::Status FlashService::Coprocessor(
    grpc::ServerContext * grpc_context,
    const coprocessor::Request * request,
    coprocessor::Response * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    bool is_remote_read = getClientMetaVarWithDefault(grpc_context, "is_remote_read", "") == "true";
    auto log_level = is_remote_read ? Poco::Message::PRIO_INFORMATION : Poco::Message::PRIO_DEBUG;
    LOG_IMPL(log, log_level, "Handling coprocessor request, is_remote_read: {}, start ts: {}, region info: {}, region epoch: {}", is_remote_read, request->start_ts(), request->context().region_id(), request->context().region_epoch().DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    GET_METRIC(tiflash_coprocessor_request_count, type_cop).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Increment();
    if (is_remote_read)
    {
        GET_METRIC(tiflash_coprocessor_request_count, type_remote_read).Increment();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read).Increment();
    }
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_cop).Increment(response->ByteSizeLong());
        if (is_remote_read)
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read).Decrement();
    });

    context->setMockStorage(mock_storage);

    const auto & settings = context->getSettingsRef();
    auto handle_limit = settings.cop_pool_handle_limit != 0 ? settings.cop_pool_handle_limit.get() : 10 * cop_pool->size();
    auto max_queued_duration_ms = std::min(settings.cop_pool_max_queued_seconds, 20) * 1000;

    if (handle_limit > 0)
    {
        // We use this atomic variable metrics from the prometheus-cpp library to mark the number of queued queries.
        // TODO: Use grpc asynchronous server and a more fully-featured thread pool.
        if (auto current = GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Value(); current > handle_limit)
        {
            response->mutable_region_error()->mutable_server_is_busy()->set_reason(fmt::format("tiflash cop pool queued too much, current = {}, limit = {}", current, handle_limit));
            return grpc::Status::OK;
        }
    }


    grpc::Status ret = executeInThreadPool(*cop_pool, [&] {
        auto wait_ms = watch.elapsedMilliseconds();
        if (max_queued_duration_ms > 0)
        {
            if (wait_ms > static_cast<UInt64>(max_queued_duration_ms))
            {
                response->mutable_region_error()->mutable_server_is_busy()->set_reason(fmt::format("this task queued in tiflash cop pool too long, current = {} ms, limit = {} ms", wait_ms, max_queued_duration_ms));
                return grpc::Status::OK;
            }
        }
        if (wait_ms > 1000)
            log_level = Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(log, log_level, "Begin process cop request after wait {} ms, start ts: {}, region info: {}, region epoch: {}", wait_ms, request->start_ts(), request->context().region_id(), request->context().region_epoch().DebugString());
        auto [db_context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        if (is_remote_read)
            GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read_executing).Increment();
        SCOPE_EXIT({
            if (is_remote_read)
                GET_METRIC(tiflash_coprocessor_handling_request_count, type_remote_read_executing).Decrement();
        });
        CoprocessorContext cop_context(*db_context, request->context(), *grpc_context);
        CoprocessorHandler cop_handler(cop_context, request, response);
        return cop_handler.execute();
    });

    LOG_IMPL(log, log_level, "Handle coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

grpc::Status FlashService::BatchCoprocessor(grpc::ServerContext * grpc_context, const coprocessor::BatchRequest * request, grpc::ServerWriter<coprocessor::BatchResponse> * writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_INFO(log, "Handling batch coprocessor request, start ts: {}", request->start_ts());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    GET_METRIC(tiflash_coprocessor_request_count, type_batch).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_batch).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_batch).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    grpc::Status ret = executeInThreadPool(*batch_cop_pool, [&] {
        auto wait_ms = watch.elapsedMilliseconds();
        LOG_INFO(log, "Begin process batch cop request after wait {} ms, start ts: {}", wait_ms, request->start_ts());
        auto [db_context, status] = createDBContext(grpc_context);
        if (!status.ok())
        {
            return status;
        }
        CoprocessorContext cop_context(*db_context, request->context(), *grpc_context);
        BatchCoprocessorHandler cop_handler(cop_context, request, writer);
        return cop_handler.execute();
    });

    LOG_INFO(log, "Handle batch coprocessor request done: {}, {}", ret.error_code(), ret.error_message());
    return ret;
}

grpc::Status FlashService::DispatchMPPTask(
    grpc::ServerContext * grpc_context,
    const mpp::DispatchTaskRequest * request,
    mpp::DispatchTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_INFO(log, "Handling mpp dispatch request, task meta: {}", request->meta().DebugString());
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    // DO NOT register mpp task and return grpc error
    if (auto mpp_version = request->meta().mpp_version(); !DB::CheckMppVersion(mpp_version))
    {
        auto && err_msg = fmt::format("Failed to handling mpp dispatch request, reason=`{}`", DB::GenMppVersionErrorMessage(mpp_version));
        LOG_WARNING(log, err_msg);
        return grpc::Status(grpc::StatusCode::CANCELLED, std::move(err_msg));
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }

    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_dispatch_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_dispatch_mpp_task).Increment(response->ByteSizeLong());
    });

    auto [db_context, status] = createDBContext(grpc_context);
    if (!status.ok())
    {
        return status;
    }
    db_context->setMockStorage(mock_storage);
    db_context->setMockMPPServerInfo(mpp_test_info);

    MPPHandler mpp_handler(*request);
    return mpp_handler.execute(db_context, response);
}

grpc::Status FlashService::IsAlive(grpc::ServerContext * grpc_context [[maybe_unused]],
                                   const mpp::IsAliveRequest * request [[maybe_unused]],
                                   mpp::IsAliveResponse * response [[maybe_unused]])
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    auto & tmt_context = context->getTMTContext();
    response->set_available(tmt_context.checkRunning());
    response->set_mpp_version(DB::GetMppVersion());
    return grpc::Status::OK;
}

static grpc::Status CheckMppVersionForEstablishMPPConnection(const mpp::EstablishMPPConnectionRequest * request)
{
    const auto & sender_mpp_version = request->sender_meta().mpp_version();
    const auto & receiver_mpp_version = request->receiver_meta().mpp_version();

    std::string && err_reason{};

    if (!DB::CheckMppVersion(sender_mpp_version))
    {
        err_reason += fmt::format("sender failed: {}; ", DB::GenMppVersionErrorMessage(sender_mpp_version));
    }
    if (!DB::CheckMppVersion(receiver_mpp_version))
    {
        err_reason += fmt::format("receiver failed: {}; ", DB::GenMppVersionErrorMessage(receiver_mpp_version));
    }

    if (!err_reason.empty())
    {
        auto && err_msg = fmt::format("Failed to establish MPP connection, reason=`{}`", err_reason);
        return grpc::Status(grpc::StatusCode::INTERNAL, std::move(err_msg));
    }
    return grpc::Status::OK;
}

grpc::Status AsyncFlashService::establishMPPConnectionAsync(EstablishCallData * call_data)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes have registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    const auto & request = call_data->getRequest();
    LOG_INFO(log, "Handling establish mpp connection request: {}", request.DebugString());

    auto check_result = checkGrpcContext(call_data->getGrpcContext());
    if (!check_result.ok())
        return check_result;

    if (auto res = CheckMppVersionForEstablishMPPConnection(&request); !res.ok())
    {
        LOG_WARNING(log, res.error_message());
        return res;
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();

    call_data->startEstablishConnection();
    call_data->tryConnectTunnel();
    return grpc::Status::OK;
}

grpc::Status FlashService::EstablishMPPConnection(grpc::ServerContext * grpc_context, const mpp::EstablishMPPConnectionRequest * request, grpc::ServerWriter<mpp::MPPDataPacket> * sync_writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // Establish a pipe for data transferring. The pipes have registered by the task in advance.
    // We need to find it out and bind the grpc stream with it.
    LOG_INFO(log, "Handling establish mpp connection request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    if (auto res = CheckMppVersionForEstablishMPPConnection(request); !res.ok())
    {
        LOG_WARNING(log, res.error_message());
        return res;
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();
    if (!tryToResetMaxThreadsMetrics())
    {
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value()));
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));
    }
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(watch.elapsedSeconds());
        // TODO: update the value of metric tiflash_coprocessor_response_bytes.
    });

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    std::chrono::seconds timeout(10);
    auto [tunnel, err_msg] = task_manager->findTunnelWithTimeout(request, timeout);
    if (tunnel == nullptr)
    {
        if (!sync_writer->Write(getPacketWithError(err_msg)))
        {
            LOG_DEBUG(log, "Write error message failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write error message failed for unknown reason.");
        }
    }
    else
    {
        Stopwatch stopwatch;
        SyncPacketWriter writer(sync_writer);
        tunnel->connectSync(&writer);
        tunnel->waitForFinish();
        LOG_INFO(tunnel->getLogger(), "connection for {} cost {} ms.", tunnel->id(), stopwatch.elapsedMilliseconds());
    }
    return grpc::Status::OK;
}

grpc::Status FlashService::CancelMPPTask(
    grpc::ServerContext * grpc_context,
    const mpp::CancelTaskRequest * request,
    mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_INFO(log, "cancel mpp task request: {}", request->DebugString());

    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    if (auto mpp_version = request->meta().mpp_version(); !DB::CheckMppVersion(mpp_version))
    {
        auto && err_msg = fmt::format("Failed to cancel mpp task, reason=`{}`", DB::GenMppVersionErrorMessage(mpp_version));
        LOG_WARNING(log, err_msg);
        return grpc::Status(grpc::StatusCode::INTERNAL, std::move(err_msg));
    }

    GET_METRIC(tiflash_coprocessor_request_count, type_cancel_mpp_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cancel_mpp_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_cancel_mpp_task).Increment(response->ByteSizeLong());
    });

    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->abortMPPQuery(MPPQueryId(request->meta()), "Receive cancel request from TiDB", AbortType::ONCANCELLATION);
    return grpc::Status::OK;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContextForTest() const
{
    try
    {
        /// Create DB context.
        auto tmp_context = std::make_shared<Context>(*context);
        tmp_context->setGlobalContext(*context);

        String query_id;
        tmp_context->setCurrentQueryId(query_id);
        ClientInfo & client_info = tmp_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        String max_threads;
        tmp_context->setSetting("enable_async_server", is_async ? "true" : "false");
        tmp_context->setSetting("enable_local_tunnel", enable_local_tunnel ? "true" : "false");
        tmp_context->setSetting("enable_async_grpc_client", enable_async_grpc_client ? "true" : "false");
        return std::make_tuple(tmp_context, grpc::Status::OK);
    }
    CATCH_FLASHSERVICE_EXCEPTION
}

::grpc::Status FlashService::cancelMPPTaskForTest(const ::mpp::CancelTaskRequest * request, ::mpp::CancelTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    // CancelMPPTask cancels the query of the task.
    LOG_INFO(log, "cancel mpp task request: {}", request->DebugString());
    auto [context, status] = createDBContextForTest();
    if (!status.ok())
    {
        auto err = std::make_unique<mpp::Error>();
        err->set_mpp_version(DB::GetMppVersion());
        err->set_msg("error status");
        response->set_allocated_error(err.release());
        return status;
    }
    auto & tmt_context = context->getTMTContext();
    auto task_manager = tmt_context.getMPPTaskManager();
    task_manager->abortMPPQuery(MPPQueryId(request->meta()), "Receive cancel request from GTest", AbortType::ONCANCELLATION);
    return grpc::Status::OK;
}

grpc::Status FlashService::checkGrpcContext(const grpc::ServerContext * grpc_context) const
{
    // For coprocessor/mpp test, we don't care about security config.
    if (likely(!context->isMPPTest() && !context->isCopTest()))
    {
        if (!context->getSecurityConfig()->checkGrpcContext(grpc_context))
        {
            return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg);
        }
    }
    std::string peer = grpc_context->peer();
    Int64 pos = peer.find(':');
    if (pos == -1)
    {
        return grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid peer address: " + peer);
    }
    return grpc::Status::OK;
}

std::tuple<ContextPtr, grpc::Status> FlashService::createDBContext(const grpc::ServerContext * grpc_context) const
{
    try
    {
        /// Create DB context.
        auto tmp_context = std::make_shared<Context>(*context);
        tmp_context->setGlobalContext(*context);

        /// Set a bunch of client information.
        std::string user = getClientMetaVarWithDefault(grpc_context, "user", "default");
        std::string password = getClientMetaVarWithDefault(grpc_context, "password", "");
        std::string quota_key = getClientMetaVarWithDefault(grpc_context, "quota_key", "");
        std::string peer = grpc_context->peer();
        Int64 pos = peer.find(':');
        std::string client_ip = peer.substr(pos + 1);
        Poco::Net::SocketAddress client_address(client_ip);

        // For MPP or Cop test, we don't care about security config.
        if (likely(!context->isTest()))
            tmp_context->setUser(user, password, client_address, quota_key);

        String query_id = getClientMetaVarWithDefault(grpc_context, "query_id", "");
        tmp_context->setCurrentQueryId(query_id);

        ClientInfo & client_info = tmp_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        /// Set DAG parameters.
        std::string dag_records_per_chunk_str = getClientMetaVarWithDefault(grpc_context, "dag_records_per_chunk", "");
        if (!dag_records_per_chunk_str.empty())
        {
            tmp_context->setSetting("dag_records_per_chunk", dag_records_per_chunk_str);
        }

        updateSettingsFromTiDB(grpc_context, tmp_context, log);

        tmp_context->setSetting("enable_async_server", is_async ? "true" : "false");
        tmp_context->setSetting("enable_local_tunnel", enable_local_tunnel ? "true" : "false");
        tmp_context->setSetting("enable_async_grpc_client", enable_async_grpc_client ? "true" : "false");
        return std::make_tuple(tmp_context, grpc::Status::OK);
    }
    CATCH_FLASHSERVICE_EXCEPTION
}

grpc::Status FlashService::Compact(grpc::ServerContext * grpc_context, const kvrpcpb::CompactRequest * request, kvrpcpb::CompactResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    return manual_compact_manager->handleRequest(request, response);
}

grpc::Status FlashService::tryAddLock(grpc::ServerContext * grpc_context, const disaggregated::TryAddLockRequest * request, disaggregated::TryAddLockResponse * response)
{
    if (!s3_lock_service)
    {
        return grpc::Status(::grpc::StatusCode::INTERNAL,
                            fmt::format(
                                "can not handle tryAddLock, s3enabled={} compute_node={}",
                                S3::ClientFactory::instance().isEnabled(),
                                context->getSharedContextDisagg()->isDisaggregatedComputeMode()));
    }

    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    return s3_lock_service->tryAddLock(request, response);
}

grpc::Status FlashService::tryMarkDelete(grpc::ServerContext * grpc_context, const disaggregated::TryMarkDeleteRequest * request, disaggregated::TryMarkDeleteResponse * response)
{
    if (!s3_lock_service)
    {
        return grpc::Status(::grpc::StatusCode::INTERNAL,
                            fmt::format(
                                "can not handle tryMarkDelete, s3enabled={} compute_node={}",
                                S3::ClientFactory::instance().isEnabled(),
                                context->getSharedContextDisagg()->isDisaggregatedComputeMode()));
    }

    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    return s3_lock_service->tryMarkDelete(request, response);
}

grpc::Status FlashService::EstablishDisaggTask(grpc::ServerContext * grpc_context, const disaggregated::EstablishDisaggTaskRequest * request, disaggregated::EstablishDisaggTaskResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_DEBUG(log, "Handling EstablishDisaggTask request: {}", request->ShortDebugString());
    if (auto check_result = checkGrpcContext(grpc_context); !check_result.ok())
        return check_result;

    GET_METRIC(tiflash_coprocessor_request_count, type_disagg_establish_task).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_disagg_establish_task).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_disagg_establish_task).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_disagg_establish_task).Observe(watch.elapsedSeconds());
        GET_METRIC(tiflash_coprocessor_response_bytes, type_disagg_establish_task).Increment(response->ByteSizeLong());
    });

    auto [db_context, status] = createDBContext(grpc_context);
    if (!status.ok())
        return status;
    db_context->setMockStorage(mock_storage);
    db_context->setMockMPPServerInfo(mpp_test_info);

    RUNTIME_CHECK_MSG(context->getSharedContextDisagg()->isDisaggregatedStorageMode(), "EstablishDisaggTask should only be called on write node");

    const auto & meta = request->meta();
    DM::DisaggTaskId task_id(meta);
    auto logger = Logger::get(task_id);

    auto handler = std::make_shared<WNEstablishDisaggTaskHandler>(db_context, task_id);
    SCOPE_EXIT({
        current_memory_tracker = nullptr;
    });

    auto record_other_error = [&](int flash_err_code, const String & err_msg) {
        // Note: We intentinally do not remove the snapshot from the SnapshotManager
        // when this request is failed. Consider this case:
        // EstablishDisagg for A: ---------------- Failed --------------------------------------------- Cleanup Snapshot for A
        // EstablishDisagg for B: - Failed - RN retry EstablishDisagg for A+B -- InsertSnapshot for A+B ----- FetchPages (Boom!)
        auto * err = response->mutable_error()->mutable_error_other();
        err->set_code(flash_err_code);
        err->set_msg(err_msg);
    };

    try
    {
        handler->prepare(request);
        handler->execute(response);
    }
    catch (const RegionException & e)
    {
        LOG_INFO(logger, "EstablishDisaggTask meet RegionException {} (retryable), regions={}", e.message(), e.unavailable_region);

        auto * error = response->mutable_error()->mutable_error_region();
        error->set_msg(e.message());
        for (const auto & region_id : e.unavailable_region)
            error->add_region_ids(region_id);
    }
    catch (const LockException & e)
    {
        LOG_INFO(logger, "EstablishDisaggTask meet LockException: {} (retryable)", e.message());

        auto * error = response->mutable_error()->mutable_error_locked();
        error->set_msg(e.message());
        for (const auto & lock : e.locks)
            error->add_locked()->CopyFrom(*lock.second);
    }
    catch (Exception & e)
    {
        LOG_ERROR(logger, "EstablishDisaggTask meet exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        record_other_error(e.code(), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(logger, "EstablishDisaggTask meet KV Client Exception: {}", e.message());
        record_other_error(e.code(), e.message());
    }
    catch (std::exception & e)
    {
        LOG_ERROR(logger, "EstablishDisaggTask meet std::exception: {}", e.what());
        record_other_error(ErrorCodes::UNKNOWN_EXCEPTION, e.what());
    }
    catch (...)
    {
        LOG_ERROR(logger, "EstablishDisaggTask meet unknown exception");
        record_other_error(ErrorCodes::UNKNOWN_EXCEPTION, "other exception");
    }

    LOG_DEBUG(
        logger,
        "Handle EstablishDisaggTask request done, resp_err={}",
        response->has_error() ? response->error().ShortDebugString() : "(null)");

    // The response is send back to TiFlash. Always assign gRPC::Status::OK, so that TiFlash response
    // handlers can deal with the embedded error correctly.
    return grpc::Status::OK;
}

grpc::Status FlashService::FetchDisaggPages(
    grpc::ServerContext * grpc_context,
    const disaggregated::FetchDisaggPagesRequest * request,
    grpc::ServerWriter<disaggregated::PagesPacket> * sync_writer)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    LOG_DEBUG(log, "Handling FetchDisaggPages request: {}", request->ShortDebugString());
    if (auto check_result = checkGrpcContext(grpc_context); !check_result.ok())
        return check_result;

    auto record_error = [&](grpc::StatusCode err_code, const String & err_msg) {
        disaggregated::PagesPacket err_response;
        auto * err = err_response.mutable_error();
        err->set_code(ErrorCodes::UNKNOWN_EXCEPTION);
        err->set_msg(err_msg);
        sync_writer->Write(err_response);
        return grpc::Status(err_code, err_msg);
    };

    RUNTIME_CHECK_MSG(context->getSharedContextDisagg()->isDisaggregatedStorageMode(), "FetchDisaggPages should only be called on write node");

    GET_METRIC(tiflash_coprocessor_request_count, type_disagg_fetch_pages).Increment();
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_disagg_fetch_pages).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_disagg_fetch_pages).Decrement();
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_disagg_fetch_pages).Observe(watch.elapsedSeconds());
    });

    auto snaps = context->getSharedContextDisagg()->wn_snapshot_manager;
    const DM::DisaggTaskId task_id(request->snapshot_id());
    // get the keyspace from meta, it is now used for debugging
    const auto keyspace_id = RequestUtils::deriveKeyspaceID(request->snapshot_id());
    auto logger = Logger::get(task_id);

    LOG_DEBUG(logger, "Fetching pages, keyspace_id={} table_id={} segment_id={} num_fetch={}", keyspace_id, request->table_id(), request->segment_id(), request->page_ids_size());

    SCOPE_EXIT({
        // The snapshot is created in the 1st request (Establish), and will be destroyed when all FetchPages are finished.
        snaps->unregisterSnapshotIfEmpty(task_id);
    });

    try
    {
        auto snap = snaps->getSnapshot(task_id);
        RUNTIME_CHECK_MSG(snap != nullptr, "Can not find disaggregated task, task_id={}", task_id);
        auto task = snap->popSegTask(request->table_id(), request->segment_id());
        RUNTIME_CHECK(task.isValid(), task.err_msg);

        PageIdU64s read_ids;
        read_ids.reserve(request->page_ids_size());
        for (auto page_id : request->page_ids())
            read_ids.emplace_back(page_id);

        auto stream_writer = WNFetchPagesStreamWriter::build(task, read_ids);
        stream_writer->pipeTo(sync_writer);
        stream_writer.reset();

        LOG_DEBUG(logger, "FetchDisaggPages respond finished, task_id={}", task_id);
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(logger, "FetchDisaggPages meet TiFlashException: {}\n{}", e.displayText(), e.getStackTrace().toString());
        return record_error(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "FetchDisaggPages meet exception: {}\n{}", e.message(), e.getStackTrace().toString());
        return record_error(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(logger, "FetchDisaggPages meet KV Client Exception: {}", e.message());
        return record_error(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(logger, "FetchDisaggPages meet std::exception: {}", e.what());
        return record_error(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(logger, "FetchDisaggPages meet unknown exception");
        return record_error(grpc::StatusCode::INTERNAL, "other exception");
    }
}

grpc::Status FlashService::GetDisaggConfig(grpc::ServerContext * grpc_context, const disaggregated::GetDisaggConfigRequest *, disaggregated::GetDisaggConfigResponse * response)
{
    if (!context->getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        return grpc::Status(
            grpc::StatusCode::UNIMPLEMENTED,
            fmt::format(
                "can not handle GetDisaggConfig with mode={}",
                magic_enum::enum_name(context->getSharedContextDisagg()->disaggregated_mode)));
    }

    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    const auto local_s3config = S3::ClientFactory::instance().getConfigCopy();
    auto * s3_config = response->mutable_s3_config();
    s3_config->set_endpoint(local_s3config.endpoint);
    s3_config->set_bucket(local_s3config.bucket);
    s3_config->set_root(local_s3config.root);

    return grpc::Status::OK;
}

grpc::Status FlashService::GetTiFlashSystemTable(
    grpc::ServerContext * grpc_context,
    const kvrpcpb::TiFlashSystemTableRequest * request,
    kvrpcpb::TiFlashSystemTableResponse * response)
{
    CPUAffinityManager::getInstance().bindSelfGrpcThread();
    auto check_result = checkGrpcContext(grpc_context);
    if (!check_result.ok())
        return check_result;

    try
    {
        auto [ctx, status] = createDBContext(grpc_context);
        if (!status.ok())
            return status;
        ctx->setDefaultFormat("JSONCompact");
        ReadBufferFromString in_buf(request->sql());
        MemoryWriteBuffer out_buf;
        executeQuery(in_buf, out_buf, false, *ctx, nullptr);
        auto data_size = out_buf.count();
        auto buf = out_buf.tryGetReadBuffer();
        String data;
        data.resize(data_size);
        buf->readStrict(&data[0], data_size);
        response->set_data(data);
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        return grpc::Status(grpc::StatusCode::INTERNAL, "other exception");
    }
    return grpc::Status::OK;
}

void FlashService::setMockStorage(MockStorage * mock_storage_)
{
    mock_storage = mock_storage_;
}

void FlashService::setMockMPPServerInfo(MockMPPServerInfo & mpp_test_info_)
{
    mpp_test_info = mpp_test_info_;
}
} // namespace DB
