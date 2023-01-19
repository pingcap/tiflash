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

#include <Common/TiFlashSecurity.h>
#include <Flash/EstablishCall.h>
#include <Interpreters/Context.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/tikvpb.grpc.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
class IServer;
class CallExecPool;
class EstablishCallData;
namespace Management
{
class ManualCompactManager;
} // namespace Management

class FlashService : public tikvpb::Tikv::Service
    , public std::enable_shared_from_this<FlashService>
    , private boost::noncopyable
{
public:
    explicit FlashService(IServer & server_);

    ~FlashService() override;

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context,
        const coprocessor::Request * request,
        coprocessor::Response * response) override;

    ::grpc::Status BatchCoprocessor(::grpc::ServerContext * context,
                                    const ::coprocessor::BatchRequest * request,
                                    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer) override;

    ::grpc::Status DispatchMPPTask(
        ::grpc::ServerContext * context,
        const ::mpp::DispatchTaskRequest * request,
        ::mpp::DispatchTaskResponse * response) override;

    ::grpc::Status IsAlive(
        ::grpc::ServerContext * context,
        const ::mpp::IsAliveRequest * request,
        ::mpp::IsAliveResponse * response) override;

    ::grpc::Status establishMPPConnectionSyncOrAsync(::grpc::ServerContext * context, const ::mpp::EstablishMPPConnectionRequest * request, ::grpc::ServerWriter<::mpp::MPPDataPacket> * sync_writer, EstablishCallData * calldata);

    ::grpc::Status EstablishMPPConnection(::grpc::ServerContext * context, const ::mpp::EstablishMPPConnectionRequest * request, ::grpc::ServerWriter<::mpp::MPPDataPacket> * sync_writer) override
    {
        return establishMPPConnectionSyncOrAsync(context, request, sync_writer, nullptr);
    }

    ::grpc::Status CancelMPPTask(::grpc::ServerContext * context, const ::mpp::CancelTaskRequest * request, ::mpp::CancelTaskResponse * response) override;

    ::grpc::Status Compact(::grpc::ServerContext * context, const ::kvrpcpb::CompactRequest * request, ::kvrpcpb::CompactResponse * response) override;

protected:
    std::tuple<ContextPtr, grpc::Status> createDBContext(const grpc::ServerContext * grpc_context) const;
    grpc::Status checkGrpcContext(const grpc::ServerContext * grpc_context) const;

    IServer & server;
    const TiFlashSecurityConfig & security_config;
    Poco::Logger * log;
    bool is_async = false;
    bool enable_local_tunnel = false;
    bool enable_async_grpc_client = false;

    std::unique_ptr<Management::ManualCompactManager> manual_compact_manager;

    // Put thread pool member(s) at the end so that ensure it will be destroyed firstly.
    std::unique_ptr<ThreadPool> cop_pool, batch_cop_pool;
};

// a copy of WithAsyncMethod_EstablishMPPConnection, since we want both sync & async server, we need copy it and inherit from FlashService.
class AsyncFlashService final : public FlashService
{
public:
    // 48 is EstablishMPPConnection API ID of GRPC
    // note: if the kvrpc protocal is updated, please keep consistent with the generated code.
    static constexpr int EstablishMPPConnectionApiID = 48;
    explicit AsyncFlashService(IServer & server)
        : FlashService(server)
    {
        is_async = true;
        ::grpc::Service::MarkMethodAsync(EstablishMPPConnectionApiID);
    }

    // disable synchronous version of this method
    ::grpc::Status EstablishMPPConnection(::grpc::ServerContext * /*context*/, const ::mpp::EstablishMPPConnectionRequest * /*request*/, ::grpc::ServerWriter<::mpp::MPPDataPacket> * /*writer*/) override
    {
        abort();
        return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void requestEstablishMPPConnection(::grpc::ServerContext * context, ::mpp::EstablishMPPConnectionRequest * request, ::grpc::ServerAsyncWriter<::mpp::MPPDataPacket> * writer, ::grpc::CompletionQueue * new_call_cq, ::grpc::ServerCompletionQueue * notification_cq, void * tag)
    {
        ::grpc::Service::RequestAsyncServerStreaming(EstablishMPPConnectionApiID, context, request, writer, new_call_cq, notification_cq, tag);
    }
};

} // namespace DB
