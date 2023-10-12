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

#include <Common/Limiter.h>
#include <Debug/MockServerInfo.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/server_context.h>
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class IServer;
class IAsyncCallData;
class EstablishCallData;
class MockStorage;
class Context;
using ContextPtr = std::shared_ptr<Context>;
using MockMPPServerInfo = tests::MockMPPServerInfo;

namespace Management
{
class ManualCompactManager;
} // namespace Management
namespace S3
{
class S3LockService;
} // namespace S3

class FlashService
    : public tikvpb::Tikv::Service
    , public std::enable_shared_from_this<FlashService>
    , private boost::noncopyable
{
public:
    FlashService();
    void init(Context & context_);

    ~FlashService() override;

    grpc::Status Coprocessor(
        grpc::ServerContext * grpc_context,
        const coprocessor::Request * request,
        coprocessor::Response * response) override;

    grpc::Status BatchCoprocessor(
        grpc::ServerContext * context,
        const coprocessor::BatchRequest * request,
        grpc::ServerWriter<coprocessor::BatchResponse> * writer) override;

    grpc::Status CoprocessorStream(
        grpc::ServerContext * context,
        const coprocessor::Request * request,
        grpc::ServerWriter<coprocessor::Response> * writer) override;

    grpc::Status DispatchMPPTask(
        grpc::ServerContext * context,
        const mpp::DispatchTaskRequest * request,
        mpp::DispatchTaskResponse * response) override;

    grpc::Status IsAlive(
        grpc::ServerContext * context,
        const mpp::IsAliveRequest * request,
        mpp::IsAliveResponse * response) override;

    grpc::Status EstablishMPPConnection(
        grpc::ServerContext * grpc_context,
        const mpp::EstablishMPPConnectionRequest * request,
        grpc::ServerWriter<mpp::MPPDataPacket> * sync_writer) override;

    grpc::Status CancelMPPTask(
        grpc::ServerContext * context,
        const mpp::CancelTaskRequest * request,
        mpp::CancelTaskResponse * response) override;
    grpc::Status cancelMPPTaskForTest(const mpp::CancelTaskRequest * request, mpp::CancelTaskResponse * response);

    grpc::Status Compact(
        grpc::ServerContext * grpc_context,
        const kvrpcpb::CompactRequest * request,
        kvrpcpb::CompactResponse * response) override;


    // For S3 Lock Service
    grpc::Status tryAddLock(
        grpc::ServerContext * grpc_context,
        const disaggregated::TryAddLockRequest * request,
        disaggregated::TryAddLockResponse * response) override;
    grpc::Status tryMarkDelete(
        grpc::ServerContext * grpc_context,
        const disaggregated::TryMarkDeleteRequest * request,
        disaggregated::TryMarkDeleteResponse * response) override;

    // The TiFlash read node call this RPC to build the disaggregated task
    // on the TiFlash write node.
    // It returns the serialized remote segments info to the compute node.
    grpc::Status EstablishDisaggTask(
        grpc::ServerContext * grpc_context,
        const disaggregated::EstablishDisaggTaskRequest * request,
        disaggregated::EstablishDisaggTaskResponse * response) override;
    // The TiFlash read node call this RPC to fetch the delta-layer data
    // from the TiFlash write node.
    grpc::Status FetchDisaggPages(
        grpc::ServerContext * grpc_context,
        const disaggregated::FetchDisaggPagesRequest * request,
        grpc::ServerWriter<disaggregated::PagesPacket> * sync_writer) override;

    grpc::Status GetDisaggConfig(
        grpc::ServerContext * grpc_context,
        const disaggregated::GetDisaggConfigRequest * request,
        disaggregated::GetDisaggConfigResponse * response) override;
    grpc::Status GetTiFlashSystemTable(
        grpc::ServerContext * grpc_context,
        const kvrpcpb::TiFlashSystemTableRequest * request,
        kvrpcpb::TiFlashSystemTableResponse * response) override;

    void setMockStorage(MockStorage * mock_storage_);
    void setMockMPPServerInfo(MockMPPServerInfo & mpp_test_info_);
    Context * getContext() { return context; }

protected:
    std::tuple<ContextPtr, grpc::Status> createDBContextForTest() const;
    std::tuple<ContextPtr, grpc::Status> createDBContext(const grpc::ServerContext * grpc_context) const;
    grpc::Status checkGrpcContext(const grpc::ServerContext * grpc_context) const;

    Context * context = nullptr;
    LoggerPtr log;
    bool is_async = false;
    bool enable_local_tunnel = false;
    bool enable_async_grpc_client = false;

    std::unique_ptr<Management::ManualCompactManager> manual_compact_manager;
    std::unique_ptr<S3::S3LockService> s3_lock_service;

    /// for mpp unit test.
    MockStorage * mock_storage = nullptr;
    MockMPPServerInfo mpp_test_info{};

    // Put Limiter member(s) at the end so that ensure it will be destroyed firstly.
    std::unique_ptr<Limiter<grpc::Status>> cop_limiter, cop_stream_limiter, batch_cop_limiter;
};

class AsyncFlashService final : public tikvpb::Tikv::WithAsyncMethod_EstablishMPPConnection<FlashService>
{
public:
    AsyncFlashService() { is_async = true; }
    /// Return grpc::Status::OK when the connection is established.
    /// Return non-OK grpc::Status when the connection can not be established.
    grpc::Status establishMPPConnectionAsync(EstablishCallData * call_data);
};
} // namespace DB
