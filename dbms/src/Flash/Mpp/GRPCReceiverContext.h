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

#include <Common/grpcpp.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Mpp/LocalRequestHandler.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <common/types.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Cluster.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
using MPPDataPacket = mpp::MPPDataPacket;
using TrackedMppDataPacketPtr = std::shared_ptr<DB::TrackedMppDataPacket>;
using TrackedMPPDataPacketPtrs = std::vector<TrackedMppDataPacketPtr>;

class ExchangePacketReader
{
public:
    virtual ~ExchangePacketReader() = default;
    virtual bool read(TrackedMppDataPacketPtr & packet) = 0;
    virtual grpc::Status finish() = 0;
    virtual void cancel(const String & reason) = 0;
};
using ExchangePacketReaderPtr = std::unique_ptr<ExchangePacketReader>;

class AsyncExchangePacketReader
{
public:
    virtual ~AsyncExchangePacketReader() = default;
    virtual void init(GRPCKickTag * tag) = 0;
    virtual void read(TrackedMppDataPacketPtr & packet, GRPCKickTag * tag) = 0;
    virtual void finish(::grpc::Status & status, GRPCKickTag * tag) = 0;
    virtual grpc::ClientContext * getClientContext() = 0;
};
using AsyncExchangePacketReaderPtr = std::unique_ptr<AsyncExchangePacketReader>;

struct ExchangeRecvRequest
{
    Int64 source_index = -1;
    Int64 send_task_id
        = -2; // Do not use -1 as default, since -1 has special meaning to show it's the root sender from the TiDB.
    Int64 recv_task_id = -2;
    mpp::EstablishMPPConnectionRequest req;
    bool is_local = false;

    String debugString() const;
};

class GRPCReceiverContext
{
public:
    using Status = grpc::Status;
    using Request = ExchangeRecvRequest;
    using Reader = ExchangePacketReader;
    using AsyncReader = AsyncExchangePacketReader;

    explicit GRPCReceiverContext(
        const tipb::ExchangeReceiver & exchange_receiver_meta_,
        const mpp::TaskMeta & task_meta_,
        pingcap::kv::Cluster * cluster_,
        std::shared_ptr<MPPTaskManager> task_manager_,
        bool enable_local_tunnel_,
        bool enable_async_grpc_);

    ExchangeRecvRequest makeRequest(int index) const;

    bool supportAsync(const ExchangeRecvRequest & request) const;

    ExchangePacketReaderPtr makeReader(const ExchangeRecvRequest & request) const;

    ExchangePacketReaderPtr makeSyncReader(const ExchangeRecvRequest & request) const;

    AsyncExchangePacketReaderPtr makeAsyncReader(
        const ExchangeRecvRequest & request,
        grpc::CompletionQueue * cq,
        GRPCKickTag * tag) const;

    static Status getStatusOK() { return grpc::Status::OK; }

    void fillSchema(DAGSchema & schema) const;

    void establishMPPConnectionLocalV2(
        const ExchangeRecvRequest & request,
        size_t source_index,
        LocalRequestHandler & local_request_handler,
        bool has_remote_conn);

    static std::tuple<MPPTunnelPtr, grpc::Status> establishMPPConnectionLocalV1(
        const ::mpp::EstablishMPPConnectionRequest * request,
        const std::shared_ptr<MPPTaskManager> & task_manager);

private:
    tipb::ExchangeReceiver exchange_receiver_meta;
    mpp::TaskMeta task_meta;
    pingcap::kv::Cluster * cluster;
    std::shared_ptr<MPPTaskManager> task_manager;
    bool enable_local_tunnel;
    bool enable_async_grpc;
};
} // namespace DB
