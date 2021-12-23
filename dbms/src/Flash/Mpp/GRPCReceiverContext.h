#pragma once

#include <Common/UnaryCallback.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <common/types.h>
#include <grpc++/grpc++.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Cluster.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
using MPPDataPacket = mpp::MPPDataPacket;
using MPPDataPacketPtr = std::shared_ptr<MPPDataPacket>;
using MPPDataPacketPtrs = std::vector<MPPDataPacketPtr>;

class ExchangePacketReader
{
public:
    virtual ~ExchangePacketReader() = default;
    virtual bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const = 0;
    virtual ::grpc::Status finish() const = 0;
};

class AsyncExchangePacketReader
{
public:
    virtual ~AsyncExchangePacketReader() = default;
    virtual void batchRead(MPPDataPacketPtrs & packets, UnaryCallback<size_t> * callback) const = 0;
    virtual void finish(UnaryCallback<::grpc::Status> * callback) const = 0;
};

struct ExchangeRecvRequest
{
    Int64 source_index = -1;
    Int64 send_task_id = -2; //Do not use -1 as default, since -1 has special meaning to show it's the root sender from the TiDB.
    Int64 recv_task_id = -2;
    std::shared_ptr<mpp::EstablishMPPConnectionRequest> req;
    bool is_local = false;

    String debugString() const;
};

class GRPCReceiverContext
{
public:
    using Status = ::grpc::Status;
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

    bool supportAsync(const ExchangeRecvRequest & request);

    std::shared_ptr<ExchangePacketReader> makeReader(const ExchangeRecvRequest & request) const;

    void makeAsyncReader(const ExchangeRecvRequest & request, UnaryCallback<std::shared_ptr<AsyncExchangePacketReader>> * callback) const;

    static Status getStatusOK()
    {
        return ::grpc::Status::OK;
    }

    void fillSchema(DAGSchema & schema) const;

private:
    tipb::ExchangeReceiver exchange_receiver_meta;
    mpp::TaskMeta task_meta;
    pingcap::kv::Cluster * cluster;
    std::shared_ptr<MPPTaskManager> task_manager;
    bool enable_local_tunnel;
    bool enable_async_grpc;
};
} // namespace DB
