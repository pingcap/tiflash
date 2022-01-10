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
    virtual bool read(MPPDataPacketPtr & packet) = 0;
    virtual ::grpc::Status finish() = 0;
};
using ExchangePacketReaderPtr = std::shared_ptr<ExchangePacketReader>;

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

    explicit GRPCReceiverContext(
        const tipb::ExchangeReceiver & exchange_receiver_meta_,
        const mpp::TaskMeta & task_meta_,
        pingcap::kv::Cluster * cluster_,
        std::shared_ptr<MPPTaskManager> task_manager_,
        bool enable_local_tunnel_);

    ExchangeRecvRequest makeRequest(int index) const;

    ExchangePacketReaderPtr makeReader(const ExchangeRecvRequest & request) const;

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
};
} // namespace DB
