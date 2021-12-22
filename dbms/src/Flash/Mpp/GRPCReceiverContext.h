#pragma once

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
class ExchangePacketReader
{
public:
    virtual ~ExchangePacketReader() = default;
    virtual void initialize() const = 0;
    virtual bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const = 0;
    virtual ::grpc::Status finish() const = 0;
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

class GrpcExchangePacketReader : public ExchangePacketReader
{
public:
    std::shared_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> reader;

    explicit GrpcExchangePacketReader(const ExchangeRecvRequest & req);

    /// put the implementation of dtor in .cpp so we don't need to put the specialization of
    /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
    ~GrpcExchangePacketReader() override;

    void initialize() const override;
    bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const override;
    ::grpc::Status finish() const override;
};


class LocalExchangePacketReader : public ExchangePacketReader
{
public:
    MPPTunnelPtr tunnel;

    explicit LocalExchangePacketReader(const std::shared_ptr<MPPTunnel> & tunnel_)
        : tunnel(tunnel_){};

    /// put the implementation of dtor in .cpp so we don't need to put the specialization of
    /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
    ~LocalExchangePacketReader() override
    {
        if (tunnel)
        { // In case that ExchangeReceiver throw error before finish reading from mpptunnel
            tunnel->finishWithLock();
        }
    }

    void initialize() const override {}
    bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const override;
    ::grpc::Status finish() const override;
};

class GRPCReceiverContext
{
public:
    using StatusType = ::grpc::Status;
    using RequestType = ExchangeRecvRequest;

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

    static StatusType getStatusOK()
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
