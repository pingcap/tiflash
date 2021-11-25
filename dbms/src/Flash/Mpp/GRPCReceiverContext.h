#pragma once

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
    virtual ~ExchangePacketReader() {}
    virtual void initialize() const = 0;
    virtual bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const = 0;
    virtual ::grpc::Status finish() const = 0;
};

struct ExchangeRecvRequest
{
    Int64 send_task_id = -1;
    std::shared_ptr<mpp::EstablishMPPConnectionRequest> req;

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
    struct LocalEnv
    {
        LocalEnv()
            : tunnel(nullptr)
        {}
        explicit LocalEnv(MPPTunnelPtr tunnel_)
            : tunnel(std::move(tunnel_))
        {}
        MPPTunnelPtr tunnel;
    };

    std::shared_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> reader;
    LocalEnv local_env;

    explicit LocalExchangePacketReader(const LocalEnv & env)
        : call(nullptr)
        , local_env(env){};

    /// put the implementation of dtor in .cpp so we don't need to put the specialization of
    /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
    ~LocalExchangePacketReader() override {}

    void initialize() const override {}
    bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const override;
    ::grpc::Status finish() const override;
};

class GRPCReceiverContext
{
public:
    using StatusType = ::grpc::Status;

    explicit GRPCReceiverContext(pingcap::kv::Cluster * cluster_, std::shared_ptr<MPPTaskManager> task_manager_ = nullptr, bool enable_local_tunnel_ = false);

    ExchangeRecvRequest makeRequest(
        int index,
        const tipb::ExchangeReceiver & pb_exchange_receiver,
        const ::mpp::TaskMeta & task_meta) const;

    std::shared_ptr<ExchangePacketReader> makeReader(const ExchangeRecvRequest & request, const std::string & recv_addr = "") const;

    static StatusType getStatusOK()
    {
        return ::grpc::Status::OK;
    }

    bool isLocalTunnelEnabled()
    {
        return enable_local_tunnel;
    }

private:
    pingcap::kv::Cluster * cluster;
    std::shared_ptr<MPPTaskManager> task_manager;
    bool enable_local_tunnel;
};
} // namespace DB
