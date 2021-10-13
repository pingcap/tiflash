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
class GRPCReceiverContext
{
public:
    using StatusType = ::grpc::Status;

    struct Request
    {
        Int64 send_task_id = -1;
        std::shared_ptr<mpp::EstablishMPPConnectionRequest> req;

        String debugString() const;
    };

    struct LocalEnv
    {
        LocalEnv()
            : tunnel(nullptr)
            , status()
        {}
        LocalEnv(MPPTunnelPtr tunnel_, grpc::Status status_)
            : tunnel(tunnel_)
            , status(status_)
        {}
        MPPTunnelPtr tunnel;
        grpc::Status status;
    };

    struct Reader
    {
        std::shared_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
        grpc::ClientContext client_context;
        std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> reader;
        std::shared_ptr<MPPTaskManager> task_manager;
        LocalEnv local_env;
        bool is_local;

        explicit Reader(const Request & req);
        explicit Reader(const LocalEnv & env)
            : call(nullptr)
            , local_env(env)
            , is_local(true){};
        /// put the implementation of dtor in .cpp so we don't need to put the specialization of
        /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
        ~Reader();

        void initialize() const;
        bool read(std::shared_ptr<mpp::MPPDataPacket> & packet) const;
        StatusType finish() const;
        // std::tuple<MPPTunnelPtr, grpc::Status> EstablishMPPConnectionLocal(const ::mpp::EstablishMPPConnectionRequest * request);
    };

    explicit GRPCReceiverContext(pingcap::kv::Cluster * cluster_, std::shared_ptr<MPPTaskManager> task_manager_ = nullptr);

    Request makeRequest(
        int index,
        const tipb::ExchangeReceiver & pb_exchange_receiver,
        const ::mpp::TaskMeta & task_meta) const;

    std::shared_ptr<Reader> makeReader(const Request & request) const;

    static StatusType getStatusOK()
    {
        return ::grpc::Status::OK;
    }

    void enableLocal()
    {
        is_local = true;
    }

private:
    pingcap::kv::Cluster * cluster;
    std::shared_ptr<MPPTaskManager> task_manager;
    bool is_local;
};
} // namespace DB
