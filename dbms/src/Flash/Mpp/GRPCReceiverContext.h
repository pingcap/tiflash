#pragma once

#include <common/logger_useful.h>
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

    struct Reader
    {
        std::unique_ptr<pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest>> call;
        grpc::ClientContext client_context;
        std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;
        Poco::Logger * log = nullptr;

        explicit Reader(const Request & req);
        /// put the implementation of dtor in .cpp so we don't need to put the specialization of
        /// pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> in header file.
        ~Reader();

        void initialize() const;
        bool read(mpp::MPPDataPacket * packet) const;
        StatusType finish() const;
    };

    explicit GRPCReceiverContext(pingcap::kv::Cluster * cluster_);

    Request makeRequest(
        int index,
        const tipb::ExchangeReceiver & pb_exchange_receiver,
        const ::mpp::TaskMeta & task_meta) const;

    std::shared_ptr<Reader> makeReader(const Request & request) const;

    static StatusType getStatusOK()
    {
        return ::grpc::Status::OK;
    }

private:
    pingcap::kv::Cluster * cluster;
    Poco::Logger * log;
};
} // namespace DB
