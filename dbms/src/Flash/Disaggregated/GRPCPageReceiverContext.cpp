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

#include <Common/Exception.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Storages/Transaction/TMTContext.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/mpp.pb.h>

#include <cassert>
#include <tuple>

namespace pingcap
{
namespace kv
{
template <>
struct RpcTypeTraits<::mpp::FetchDisaggregatedPagesRequest>
{
    using RequestType = mpp::FetchDisaggregatedPagesRequest;
    using ResultType = mpp::PagesPacket;
    static std::unique_ptr<grpc::ClientReader<::mpp::PagesPacket>> doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->FetchDisaggregatedPages(context, req);
    }
    static std::unique_ptr<grpc::ClientAsyncReader<::mpp::PagesPacket>> doAsyncRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        grpc::CompletionQueue & cq,
        void * call)
    {
        return client->stub->AsyncFetchDisaggregatedPages(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{
namespace
{
struct GrpcExchangePacketReader : public ExchangePagePacketReader
{
    std::shared_ptr<pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<grpc::ClientReader<mpp::PagesPacket>> reader;

    explicit GrpcExchangePacketReader(const FetchPagesRequest & req)
    {
        call = std::make_shared<pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest>>(req.req);
    }

    bool read(TrackedPageDataPacketPtr & packet) override
    {
        return reader->Read(packet.get());
    }

    grpc::Status finish() override
    {
        return reader->Finish();
    }

    void cancel(const String &) override {}
};

struct AsyncGrpcExchangePacketReader : public AsyncExchangePagePacketReader
{
    pingcap::kv::Cluster * cluster;
    const FetchPagesRequest & request;
    pingcap::kv::RpcCall<mpp::FetchDisaggregatedPagesRequest> call;
    grpc::ClientContext client_context;
    grpc::CompletionQueue * cq; // won't be null
    std::unique_ptr<grpc::ClientAsyncReader<::mpp::PagesPacket>> reader;

    AsyncGrpcExchangePacketReader(
        pingcap::kv::Cluster * cluster_,
        grpc::CompletionQueue * cq_,
        const FetchPagesRequest & req_)
        : cluster(cluster_)
        , request(req_)
        , call(req_.req)
        , cq(cq_)
    {
        assert(cq != nullptr);
    }

    void init(UnaryCallback<bool> * callback) override
    {
        reader = cluster->rpc_client->sendStreamRequestAsync(
            request.req->address(),
            &client_context,
            call,
            *cq,
            callback);
    }

    void read(TrackedPageDataPacketPtr & packet, UnaryCallback<bool> * callback) override
    {
        reader->Read(packet.get(), callback);
    }

    void finish(::grpc::Status & status, UnaryCallback<bool> * callback) override
    {
        reader->Finish(&status, callback);
    }
};

} // namespace

GRPCPagesReceiverContext::GRPCPagesReceiverContext(
    const DM::RemoteReadTaskPtr & remote_read_tasks_,
    pingcap::kv::Cluster * cluster_,
    bool enable_async_grpc_)
    : remote_read_tasks(remote_read_tasks_)
    , cluster(cluster_)
    , enable_async_grpc(enable_async_grpc_)
{}

GRPCPagesReceiverContext::Request GRPCPagesReceiverContext::popRequest() const
{
    Request req;
    req.seg_task = remote_read_tasks->nextFetchTask();
    req.req = std::make_shared<mpp::FetchDisaggregatedPagesRequest>();
    // TODO: build req.req from req.seg_task
    return req;
}

void GRPCPagesReceiverContext::updateTaskState(const Request & req, bool meet_error)
{
    remote_read_tasks->updateTaskState(req.seg_task, meet_error);
}

void GRPCPagesReceiverContext::setDispatchMPPTaskErrMsg(const std::string & err)
{
    std::lock_guard<std::mutex> lock(dispatch_mpp_task_err_msg_mu);
    // Only record first dispatch_mpp_task_err_msg.
    if (dispatch_mpp_task_err_msg.empty())
    {
        dispatch_mpp_task_err_msg = err;
    }
}

void GRPCPagesReceiverContext::cancelMPPTaskOnTiFlashStorageNode(LoggerPtr /*log*/)
{
    // TODO cancel
}

bool GRPCPagesReceiverContext::supportAsync(const Request & /*request*/) const
{
    return enable_async_grpc;
}

ExchangePagePacketReaderPtr GRPCPagesReceiverContext::makeReader(const Request & request) const
{
    auto reader = std::make_shared<GrpcExchangePacketReader>(request);
    reader->reader = cluster->rpc_client->sendStreamRequest(
        request.req->address(),
        &reader->client_context,
        *reader->call);
    return reader;
}

void GRPCPagesReceiverContext::makeAsyncReader(
    const Request & request,
    AsyncExchangePagePacketReaderPtr & reader,
    grpc::CompletionQueue * cq,
    UnaryCallback<bool> * callback) const
{
    reader = std::make_shared<AsyncGrpcExchangePacketReader>(cluster, cq, request);
    reader->init(callback);
}

void GRPCPagesReceiverContext::fillSchema(DAGSchema & schema) const
{
    schema.clear();
    // TODO: do we need this?
    // for (int i = 0; i < exchange_receiver_meta.field_types_size(); ++i)
    // {
    //     String name = genNameForExchangeReceiver(i);
    //     ColumnInfo info = TiDB::fieldTypeToColumnInfo(exchange_receiver_meta.field_types(i));
    //     schema.emplace_back(std::move(name), std::move(info));
    // }
}

String FetchPagesRequest::debugString() const
{
    return req->DebugString();
}
} // namespace DB
