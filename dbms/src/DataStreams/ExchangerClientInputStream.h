#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Storages/Transaction/TMTContext.h>

#include <mutex>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace pingcap
{
namespace kv
{

template <>
struct RpcTypeTraits<::mpp::EstablishMPPConnectionRequest>
{
    using RequestType = ::mpp::EstablishMPPConnectionRequest;
    using ResultType = ::mpp::MPPDataPacket;
    static std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> doRPCCall(
        grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{
class ExchangeClientInputStream : public IProfilingBlockInputStream
{
    TMTContext & context;

    tipb::ExchangeClient exchange_client;
    ::mpp::TaskMeta task_meta;
    std::vector<std::thread> workers;


    DAGSchema fake_schema;
    Block sample_block;

    // TODO: should be a concurrency bounded queue.
    std::mutex rw_mu;
    std::condition_variable cv;
    std::queue<Block> q;


    void startAndRead(const String & raw)
    {

        auto server_task = std::make_shared<mpp::TaskMeta>();
        server_task->ParseFromString(raw);
        auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req->set_allocated_client_meta(&task_meta);
        req->set_allocated_server_meta(server_task.get());
        pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
        auto stream_resp = context.getCluster()->rpc_client->sendStreamRequest(server_task->address(), call, 0);
        std::shared_ptr<mpp::MPPDataPacket> packet = std::make_shared<mpp::MPPDataPacket>();
        while (stream_resp->Read(packet.get()))
        {
            if (packet->has_error())
            {
                // TODO: Sleep for a while.
            }
            tipb::Chunk chunk;
            chunk.ParseFromString(packet->data());
            Block block = CHBlockChunkCodec().decode(chunk, fake_schema);
            std::lock_guard<std::mutex> lk(rw_mu);
            q.push(std::move(block));
            cv.notify_one();
        }
    }

public:
    ExchangeClientInputStream(TMTContext & context_, const ::tipb::ExchangeClient & exc, const ::mpp::TaskMeta & meta)
        : context(context_), exchange_client(exc), task_meta(meta)
    {

        // generate sample block
        ColumnsWithTypeAndName columns;

        for (int i = 0; i < exc.field_types_size(); i++)
        {
            String name = "exchange_client_" + std::to_string(i);
            fake_schema.push_back(std::make_pair(name, ColumnInfo()));

            auto tp = getDataTypeByFieldType(exc.field_types(i));
            ColumnWithTypeAndName col(tp, name);
            columns.emplace_back(col);
        }

        sample_block = Block(columns);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return "ExchangeClient"; }

    void readPrefix() override
    {
        int task_size = exchange_client.encoded_task_meta_size();
        for (int i = 0; i < task_size; i++)
        {
            std::thread t(&ExchangeClientInputStream::startAndRead, this, std::ref(exchange_client.encoded_task_meta(i)));
            workers.push_back(std::move(t));
        }
    }

    Block readImpl() override
    {
        std::unique_lock<std::mutex> lk(rw_mu);
        cv.wait(lk, [&] { return q.size() > 0; });
        Block blk = q.front();
        q.pop();
        return blk;
    }
};
} // namespace DB