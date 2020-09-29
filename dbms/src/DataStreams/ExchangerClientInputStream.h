#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <chrono>
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
    std::atomic_int live_workers;
    bool inited;

    Logger * log;

    void decodePacket(const mpp::MPPDataPacket & p)
    {
        tipb::SelectResponse resp;
        resp.ParseFromString(p.data());
        int chunks_size = resp.chunks_size();
        LOG_DEBUG(log, "get chunk size " + std::to_string(chunks_size));
        if (chunks_size == 0)
            return;
        for (int i = 0; i < chunks_size; i++)
        {
            const tipb::Chunk & chunk = resp.chunks(i);
            Block block = CHBlockChunkCodec().decode(chunk, fake_schema);
            std::lock_guard<std::mutex> lk(rw_mu);
            q.push(std::move(block));
            cv.notify_one();
        }
    }

    // Check this error is retryable
    bool canRetry(const mpp::Error & err) { return err.msg().find("can't find") != std::string::npos; }

    // TODO: Try to catch the exception.
    void startAndRead(const String & raw)
    {
        int max_retry = 15;
        for (int idx = 0; idx < max_retry; idx++)
        {
            auto server_task = new mpp::TaskMeta();
            server_task->ParseFromString(raw);
            auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
            req->set_allocated_client_meta(new mpp::TaskMeta(task_meta));
            req->set_allocated_server_meta(server_task);
            LOG_DEBUG(log, "begin start and read : " << req->DebugString());
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            grpc::ClientContext client_context;
            auto stream_resp = context.getCluster()->rpc_client->sendStreamRequest(server_task->address(), &client_context, call);

            LOG_DEBUG(log, "wait init");

            stream_resp->WaitForInitialMetadata();

            LOG_DEBUG(log, "begin wait");

            mpp::MPPDataPacket packet;

            bool needRetry = false;
            while (stream_resp->Read(&packet))
            {
                if (packet.has_error()) // This is the only way that down stream pass an error.
                {
                    LOG_DEBUG(log, "meet error " << packet.error().msg());
                    if (canRetry(packet.error()))
                    {
                        needRetry = true;
                        break;
                    }
                    live_workers--;
                    cv.notify_all();
                    throw Exception("exchange client meet error");
                }

                LOG_DEBUG(log, "read success");
                decodePacket(packet);
            }
            if (needRetry)
            {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(100ms);
                stream_resp->Finish();
                continue;
            }

            live_workers--;
            cv.notify_all();
            LOG_DEBUG(log, "finish worker success" << std::to_string(live_workers));
            break;
        }
    }

public:
    ExchangeClientInputStream(TMTContext & context_, const ::tipb::ExchangeClient & exc, const ::mpp::TaskMeta & meta)
        : context(context_), exchange_client(exc), task_meta(meta), live_workers(0), inited(false), log(&Logger::get("exchangeclient"))
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

    void init()
    {
        int task_size = exchange_client.encoded_task_meta_size();
        for (int i = 0; i < task_size; i++)
        {
            live_workers++;
            std::thread t(&ExchangeClientInputStream::startAndRead, this, std::ref(exchange_client.encoded_task_meta(i)));
            workers.push_back(std::move(t));
        }
        inited = true;
    }

    Block readImpl() override
    {
        if (!inited)
            init();
        std::unique_lock<std::mutex> lk(rw_mu);
        cv.wait(lk, [&] { return q.size() > 0 || live_workers == 0; });
        if (q.empty())
        {
            return {};
        }
        Block blk = q.front();
        q.pop();
        return blk;
    }
};
} // namespace DB