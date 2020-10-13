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
class ExchangeReceiverInputStream : public IProfilingBlockInputStream
{
    TMTContext & context;

    tipb::ExchangeReceiver exchange_receiver;
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
    bool meet_error;
    Exception err;

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

    void startAndRead(const String & raw)
    {
        try
        {
            startAndReadImpl(raw);
        }
        catch (Exception & e)
        {
            meet_error = true;
            err = e;
        }
        catch (std::exception & e)
        {
            meet_error = true;
            err = Exception(e.what());
        }
        catch (...)
        {
            meet_error = true;
            err = Exception("fatal error");
        }
        live_workers--;
        cv.notify_all();
    }

    void startAndReadImpl(const String & raw)
    {
        // TODO: Retry backoff.
        int max_retry = 60;
        for (int idx = 0; idx < max_retry; idx++)
        {
            auto sender_task = new mpp::TaskMeta();
            sender_task->ParseFromString(raw);
            auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
            req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
            req->set_allocated_sender_meta(sender_task);
            LOG_DEBUG(log, "begin start and read : " << req->DebugString());
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            grpc::ClientContext client_context;
            auto stream_resp = context.getCluster()->rpc_client->sendStreamRequest(sender_task->address(), &client_context, call);

            stream_resp->WaitForInitialMetadata();

            mpp::MPPDataPacket packet;

            bool needRetry = false;
            while (stream_resp->Read(&packet))
            {
                if (packet.has_error()) // This is the only way that down stream pass an error.
                {
                    if (canRetry(packet.error()))
                    {
                        needRetry = true;
                        break;
                    }
                    throw Exception("exchange receiver meet error : " + packet.error().msg());
                }

                LOG_DEBUG(log, "read success");
                decodePacket(packet);
            }
            if (needRetry)
            {
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1s);
                stream_resp->Finish();
                continue;
            }

            LOG_DEBUG(log, "finish worker success" << std::to_string(live_workers));
            return;
        }
        // fail
        throw Exception("cannot build connection after several tries");
    }

public:
    ExchangeReceiverInputStream(TMTContext & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta)
        : context(context_),
          exchange_receiver(exc),
          task_meta(meta),
          live_workers(0),
          inited(false),
          meet_error(false),
          log(&Logger::get("exchange_receiver"))
    {

        // generate sample block
        ColumnsWithTypeAndName columns;

        for (int i = 0; i < exc.field_types_size(); i++)
        {
            String name = "exchange_receiver_" + std::to_string(i);
            fake_schema.push_back(std::make_pair(name, ColumnInfo()));

            auto tp = getDataTypeByFieldType(exc.field_types(i));
            ColumnWithTypeAndName col(tp, name);
            columns.emplace_back(col);
        }

        sample_block = Block(columns);
    }

    ~ExchangeReceiverInputStream()
    {
        for (auto & worker : workers)
        {
            worker.join();
        }
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return "ExchangeReceiver"; }

    void init()
    {
        int task_size = exchange_receiver.encoded_task_meta_size();
        for (int i = 0; i < task_size; i++)
        {
            live_workers++;
            std::thread t(&ExchangeReceiverInputStream::startAndRead, this, std::ref(exchange_receiver.encoded_task_meta(i)));
            workers.push_back(std::move(t));
        }
        inited = true;
    }

    Block readImpl() override
    {
        if (!inited)
            init();
        std::unique_lock<std::mutex> lk(rw_mu);
        cv.wait(lk, [&] { return q.size() > 0 || live_workers == 0 || meet_error; });
        if (meet_error)
        {
            throw err;
        }
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
