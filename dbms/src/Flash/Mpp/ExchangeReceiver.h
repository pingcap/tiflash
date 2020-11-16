#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Interpreters/Context.h>
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
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client, const RequestType & req, grpc::CompletionQueue & cq, void * call)
    {
        return client->stub->AsyncEstablishMPPConnection(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{

class ExchangeReceiver
{
    TMTContext & context;
    std::chrono::seconds timeout;

    tipb::ExchangeReceiver pb_exchange_receiver;
    ::mpp::TaskMeta task_meta;
    std::vector<std::thread> workers;
    // async grpc
    grpc::CompletionQueue grpc_com_queue;

    DAGSchema schema;

    // TODO: should be a concurrency bounded queue.
    std::mutex rw_mu;
    std::condition_variable cv;
    std::queue<Block> block_buffer;
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
            Block block;
            const tipb::Chunk & chunk = resp.chunks(i);
            switch (resp.encode_type())
            {
                case tipb::EncodeType::TypeCHBlock:
                    block = CHBlockChunkCodec().decode(chunk, schema);
                    break;
                case tipb::EncodeType::TypeChunk:
                    block = ArrowChunkCodec().decode(chunk, schema);
                    break;
                case tipb::EncodeType::TypeDefault:
                    block = DefaultChunkCodec().decode(chunk, schema);
                    break;
                default:
                    throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }
            std::lock_guard<std::mutex> lk(rw_mu);
            block_buffer.push(std::move(block));
            cv.notify_one();
        }
    }

    // Each call owns contexts to proceed asynchronous requests
    struct ExchangeCall
    {
        using RequestType = ::mpp::EstablishMPPConnectionRequest;
        using ResultType = ::mpp::MPPDataPacket;
        grpc::ClientContext client_context;
        std::shared_ptr<RequestType> req;
        mpp::MPPDataPacket packet;
        std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> reader;
        enum StateType
        {
            CONNECTED,
            TOREAD,
            DONE
        };
        StateType state_type;
        ExchangeCall(TMTContext & tmtContext, std::string meta_str, ::mpp::TaskMeta & task_meta, grpc::CompletionQueue & cq)
        {
            auto sender_task = new mpp::TaskMeta();
            sender_task->ParseFromString(meta_str);
            req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
            req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
            req->set_allocated_sender_meta(sender_task);
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            reader = tmtContext.getCluster()->rpc_client->sendStreamRequestAsync(
                req->sender_meta().address(), &client_context, call, cq, (void *)this);
            state_type = CONNECTED;
        }
    };
    // All calls should live until the receiver shuts down
    std::vector<std::shared_ptr<ExchangeCall>> exchangeCalls;
    void sendAsyncReq()
    {
        for (auto & meta : pb_exchange_receiver.encoded_task_meta())
        {
            live_workers++;
            auto ptr = std::make_shared<ExchangeCall>(context, meta, task_meta, grpc_com_queue);
            exchangeCalls.emplace_back(ptr);
            LOG_DEBUG(log, "begin to async read : " << ptr << " " << ptr->req->DebugString());
        }
    }
    void proceedAsyncReq()
    {
        try
        {
            void * got_tag;
            bool ok = false;

            // Block until the next result is available in the completion queue "cq".
            while (live_workers > 0 && grpc_com_queue.Next(&got_tag, &ok))
            {
                ExchangeCall * call = static_cast<ExchangeCall *>(got_tag);
                if (!ok)
                {
                    call->state_type = ExchangeCall::DONE;
                }
                switch (call->state_type)
                {
                    case ExchangeCall::StateType::CONNECTED:
                    {
                        call->reader->Read(&call->packet, (void *)call);
                        call->state_type = ExchangeCall::StateType::TOREAD;
                    }
                    break;
                    case ExchangeCall::StateType::TOREAD:
                    {
                        // the last read() asynchronously succeed!
                        if (call->packet.has_error()) // This is the only way that down stream pass an error.
                        {
                            throw Exception("exchange receiver meet error : " + call->packet.error().msg());
                        }
                        decodePacket(call->packet);
                        // issue a new read request
                        call->reader->Read(&call->packet, (void *)call);
                    }
                    break;
                    case ExchangeCall::StateType::DONE:
                    {
                        live_workers--;
                    }
                    break;
                    default:
                    {
                        throw Exception("exchange receiver meet unknown msg");
                    }
                }
            }
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
        live_workers = 0;
        cv.notify_all();
        LOG_INFO(log, "async thread end!!!");
    }

public:
    ExchangeReceiver(Context & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta)
        : context(context_.getTMTContext()),
          timeout(context_.getSettings().mpp_task_timeout),
          pb_exchange_receiver(exc),
          task_meta(meta),
          live_workers(0),
          inited(false),
          meet_error(false),
          log(&Logger::get("exchange_receiver"))
    {
        for (int i = 0; i < exc.field_types_size(); i++)
        {
            String name = "exchange_receiver_" + std::to_string(i);
            ColumnInfo info = fieldTypeToColumnInfo(exc.field_types(i));
            schema.push_back(std::make_pair(name, info));
        }
    }

    ~ExchangeReceiver()
    {
        for (auto & worker : workers)
        {
            worker.join();
        }
        exchangeCalls.clear();
        grpc_com_queue.Shutdown();
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    void init()
    {
        std::lock_guard<std::mutex> lk(rw_mu);
        if (!inited)
        {
            sendAsyncReq();
            workers.emplace_back(std::thread(&ExchangeReceiver::proceedAsyncReq, this));
            inited = true;
        }
    }

    Block nextBlock()
    {
        if (!inited)
            init();
        std::unique_lock<std::mutex> lk(rw_mu);
        cv.wait(lk, [&] { return block_buffer.size() > 0 || live_workers == 0 || meet_error; });
        if (meet_error)
        {
            throw err;
        }
        if (block_buffer.empty())
        {
            return {};
        }
        Block blk = block_buffer.front();
        block_buffer.pop();
        return blk;
    }
};
} // namespace DB
