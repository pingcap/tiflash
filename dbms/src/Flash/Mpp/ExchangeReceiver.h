#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>

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
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> doAsyncRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        grpc::CompletionQueue & cq,
        void * call)
    {
        return client->stub->AsyncEstablishMPPConnection(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{
struct ExchangeReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;
    ExchangeReceiverResult(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, const String & req_info_ = "", bool meet_error_ = false, const String & error_msg_ = "", bool eof_ = false)
        : resp(resp_)
        , call_index(call_index_)
        , req_info(req_info_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
    {}
    ExchangeReceiverResult()
        : ExchangeReceiverResult(nullptr, 0)
    {}
};

enum State
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
};

struct GRPCReceiverContext
{
    using StatusType = ::grpc::Status;

    struct Request
    {
        Int64 send_task_id = -1;
        std::shared_ptr<mpp::EstablishMPPConnectionRequest> req;

        String DebugString() const
        {
            return req->DebugString();
        }
    };

    struct Reader
    {
        pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call;
        grpc::ClientContext client_context;
        std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> reader;

        explicit Reader(const Request & req)
            : call(req.req)
        {}

        void initialize() const
        {
            reader->WaitForInitialMetadata();
        }

        bool read(mpp::MPPDataPacket * packet) const
        {
            return reader->Read(packet);
        }

        StatusType finish() const
        {
            return reader->Finish();
        }
    };

    pingcap::kv::Cluster * cluster;

    explicit GRPCReceiverContext(pingcap::kv::Cluster * cluster_)
        : cluster(cluster_)
    {}

    Request makeRequest(
        int index,
        const tipb::ExchangeReceiver & pb_exchange_receiver,
        const ::mpp::TaskMeta & task_meta) const
    {
        const auto & meta_raw = pb_exchange_receiver.encoded_task_meta(index);
        auto sender_task = std::make_unique<mpp::TaskMeta>();
        sender_task->ParseFromString(meta_raw);
        Request req;
        req.send_task_id = sender_task->task_id();
        req.req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
        req.req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
        req.req->set_allocated_sender_meta(sender_task.release());
        return req;
    }

    std::shared_ptr<Reader> makeReader(const Request & request) const
    {
        auto reader = std::make_shared<Reader>(request);
        reader->reader = cluster->rpc_client->sendStreamRequest(
            request.req->sender_meta().address(),
            &reader->client_context,
            reader->call);
        return reader;
    }

    static StatusType getStatusOK()
    {
        return ::grpc::Status::OK;
    }
};

template <typename RPCContext>
class ExchangeReceiverBase
{
public:
    static constexpr bool is_streaming_reader = true;

private:
    std::shared_ptr<RPCContext> rpc_context;
    const tipb::ExchangeReceiver pb_exchange_receiver;
    const size_t source_num;
    const ::mpp::TaskMeta task_meta;
    const size_t max_buffer_size;
    std::vector<std::thread> workers;
    DAGSchema schema;

    // TODO: should be a concurrency bounded queue.
    std::mutex mu;
    std::condition_variable cv;
    std::queue<ExchangeReceiverResult> result_buffer;
    Int32 live_connections;
    State state;
    String err_msg;

    std::shared_ptr<LogWithPrefix> log;

    void setUpConnection()
    {
        for (size_t index = 0; index < source_num; ++index)
        {
            std::thread t(&ExchangeReceiverBase::ReadLoop, this, index);
            workers.push_back(std::move(t));
        }
    }

    void ReadLoop(size_t source_index)
    {
        bool meet_error = false;
        String local_err_msg;

        Int64 send_task_id = -1;
        Int64 recv_task_id = task_meta.task_id();

        try
        {
            auto req = rpc_context->makeRequest(source_index, pb_exchange_receiver, task_meta);
            send_task_id = req.send_task_id;
            String req_info = "tunnel" + std::to_string(send_task_id) + "+" + std::to_string(recv_task_id);
            LOG_DEBUG(log, "begin start and read : " << req.DebugString());
            auto status = RPCContext::getStatusOK();
            for (int i = 0; i < 10; i++)
            {
                auto reader = rpc_context->makeReader(req);
                reader->initialize();
                mpp::MPPDataPacket packet;
                bool has_data = false;
                for (;;)
                {
                    LOG_TRACE(log, "begin next ");
                    bool success = reader->read(&packet);
                    if (!success)
                        break;
                    else
                        has_data = true;
                    if (packet.has_error())
                    {
                        throw Exception("Exchange receiver meet error : " + packet.error().msg());
                    }
                    if (!decodePacket(packet, source_index, req_info))
                    {
                        meet_error = true;
                        local_err_msg = "Decode packet meet error";
                        LOG_WARNING(log, "Decode packet meet error, exit from ReadLoop");
                        break;
                    }
                }
                // if meet error, such as decode packect fails, it will not retry.
                if (meet_error)
                {
                    break;
                }
                status = reader->finish();
                if (status.ok())
                {
                    LOG_DEBUG(log, "finish read : " << req.DebugString());
                    break;
                }
                else
                {
                    LOG_WARNING(
                        log,
                        "EstablishMPPConnectionRequest meets rpc fail. Err msg is: " << status.error_message() << " req info " << req_info);
                    // if we have received some data, we should not retry.
                    if (has_data)
                        break;

                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(1s);
                }
            }
            if (!status.ok())
            {
                meet_error = true;
                local_err_msg = status.error_message();
            }
        }
        catch (Exception & e)
        {
            meet_error = true;
            local_err_msg = e.message();
        }
        catch (std::exception & e)
        {
            meet_error = true;
            local_err_msg = e.what();
        }
        catch (...)
        {
            meet_error = true;
            local_err_msg = "fatal error";
        }
        std::lock_guard<std::mutex> lock(mu);
        live_connections--;

        // avoid concurrent conflict
        Int32 live_conn_copy = live_connections;

        if (meet_error && state == NORMAL)
            state = ERROR;
        if (meet_error && err_msg.empty())
            err_msg = local_err_msg;
        cv.notify_all();

        LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, live_conn_copy));

        if (live_conn_copy == 0)
            LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
    }

    bool decodePacket(const mpp::MPPDataPacket & p, size_t source_index, const String & req_info)
    {
        bool ret = true;
        std::shared_ptr<tipb::SelectResponse> resp_ptr = std::make_shared<tipb::SelectResponse>();
        if (!resp_ptr->ParseFromString(p.data()))
        {
            resp_ptr = nullptr;
            ret = false;
        }
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return result_buffer.size() < max_buffer_size || state != NORMAL; });
        if (state == NORMAL)
        {
            if (resp_ptr != nullptr)
                result_buffer.emplace(resp_ptr, source_index, req_info);
            else
                result_buffer.emplace(resp_ptr, source_index, req_info, true, "Error while decoding MPPDataPacket");
        }
        else
        {
            ret = false;
        }
        cv.notify_all();
        return ret;
    }

public:
    ExchangeReceiverBase(
        std::shared_ptr<RPCContext> rpc_context_,
        const ::tipb::ExchangeReceiver & exc,
        const ::mpp::TaskMeta & meta,
        size_t max_buffer_size_,
        const LogWithPrefixPtr & log_ = nullptr)
        : rpc_context(std::move(rpc_context_))
        , pb_exchange_receiver(exc)
        , source_num(pb_exchange_receiver.encoded_task_meta_size())
        , task_meta(meta)
        , max_buffer_size(max_buffer_size_)
        , live_connections(source_num)
        , state(NORMAL)
    {
        log = log_ != nullptr ? log_ : std::make_shared<LogWithPrefix>(&Poco::Logger::get("ExchangeReceiver"), "");

        for (int i = 0; i < exc.field_types_size(); i++)
        {
            String name = "exchange_receiver_" + std::to_string(i);
            ColumnInfo info = TiDB::fieldTypeToColumnInfo(exc.field_types(i));
            schema.push_back(std::make_pair(name, info));
        }

        setUpConnection();
    }

    ~ExchangeReceiverBase()
    {
        {
            std::unique_lock<std::mutex> lk(mu);
            state = CLOSED;
            cv.notify_all();
        }
        for (auto & worker : workers)
        {
            worker.join();
        }
    }

    void cancel()
    {
        std::unique_lock<std::mutex> lk(mu);
        state = CANCELED;
        cv.notify_all();
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult()
    {
        std::unique_lock<std::mutex> lk(mu);
        cv.wait(lk, [&] { return !result_buffer.empty() || live_connections == 0 || state != NORMAL; });
        ExchangeReceiverResult result;
        if (state != NORMAL)
        {
            String msg;
            if (state == CANCELED)
                msg = "Query canceled";
            else if (state == CLOSED)
                msg = "ExchangeReceiver closed";
            else if (!err_msg.empty())
                msg = err_msg;
            else
                msg = "Unknown error";
            result = {nullptr, 0, "ExchangeReceiver", true, msg, false};
        }
        else if (result_buffer.empty())
        {
            result = {nullptr, 0, "ExchangeReceiver", false, "", true};
        }
        else
        {
            result = result_buffer.front();
            result_buffer.pop();
        }
        cv.notify_all();
        return result;
    }

    size_t getSourceNum() { return source_num; }
    String getName() { return "ExchangeReceiver"; }
};

class ExchangeReceiver : public ExchangeReceiverBase<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverBase<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
