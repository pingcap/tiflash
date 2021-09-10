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

struct ReceivedPacket
{
    ReceivedPacket()
    {
        packet = std::make_shared<mpp::MPPDataPacket>();
    }
    std::shared_ptr<mpp::MPPDataPacket> packet;
    size_t source_index = 0;
    String req_info;
};

/// RecyclableBuffer recycles unused objects to avoid too much allocation of objects.
template <typename T>
class RecyclableBuffer
{
public:
    explicit RecyclableBuffer(size_t limit)
        : capacity(limit)
    {
        /// init empty objects
        for (size_t i = 0; i < limit; ++i)
        {
            empty_objects.push(std::make_shared<T>());
        }
    }
    bool hasEmpty() const
    {
        assert(!isOverflow(empty_objects));
        return !empty_objects.empty();
    }
    bool hasObjects() const
    {
        assert(!isOverflow(objects));
        return !objects.empty();
    }
    bool canPushEmpty() const
    {
        assert(!isOverflow(empty_objects));
        return !isFull(empty_objects);
    }
    bool canPush() const
    {
        assert(!isOverflow(objects));
        return !isFull(objects);
    }

    void popEmpty(std::shared_ptr<T> & t)
    {
        assert(!empty_objects.empty() && !isOverflow(empty_objects));
        t = empty_objects.front();
        empty_objects.pop();
    }
    void popObject(std::shared_ptr<T> & t)
    {
        assert(!objects.empty() && !isOverflow(objects));
        t = objects.front();
        objects.pop();
    }
    void pushObject(const std::shared_ptr<T> & t)
    {
        assert(!isFullOrOverflow(objects));
        objects.push(t);
    }
    void pushEmpty(const std::shared_ptr<T> & t)
    {
        assert(!isFullOrOverflow(empty_objects));
        empty_objects.push(t);
    }
    void pushEmpty(std::shared_ptr<T> && t)
    {
        assert(!isFullOrOverflow(empty_objects));
        empty_objects.push(std::move(t));
    }

private:
    bool isFullOrOverflow(const std::queue<std::shared_ptr<T>> & q) const
    {
        return q.size() >= capacity;
    }
    bool isOverflow(const std::queue<std::shared_ptr<T>> & q) const
    {
        return q.size() > capacity;
    }
    bool isFull(const std::queue<std::shared_ptr<T>> & q) const
    {
        return q.size() == capacity;
    }

    std::queue<std::shared_ptr<T>> empty_objects;
    std::queue<std::shared_ptr<T>> objects;
    size_t capacity;
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
    const size_t max_streams;
    const size_t max_buffer_size;

    std::vector<std::thread> workers;
    DAGSchema schema;
    std::mutex mu;
    std::condition_variable cv;
    RecyclableBuffer<ReceivedPacket> res_buffer;
    Int32 live_connections;
    State state;
    String err_msg;

    std::shared_ptr<LogWithPrefix> log;

    void setUpConnection()
    {
        for (int index = 0; index < pb_exchange_receiver.encoded_task_meta_size(); index++)
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
                std::shared_ptr<ReceivedPacket> packet;
                bool has_data = false;
                for (;;)
                {
                    LOG_TRACE(log, "begin next ");
                    {
                        std::unique_lock<std::mutex> lock(mu);
                        cv.wait(lock, [&] { return res_buffer.hasEmpty() || state != NORMAL; });
                        if (state == NORMAL)
                        {
                            res_buffer.popEmpty(packet);
                            cv.notify_all();
                        }
                        else
                        {
                            meet_error = true;
                            local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from ReadLoop";
                            LOG_WARNING(log, local_err_msg);
                            break;
                        }
                    }
                    packet->req_info = req_info;
                    packet->source_index = source_index;
                    bool success = reader->read(packet->packet.get());
                    if (!success)
                        break;
                    else
                        has_data = true;
                    if (packet->packet->has_error())
                    {
                        throw Exception("Exchange receiver meet error : " + packet->packet->error().msg());
                    }
                    {
                        std::unique_lock<std::mutex> lock(mu);
                        cv.wait(lock, [&] { return res_buffer.canPush() || state != NORMAL; });
                        if (state == NORMAL)
                        {
                            res_buffer.pushObject(packet);
                            cv.notify_all();
                        }
                        else
                        {
                            meet_error = true;
                            local_err_msg = "receiver's state is " + getReceiverStateStr(state) + ", exit from ReadLoop";
                            LOG_WARNING(log, local_err_msg);
                            break;
                        }
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
                    LOG_WARNING(log,
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
        Int32 copy_live_conn = -1;
        {
            std::unique_lock<std::mutex> lock(mu);
            live_connections--;
            if (meet_error && state == NORMAL)
                state = ERROR;
            if (meet_error && err_msg.empty())
                err_msg = local_err_msg;
            copy_live_conn = live_connections;
            cv.notify_all();
        }
        LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

        if (copy_live_conn == 0)
            LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
        else if (copy_live_conn < 0)
            throw Exception("live_connections should not be less than 0!");
    }


    static String getReceiverStateStr(const State & s)
    {
        switch (s)
        {
        case NORMAL:
            return "NORMAL";
        case ERROR:
            return "ERROR";
        case CANCELED:
            return "CANCELED";
        case CLOSED:
            return "CLOSED";
        default:
            return "UNKNOWN";
        }
    }

public:
    ExchangeReceiverBase(
        std::shared_ptr<RPCContext> rpc_context_,
        const ::tipb::ExchangeReceiver & exc,
        const ::mpp::TaskMeta & meta,
        size_t max_streams_,
        const LogWithPrefixPtr & log_ = nullptr)
        : rpc_context(std::move(rpc_context_))
        , pb_exchange_receiver(exc)
        , source_num(pb_exchange_receiver.encoded_task_meta_size())
        , task_meta(meta)
        , max_streams(max_streams_)
        , max_buffer_size(max_streams_ * 2)
        , res_buffer(max_buffer_size)
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
        std::shared_ptr<ReceivedPacket> packet;
        {
            std::unique_lock<std::mutex> lock(mu);
            cv.wait(lock, [&] { return res_buffer.hasObjects() || live_connections == 0 || state != NORMAL; });

            if (state != NORMAL)
            {
                String msg;
                if (state == CANCELED)
                    msg = "query canceled";
                else if (state == CLOSED)
                    msg = "ExchangeReceiver closed";
                else if (!err_msg.empty())
                    msg = err_msg;
                else
                    msg = "Unknown error";
                return {nullptr, 0, "ExchangeReceiver", true, msg, false};
            }
            else if (res_buffer.hasObjects())
            {
                res_buffer.popObject(packet);
                cv.notify_all();
            }
            else /// live_connections == 0, res_buffer is empty, and state is NORMAL, that is the end.
            {
                return {nullptr, 0, "ExchangeReceiver", false, "", true};
            }
        }
        assert(packet != nullptr && packet->packet != nullptr);
        ExchangeReceiverResult result;
        if (packet->packet->has_error())
        {
            result = {nullptr, packet->source_index, packet->req_info, true, packet->packet->error().msg(), false};
        }
        else
        {
            auto resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(packet->packet->data()))
            {
                result = {nullptr, packet->source_index, packet->req_info, true, "decode error", false};
            }
            else
            {
                result = {resp_ptr, packet->source_index, packet->req_info};
            }
            if (resp_ptr->chunks_size() == 0)
                throw Exception("Exchange receiver find empty response: ");
        }
        packet->packet->Clear();
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return res_buffer.canPushEmpty(); });
        res_buffer.pushEmpty(std::move(packet));
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
