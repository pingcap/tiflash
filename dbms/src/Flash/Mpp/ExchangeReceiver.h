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
    mpp::MPPDataPacket packet;
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

template <typename T>
struct SingleElementQueue
{
    std::unique_ptr<T> obj;
    std::mutex mu;
    std::condition_variable cv;
};

template <typename T>
class MPMCQueue
{
public:
    enum Status
    {
        NORMAL,
        CANCELLED,
        FINISHED,
    };

    explicit MPMCQueue(Int64 capacity_)
        : capacity(capacity_)
        , objs(capacity)
    {}

    std::unique_ptr<T> popOne()
    {
        Int64 ticket = getReadTicket();
        if (ticket < 0)
            return {};

        std::unique_ptr<T> res = popObj(ticket);

        if (res)
            finishRead(ticket);
        return res;
    }

    bool pushOne(std::unique_ptr<T> v)
    {
        Int64 ticket = getWriteTicket();
        if (ticket < 0)
            return {};

        bool res = pushObj(ticket, std::move(v));

        if (res)
            finishWrite(ticket);
        return res;
    }

    void cancel()
    {
        cancelled.store(true, std::memory_order_relaxed);
        read_cv.notify_all();
        write_cv.notify_all();
        for (auto & obj : objs)
            obj.cv.notify_all();
    }

    void finish()
    {
        finished.store(true, std::memory_order_relaxed);
        read_cv.notify_all();
        write_cv.notify_all();
        for (auto & obj : objs)
            obj.cv.notify_all();
    }

    Status getStatus() const
    {
        if (isCancelled())
            return CANCELLED;
        if (isFinished())
            return FINISHED;
        return NORMAL;
    }
private:
    Int64 getReadTicket()
    {
        Int64 ticket = -1;
        {
            std::unique_lock lock(read_mu);
            read_cv.wait(lock, [&] { return read_allocated - read_finished < capacity || isCancelled() || readPassFinishedPoint(); });
            if (!isCancelled() && !readPassFinishedPoint() && read_allocated - read_finished < capacity)
                ticket = read_allocated++;
        }
        read_cv.notify_all();
        return ticket;
    }

    Int64 getWriteTicket()
    {
        Int64 ticket = -1;
        {
            std::unique_lock lock(write_mu);
            write_cv.wait(lock, [&] { return write_allocated - write_finished < capacity || finishedOrCancelled(); });
            if (!finishedOrCancelled() && write_allocated - write_finished < capacity)
                ticket = write_allocated++;
        }
        write_cv.notify_all();
        return ticket;
    }

    std::unique_ptr<T> popObj(Int64 ticket)
    {
        SingleElementQueue<T> & queue = objs[ticket % capacity];
        std::unique_ptr<T> res;
        {
            std::unique_lock lock(queue.mu);
            queue.cv.wait(lock, [&] { return queue.obj || isCancelled(); });
            if (queue.obj)
                res = std::move(queue.obj);
        }
        if (res)
            queue.cv.notify_all();
        return res;
    }

    bool pushObj(Int64 ticket, std::unique_ptr<T> && v)
    {
        SingleElementQueue<T> & queue = objs[ticket % capacity];
        {
            std::unique_lock lock(queue.mu);
            queue.cv.wait(lock, [&] { return !queue.obj || isCancelled(); });
            if (queue.obj) /// cancelled
                return false;
            queue.obj = std::move(v);
        }
        queue.cv.notify_all();
        return true;
    }

    void bumpFinished(Int64 ticket, std::mutex & mu, std::condition_variable & cv, Int64 & finished)
    {
        {
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return finished == ticket || isCancelled(); });
            ++finished; // when cancelled it's ok to bump finished.
        }
        cv.notify_all();
    }

    void finishRead(Int64 ticket)
    {
        bumpFinished(ticket, read_mu, read_cv, read_finished);
    }

    void finishWrite(Int64 ticket)
    {
        bumpFinished(ticket, write_mu, write_cv, write_finished);
    }

    bool finishedOrCancelled() const
    {
        return isFinished() || isCancelled();
    }

    /// must call under protection of `read_mu`.
    bool readPassFinishedPoint() const
    {
        return isFinished() && read_allocated > write_allocated;
    }

    bool isFinished() const
    {
        return finished.load(std::memory_order_relaxed);
    }

    bool isCancelled() const
    {
        return cancelled.load(std::memory_order_relaxed);
    }
private:
    const Int64 capacity;

    std::mutex read_mu;
    std::mutex write_mu;
    std::condition_variable read_cv;
    std::condition_variable write_cv;
    Int64 read_finished = 0;
    Int64 read_allocated = 0;
    Int64 write_finished = 0;
    Int64 write_allocated = 0;
    std::atomic<bool> cancelled = false;
    std::atomic<bool> finished = false;

    std::vector<SingleElementQueue<T>> objs;
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
    MPMCQueue<ReceivedPacket> received_packets;
    MPMCQueue<ReceivedPacket> empty_received_packets;
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
                    std::unique_ptr<ReceivedPacket> packet = empty_received_packets.popOne();
                    if (!packet)
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(getState()) + ", exit from ReadLoop";
                        LOG_WARNING(log, local_err_msg);
                        break;
                    }
                    packet->req_info = req_info;
                    packet->source_index = source_index;
                    bool success = reader->read(&packet->packet);
                    if (!success)
                        break;
                    else
                        has_data = true;
                    if (packet->packet.has_error())
                    {
                        throw Exception("Exchange receiver meet error : " + packet->packet.error().msg());
                    }
                    bool push_res = received_packets.pushOne(std::move(packet));
                    if (!push_res)
                    {
                        meet_error = true;
                        local_err_msg = "receiver's state is " + getReceiverStateStr(getState()) + ", exit from ReadLoop";
                        LOG_WARNING(log, local_err_msg);
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
            if (unlikely(meet_error && state == NORMAL))
                state = ERROR;
            if (unlikely(meet_error && err_msg.empty()))
                err_msg = local_err_msg;
            copy_live_conn = live_connections;
        }
        LOG_DEBUG(log, fmt::format("{} -> {} end! current alive connections: {}", send_task_id, recv_task_id, copy_live_conn));

        if (meet_error)
        {
            received_packets.cancel();
        }
        else if (copy_live_conn == 0)
        {
            LOG_DEBUG(log, fmt::format("All threads end in ExchangeReceiver"));
            received_packets.finish();
        }
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

    State getState()
    {
        std::unique_lock lock(mu);
        return state;
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
        , received_packets(max_buffer_size)
        , empty_received_packets(max_buffer_size)
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

        for (size_t i = 0; i < max_buffer_size; ++i)
            empty_received_packets.pushOne(std::make_unique<ReceivedPacket>());

        setUpConnection();
    }

    ~ExchangeReceiverBase()
    {
        cancel();

        for (auto & worker : workers)
        {
            worker.join();
        }
    }

    void cancel()
    {
        std::unique_lock<std::mutex> lk(mu);
        state = CANCELED;
        received_packets.cancel();
        empty_received_packets.cancel();
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    ExchangeReceiverResult nextResult()
    {
        std::unique_ptr<ReceivedPacket> packet = received_packets.popOne();
        if (!packet)
        {
            String msg;
            std::unique_lock lock(mu);
            if (state == NORMAL)
                return {nullptr, 0, "ExchangeReceiver", false, "", true};
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
        assert(packet != nullptr);
        ExchangeReceiverResult result;
        if (packet->packet.has_error())
        {
            result = {nullptr, packet->source_index, packet->req_info, true, packet->packet.error().msg(), false};
        }
        else
        {
            auto resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(packet->packet.data()))
            {
                result = {nullptr, packet->source_index, packet->req_info, true, "decode error", false};
            }
            else
            {
                result = {resp_ptr, packet->source_index, packet->req_info};
            }
        }
        packet->packet.Clear();
        empty_received_packets.pushOne(std::move(packet));
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
