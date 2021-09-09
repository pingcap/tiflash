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

template <typename T>
class RecyclableBuffer
{
public:
    explicit RecyclableBuffer(size_t limit)
        : capacity(limit)
    {
        /// init empty ones
        for (size_t i = 0; i < limit; ++i)
        {
            empty_ones.push(T());
        }
    }
    bool hasEmpty()
    {
        assert(!isOverflow(empty_ones));
        return !empty_ones.empty();
    }
    bool hasOne()
    {
        assert(!isOverflow(ones));
        return !ones.empty();
    }
    /// before pushing one, the queue should have place
    bool hasEmptyPlace()
    {
        assert(!isOverflow(empty_ones));
        return !isFull(empty_ones);
    }
    bool hasPlace()
    {
        assert(!isOverflow(ones));
        return !isFull(ones);
    }

    void popEmpty(T & t)
    {
        assert(!empty_ones.empty() && !isOverflow(empty_ones));
        t = empty_ones.front();
        empty_ones.pop();
    }
    void popOne(T & t)
    {
        assert(!ones.empty() && !isOverflow(ones));
        t = ones.front();
        ones.pop();
    }
    void pushOne(const T & t)
    {
        assert(!isFullOrOverflow(ones));
        ones.push(t);
    }
    void pushEmpty(const T & t)
    {
        assert(!isFullOrOverflow(empty_ones));
        empty_ones.push(t);
    }
    void pushEmpty(T && t)
    {
        assert(!isFullOrOverflow(empty_ones));
        empty_ones.push(std::move(t));
    }

private:
    bool isFullOrOverflow(const std::queue<T> & q) const
    {
        return q.size() >= capacity;
    }
    bool isOverflow(const std::queue<T> & q) const
    {
        return q.size() > capacity;
    }
    bool isFull(const std::queue<T> & q) const
    {
        return q.size() == capacity;
    }

    std::queue<T> empty_ones;
    std::queue<T> ones;
    size_t capacity;
};

class ExchangeReceiver
{
public:
    static constexpr bool is_streaming_reader = true;

private:
    pingcap::kv::Cluster * cluster;

    tipb::ExchangeReceiver pb_exchange_receiver;
    size_t source_num;
    ::mpp::TaskMeta task_meta;
    size_t max_streams;
    size_t max_buffer_size;
    std::vector<std::thread> workers;
    DAGSchema schema;
    std::mutex mu;
    std::condition_variable cv;
    RecyclableBuffer<std::shared_ptr<ReceivedPacket>> res_buffer;
    Int32 live_connections;
    State state;
    String err_msg;

    std::shared_ptr<LogWithPrefix> log;

    void setUpConnection();

    void ReadLoop(const String & meta_raw, size_t source_index);

public:
    ExchangeReceiver(Context & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta, size_t max_streams_, const std::shared_ptr<LogWithPrefix> & log_ = nullptr)
        : cluster(context_.getTMTContext().getKVCluster())
        , pb_exchange_receiver(exc)
        , source_num(pb_exchange_receiver.encoded_task_meta_size())
        , task_meta(meta)
        , max_streams(max_streams_)
        , max_buffer_size(max_streams_ * 2)
        , res_buffer(max_buffer_size)
        , live_connections(pb_exchange_receiver.encoded_task_meta_size())
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

    ~ExchangeReceiver()
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

    ExchangeReceiverResult nextResult();

    size_t getSourceNum() { return source_num; }
    String getName() { return "ExchangeReceiver"; }
};
} // namespace DB
