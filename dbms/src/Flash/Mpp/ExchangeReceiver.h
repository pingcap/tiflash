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
    ExchangeReceiverResult(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, const String & req_info_ = "",
        bool meet_error_ = false, const String & error_msg_ = "", bool eof_ = false)
        : resp(resp_), call_index(call_index_), req_info(req_info_), meet_error(meet_error_), error_msg(error_msg_), eof(eof_)
    {}
    ExchangeReceiverResult() : ExchangeReceiverResult(nullptr, 0) {}
};

enum State
{
    NORMAL,
    ERROR,
    CANCELED,
    CLOSED,
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
    size_t max_buffer_size;
    std::vector<std::thread> workers;
    DAGSchema schema;

    // TODO: should be a concurrency bounded queue.
    std::mutex mu;
    std::condition_variable cv;
    std::queue<ExchangeReceiverResult> result_buffer;
    Int32 live_connections;
    State state;
    String err_msg;
    Logger * log;

    void setUpConnection();

    void ReadLoop(const String & meta_raw, size_t source_index);

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
    ExchangeReceiver(Context & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta, size_t max_buffer_size_)
        : cluster(context_.getTMTContext().getKVCluster()),
          pb_exchange_receiver(exc),
          source_num(pb_exchange_receiver.encoded_task_meta_size()),
          task_meta(meta),
          max_buffer_size(max_buffer_size_),
          live_connections(pb_exchange_receiver.encoded_task_meta_size()),
          state(NORMAL),
          log(&Logger::get("exchange_receiver"))
    {
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
} // namespace DB
