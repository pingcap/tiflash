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
    bool meet_error;
    String error_msg;
    bool eof;
    ExchangeReceiverResult(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, bool meet_error_ = false,
        const String & error_msg_ = "", bool eof_ = false)
        : resp(resp_), call_index(call_index_), meet_error(meet_error_), error_msg(error_msg_), eof(eof_)
    {}
};

class ExchangeReceiver
{
    pingcap::kv::Cluster * cluster;
    std::chrono::seconds timeout;

    tipb::ExchangeReceiver pb_exchange_receiver;
    size_t source_num;
    ::mpp::TaskMeta task_meta;
    std::vector<std::thread> workers;
    DAGSchema schema;

    // TODO: should be a concurrency bounded queue.
    std::mutex mu;
    std::condition_variable cv;
    std::queue<ExchangeReceiverResult> result_buffer;
    std::atomic_int live_connections;
    bool inited;
    bool meet_error;
    Exception err;
    Logger * log;

    void ReadLoop(const String & meta_raw, size_t source_index);

    void decodePacket(const mpp::MPPDataPacket & p, size_t source_index)
    {
        std::shared_ptr<tipb::SelectResponse> resp_ptr = std::make_shared<tipb::SelectResponse>();
        if (!resp_ptr->ParseFromString(p.data()))
        {
            resp_ptr = nullptr;
        }
        std::lock_guard<std::mutex> lock(mu);
        if (resp_ptr != nullptr)
            result_buffer.emplace(resp_ptr, source_index);
        else
            result_buffer.emplace(resp_ptr, source_index, true, "Error while decoding MPPDataPacket");
        cv.notify_all();
    }

public:
    ExchangeReceiver(Context & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta)
        : cluster(context_.getTMTContext().getKVCluster()),
          timeout(context_.getSettings().mpp_task_timeout),
          pb_exchange_receiver(exc),
          source_num(pb_exchange_receiver.encoded_task_meta_size()),
          task_meta(meta),
          live_connections(0),
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
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    void init();

    ExchangeReceiverResult nextResult()
    {
        if (!inited)
            init();
        std::unique_lock<std::mutex> lk(mu);
        cv.wait(lk, [&] { return result_buffer.size() > 0 || live_connections == 0 || meet_error; });
        if (meet_error)
        {
            return {nullptr, 0, true, err.message(), false};
        }
        if (result_buffer.empty())
        {
            return {nullptr, 0, false, "", true};
        }
        auto result = result_buffer.front();
        result_buffer.pop();
        return result;
    }

    size_t getSourceNum() { return source_num; }
    String getName() { return "ExchangeReceiver"; }
};
} // namespace DB
