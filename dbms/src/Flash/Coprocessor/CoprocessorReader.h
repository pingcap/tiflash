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
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop


namespace DB
{

struct CoprocessorReaderResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    bool meet_error;
    String error_msg;
    bool eof;
    String req_info = "cop request";
    CoprocessorReaderResult(
        std::shared_ptr<tipb::SelectResponse> resp_, bool meet_error_ = false, const String & error_msg_ = "", bool eof_ = false)
        : resp(resp_), meet_error(meet_error_), error_msg(error_msg_), eof(eof_)
    {}
};

/// this is an adapter for pingcap::coprocessor::ResponseIter, so it can be used in TiRemoteBlockInputStream
class CoprocessorReader
{
public:
    static constexpr bool is_streaming_reader = false;

private:
    DAGSchema schema;
    pingcap::coprocessor::ResponseIter resp_iter;

public:
    CoprocessorReader(
        const DAGSchema & schema_, pingcap::kv::Cluster * cluster, std::vector<pingcap::coprocessor::copTask> tasks, int concurrency)
        : schema(schema_), resp_iter(std::move(tasks), cluster, concurrency, &Logger::get("pingcap/coprocessor"))
    {
        resp_iter.open();
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    void cancel() { resp_iter.cancel(); }

    CoprocessorReaderResult nextResult()
    {
        auto && [result, has_next] = resp_iter.next();
        if (!result.error.empty())
        {
            return {nullptr, true, result.error.message(), false};
        }
        if (!has_next)
        {
            return {nullptr, false, "", true};
        }
        const std::string & data = result.data();
        std::shared_ptr<tipb::SelectResponse> resp = std::make_shared<tipb::SelectResponse>();
        if (resp->ParseFromString(data))
        {
            return {resp, false, "", false};
        }
        else
        {
            return {nullptr, true, "Error while decoding coprocessor::Response", false};
        }
    }

    size_t getSourceNum() { return 1; }
    String getName() { return "CoprocessorReader"; }
};
} // namespace DB
