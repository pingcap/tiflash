// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <common/logger_useful.h>

#include <memory>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpcpp/impl/codegen/client_context.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
struct BatchCoprocessorReaderResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index = 0;
    bool meet_error = false;
    bool eof = false;
    String req_info;
    String error_msg;
    DecodeDetail decode_detail{};

    static BatchCoprocessorReaderResult newOk(std::shared_ptr<tipb::SelectResponse> resp, size_t call_index, const String & req_info)
    {
        return {resp, call_index, /*meet_error*/ false, /*eof*/ false, req_info, ""};
    }
    static BatchCoprocessorReaderResult newEOF(const String & req_info)
    {
        return {/*resp*/ nullptr, 0, /*meet_error*/ false, /*eof*/ true, req_info, /*error_msg*/ ""};
    }
    static BatchCoprocessorReaderResult newError(size_t call_index, const String & req_info, const String & error_msg)
    {
        return {/*resp*/ nullptr, call_index, /*meet_error*/ false, /*eof*/ false, req_info, error_msg};
    }
};

class BatchCoprocessorReader
{
public:
    static constexpr bool is_streaming_reader = true;
    static constexpr auto name = "BatchCoprocessorReader";

public:
    BatchCoprocessorReader(
        const DAGSchema & schema_,
        pingcap::kv::Cluster * cluster_,
        pingcap::coprocessor::BatchCopTask task_,
        const pingcap::coprocessor::RequestPtr & req,
        Int64 buffer_size,
        const String & log_id)
        : schema(schema_)
        , task(std::move(task_))
        , cluster(cluster_)
        , thread_manager(newThreadManager())
        , msg_channel(std::max(1, buffer_size)) // min is 1
        , total_wait_pull_channel_elapse_ms(0)
        , total_wait_push_channel_elapse_ms(0)
        , total_wait_net_elapse_ms(0)
        , total_net_recv_bytes(0)
        , collected(false)
        , log(Logger::get(name, log_id))
        , state(ExchangeReceiverState::NORMAL)
    {
        auto batch_req = std::make_shared<::coprocessor::BatchRequest>();
        batch_req->set_tp(req->tp);
        batch_req->set_data(req->data);
        batch_req->set_start_ts(req->start_ts);
        batch_req->set_schema_ver(req->schema_version);
        batch_req->set_log_id(log_id);

        if (!task.region_infos.empty())
        {
            // Non-Partition table.
            for (const auto & ri : task.region_infos)
            {
                auto * reg = batch_req->add_regions();
                reg->set_region_id(ri.region_id.id);
                reg->mutable_region_epoch()->set_version(ri.region_id.ver);
                reg->mutable_region_epoch()->set_conf_ver(ri.region_id.conf_ver);
                for (const auto & key_range : ri.ranges)
                {
                    key_range.setKeyRange(reg->add_ranges());
                }
            }
        }
        else
        {
            // Partition table.
            for (const auto & table_region : task.table_regions)
            {
                auto * req_table_region = batch_req->add_table_regions();
                req_table_region->set_physical_table_id(table_region.physical_table_id);
                auto * reg = req_table_region->add_regions();
                for (const auto & ri : table_region.region_infos)
                {
                    reg->set_region_id(ri.region_id.id);
                    reg->mutable_region_epoch()->set_version(ri.region_id.ver);
                    reg->mutable_region_epoch()->set_conf_ver(ri.region_id.conf_ver);
                    for (const auto & key_range : ri.ranges)
                    {
                        key_range.setKeyRange(reg->add_ranges());
                    }
                }
            }
        }

        try
        {
            for (size_t index = 0; index < getSourceNum(); ++index)
            {
                thread_manager->schedule(true, "BatchCoprocessor", [this, batch_req = std::move(batch_req)] { readLoop(batch_req); });
            }
        }
        catch (...)
        {
            try
            {
                cancel();
                thread_manager->wait();
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        }
    }

    ~BatchCoprocessorReader()
    {
        try
        {
            close();
            thread_manager->wait();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
        LOG_TRACE(
            log,
            "BatchCoprocessorReader done, wait_pull_channel_ms={} wait_push_channel_ms={} wait_net_ms={} net_recv_bytes={}",
            total_wait_pull_channel_elapse_ms,
            total_wait_push_channel_elapse_ms,
            total_wait_net_elapse_ms,
            total_net_recv_bytes);
    }

    const DAGSchema & getOutputSchema() const { return schema; }

    size_t getSourceNum() const { return 1; }

    BatchCoprocessorReaderResult nextResult(std::queue<Block> & block_queue, const Block & header, size_t /*stream_id*/)
    {
        std::shared_ptr<coprocessor::BatchResponse> recv_msg;
        watch.restart();
        if (msg_channel.pop(recv_msg) != MPMCQueueResult::OK)
        {
            std::unique_lock lock(mu);
            if (state != ExchangeReceiverState::NORMAL)
            {
                String msg;
                if (state == ExchangeReceiverState::CANCELED)
                    msg = "query canceled";
                else if (state == ExchangeReceiverState::CLOSED)
                    msg = "ExchangeReceiver closed";
                else if (!err_msg.empty())
                    msg = err_msg;
                else
                    msg = "Unknown error";
                return BatchCoprocessorReaderResult::newError(0, name, msg);
            }
            else // msg_channel is finished, and state is NORMAL, that is the end.
            {
                return BatchCoprocessorReaderResult::newEOF(name);
            }
        }
        auto elapsed_ms = watch.elapsedMilliseconds();
        total_wait_pull_channel_elapse_ms += elapsed_ms;

        assert(recv_msg != nullptr);
        BatchCoprocessorReaderResult result;
        if (!recv_msg->data().empty())
        {
            auto resp_ptr = std::make_shared<tipb::SelectResponse>();
            if (!resp_ptr->ParseFromString(recv_msg->data()))
            {
                result = BatchCoprocessorReaderResult::newError(0, "", "decode error");
            }
            else
            {
                result = BatchCoprocessorReaderResult::newOk(resp_ptr, 0, "");
            }
        }
        else // Last packet.
        {
            result = BatchCoprocessorReaderResult::newOk(nullptr, 0, "");
        }

        if (!result.meet_error && result.resp != nullptr)
        {
            assert(result.decode_detail.rows == 0);
            result.decode_detail = decodeChunks(result.resp, block_queue, header, schema);
        }
        return result;
    }

    void cancel()
    {
        {
            std::unique_lock lock(mu);
            state = ExchangeReceiverState::CANCELED;
        }
        msg_channel.cancel();
    }

    void close()
    {
        {
            std::unique_lock lock(mu);
            state = ExchangeReceiverState::CLOSED;
        }
        msg_channel.finish();
    }

    void collectNewThreadCount(int & cnt)
    {
        if (!collected)
        {
            collected = true;
            cnt += computeNewThreadCount();
        }
    }

    void resetNewThreadCountCompute()
    {
        collected = false;
    }

private:
    int computeNewThreadCount() const { return 1; }

    void readLoop(const std::shared_ptr<::coprocessor::BatchRequest> req)
    {
        bool meet_error = false;
        String local_err_msg;
        auto status = grpc::Status::OK;

        size_t total_wait_net_elapse_ms = 0;
        size_t total_net_recv_bytes = 0;
        try
        {
            for (size_t i = 0; i < max_retry_times; ++i)
            {
                bool has_data = false;
                // Build stream reader.
                call = std::make_shared<pingcap::kv::RpcCall<coprocessor::BatchRequest>>(req);
                stream_reader = cluster->rpc_client->sendStreamRequest(task.store_addr, &client_context, *call);

                Stopwatch read_watch;
                while (true)
                {
                    read_watch.restart();
                    auto rsp = std::make_shared<coprocessor::BatchResponse>();
                    bool success = stream_reader->Read(rsp.get());
                    total_net_recv_bytes += rsp->ByteSizeLong();
                    total_wait_net_elapse_ms += read_watch.elapsedMilliseconds();
                    if (!success)
                    {
                        // no more incoming message
                        break;
                    }

                    read_watch.restart();
                    has_data = true;
                    if (msg_channel.push(std::move(rsp)) != MPMCQueueResult::OK)
                    {
                        meet_error = true;
                        break;
                    }
                    total_wait_push_channel_elapse_ms += read_watch.elapsedMilliseconds();
                }
                if (meet_error)
                {
                    break;
                }
                status = stream_reader->Finish();
                if (status.ok())
                {
                    LOG_DEBUG(log, "BatchCoprocessorReader finish read");
                    break;
                }
                else
                {
                    const bool retriable = !has_data && i + 1 < max_retry_times;
                    LOG_WARNING(
                        log,
                        "BatchCoprocessorReader::readLoop meets rpc fail. Err code = {}, err msg = {}, retriable = {}",
                        status.error_code(),
                        status.error_message(),
                        retriable);
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
        connectionDone(meet_error, local_err_msg, log, total_wait_net_elapse_ms, total_net_recv_bytes);
    }

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & /* log */,
        size_t net_elapsed_ms,
        size_t net_recv_bytes)
    {
        {
            std::unique_lock lock(mu);
            if (meet_error)
            {
                if (state == ExchangeReceiverState::NORMAL)
                    state = ExchangeReceiverState::ERROR;
                if (err_msg.empty())
                    err_msg = local_err_msg;
            }
        }
        msg_channel.finish();
        total_wait_net_elapse_ms = net_elapsed_ms;
        total_net_recv_bytes = net_recv_bytes;
    }

    static DecodeDetail decodeChunks(
        const std::shared_ptr<tipb::SelectResponse> & resp,
        std::queue<Block> & block_queue,
        const Block & header,
        const DAGSchema & schema)
    {
        assert(resp != nullptr);
        DecodeDetail detail;
        const int chunk_size = resp->chunks_size();
        if (chunk_size == 0)
            return detail;
        for (int i = 0; i < chunk_size; ++i)
        {
            Block block;
            const tipb::Chunk & chunk = resp->chunks(i);
            switch (resp->encode_type())
            {
            case tipb::EncodeType::TypeCHBlock:
                block = CHBlockChunkCodec::decode(chunk.rows_data(), header);
                break;
            case tipb::EncodeType::TypeChunk:
                block = ArrowChunkCodec().decode(chunk.rows_data(), schema);
                break;
            case tipb::EncodeType::TypeDefault:
                block = DefaultChunkCodec().decode(chunk.rows_data(), schema);
                break;
            default:
                throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }

            detail.rows += block.rows();

            if (unlikely(block.rows() == 0))
                continue;
            if (resp->encode_type() != tipb::EncodeType::TypeCHBlock)
                assertBlockSchema(header, block, "BatchCoprocessorReader decode chunks");
            block_queue.push(std::move(block));
        }
        return detail;
    }

private:
    static constexpr size_t max_retry_times = 10;
    DAGSchema schema;
    pingcap::coprocessor::BatchCopTask task;

    pingcap::kv::Cluster * cluster;
    std::shared_ptr<pingcap::kv::RpcCall<coprocessor::BatchRequest>> call;
    grpc::ClientContext client_context;
    std::unique_ptr<::grpc::ClientReader<coprocessor::BatchResponse>> stream_reader;

    std::shared_ptr<ThreadManager> thread_manager;
    MPMCQueue<std::shared_ptr<coprocessor::BatchResponse>> msg_channel;

    Stopwatch watch;

    size_t total_wait_pull_channel_elapse_ms;
    size_t total_wait_push_channel_elapse_ms;
    size_t total_wait_net_elapse_ms;
    size_t total_net_recv_bytes;

    bool collected;

    LoggerPtr log;

    std::mutex mu;
    ExchangeReceiverState state;
    String err_msg;
};
} // namespace DB
