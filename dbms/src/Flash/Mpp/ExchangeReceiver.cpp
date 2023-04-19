// Copyright 2023 PingCAP, Ltd.
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

#include <Common/CPUAffinityManager.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <common/logger_useful.h>
#include <fmt/core.h>
#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>

#include <magic_enum.hpp>
#include <memory>
#include <mutex>
#include <type_traits>

namespace DB
{
namespace
{
String constructStatusString(ExchangeReceiverState state, const String & error_message)
{
    if (error_message.empty())
        return fmt::format("Receiver state: {}", magic_enum::enum_name(state));
    return fmt::format("Receiver state: {}, error message: {}", magic_enum::enum_name(state), error_message);
}

enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_BATCH_READ,
    WAIT_FINISH,
    WAIT_RETRY,
    FINISHED,
};

using Clock = std::chrono::system_clock;
using TimePoint = Clock::time_point;

constexpr Int32 max_retry_times = 10;
constexpr Int32 retry_interval_time = 1; // second

template <typename RPCContext, bool enable_fine_grained_shuffle>
class AsyncRequestHandler : public UnaryCallback<bool>
{
public:
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;
    using Self = AsyncRequestHandler<RPCContext, enable_fine_grained_shuffle>;

    AsyncRequestHandler(
        MPMCQueue<Self *> * queue,
        std::vector<MsgChannelPtr> * msg_channels_,
        const std::shared_ptr<RPCContext> & context,
        const Request & req,
        const String & req_id,
        std::atomic<Int64> * data_size_in_queue)
        : rpc_context(context)
        , cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , request(&req)
        , notify_queue(queue)
        , msg_channels(msg_channels_)
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , log(Logger::get(req_id, req_info))
        , channel_writer(msg_channels_, req_info, log, data_size_in_queue, ReceiverMode::Async)
    {
        packets.resize(batch_packet_count);
        for (auto & packet : packets)
            packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);

        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    void execute(bool & ok) override
    {
        switch (stage)
        {
        case AsyncRequestStage::WAIT_RETRY:
            start();
            break;
        case AsyncRequestStage::WAIT_MAKE_READER:
        {
            // Use lock to ensure reader is created already in reactor thread
            std::lock_guard lock(mu);
            if (!ok)
            {
                reader.reset();
                LOG_WARNING(log, "MakeReader fail. retry time: {}", retry_times);
                if (!retryOrDone("Exchange receiver meet error : send async stream request fail"))
                    notifyReactor();
            }
            else
            {
                stage = AsyncRequestStage::WAIT_BATCH_READ;
                read_packet_index = 0;
                reader->read(packets[0], thisAsUnaryCallback());
            }
            break;
        }
        case AsyncRequestStage::WAIT_BATCH_READ:
            if (ok)
                ++read_packet_index;

            if (!ok || read_packet_index == batch_packet_count || packets[read_packet_index - 1]->hasError())
                notifyReactor();
            else
                reader->read(packets[read_packet_index], thisAsUnaryCallback());
            break;
        case AsyncRequestStage::WAIT_FINISH:
            notifyReactor();
            break;
        default:
            __builtin_unreachable();
        }
    }

    // handle will be called by ExchangeReceiver::reactor.
    void handle()
    {
        std::string err_info;
        LOG_TRACE(log, "stage: {}", magic_enum::enum_name(stage));
        switch (stage)
        {
        case AsyncRequestStage::WAIT_BATCH_READ:
            LOG_TRACE(log, "Received {} packets.", read_packet_index);
            if (read_packet_index > 0)
                has_data = true;

            if (auto error_message = getErrorFromPackets(); !error_message.empty())
                setDone(fmt::format("Exchange receiver meet error : {}", error_message));
            else if (!sendPackets())
                setDone("Exchange receiver meet error : push packets fail");
            else if (read_packet_index < batch_packet_count)
            {
                stage = AsyncRequestStage::WAIT_FINISH;
                reader->finish(finish_status, thisAsUnaryCallback());
            }
            else
            {
                read_packet_index = 0;
                reader->read(packets[0], thisAsUnaryCallback());
            }
            break;
        case AsyncRequestStage::WAIT_FINISH:
            if (finish_status.ok())
                setDone("");
            else
            {
                LOG_WARNING(
                    log,
                    "Finish fail. err code: {}, err msg: {}, retry time {}",
                    finish_status.error_code(),
                    finish_status.error_message(),
                    retry_times);
                retryOrDone(fmt::format("Exchange receiver meet error : {}", finish_status.error_message()));
            }
            break;
        default:
            __builtin_unreachable();
        }
    }

    bool finished() const
    {
        return stage == AsyncRequestStage::FINISHED;
    }

    bool meetError() const { return meet_error; }
    const String & getErrMsg() const { return err_msg; }
    const LoggerPtr & getLog() const { return log; }

private:
    void notifyReactor()
    {
        notify_queue->push(this);
    }

    String getErrorFromPackets()
    {
        // step 1: check if there is error packet
        // only the last packet may has error, see execute().
        if (read_packet_index != 0 && packets[read_packet_index - 1]->hasError())
            return packets[read_packet_index - 1]->error();
        // step 2: check memory overflow error
        for (size_t i = 0; i < read_packet_index; ++i)
        {
            packets[i]->recomputeTrackedMem();
            if (packets[i]->hasError())
                return packets[i]->error();
        }
        return "";
    }

    bool retriable() const
    {
        return !has_data && retry_times + 1 < max_retry_times;
    }

    void setDone(String && msg)
    {
        if (!msg.empty())
        {
            meet_error = true;
            err_msg = std::move(msg);
        }
        stage = AsyncRequestStage::FINISHED;
    }

    void start()
    {
        stage = AsyncRequestStage::WAIT_MAKE_READER;

        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::lock_guard lock(mu);
        rpc_context->makeAsyncReader(*request, reader, cq, thisAsUnaryCallback());
    }

    bool retryOrDone(String done_msg)
    {
        if (retriable())
        {
            ++retry_times;
            stage = AsyncRequestStage::WAIT_RETRY;

            // Let alarm put me into CompletionQueue after a while
            // , so that we can try to connect again.
            alarm.Set(cq, Clock::now() + std::chrono::seconds(retry_interval_time), this);
            return true;
        }
        else
        {
            setDone(std::move(done_msg));
            return false;
        }
    }

    bool sendPackets()
    {
        // note: no exception should be thrown rudely, since it's called by a GRPC poller.
        for (size_t i = 0; i < read_packet_index; ++i)
        {
            auto & packet = packets[i];
            if (!channel_writer.write<enable_fine_grained_shuffle>(request->source_index, packet))
                return false;

            // can't reuse packet since it is sent to readers.
            packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
        }
        return true;
    }

    // in case of potential multiple inheritances.
    UnaryCallback<bool> * thisAsUnaryCallback()
    {
        return static_cast<UnaryCallback<bool> *>(this);
    }

    std::shared_ptr<RPCContext> rpc_context;
    grpc::Alarm alarm{};
    grpc::CompletionQueue * cq; // won't be null and do not delete this pointer
    const Request * request; // won't be null
    MPMCQueue<Self *> * notify_queue; // won't be null
    std::vector<MsgChannelPtr> * msg_channels; // won't be null

    String req_info;
    bool meet_error = false;
    bool has_data = false;
    String err_msg;
    int retry_times = 0;
    AsyncRequestStage stage = AsyncRequestStage::NEED_INIT;

    std::shared_ptr<AsyncReader> reader;
    TrackedMPPDataPacketPtrs packets;
    size_t read_packet_index = 0;
    Status finish_status = RPCContext::getStatusOK();
    LoggerPtr log;
    ReceiverChannelWriter channel_writer;
    std::mutex mu;
};
} // namespace

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::ExchangeReceiverBase(
    std::shared_ptr<RPCContext> rpc_context_,
    size_t source_num_,
    size_t max_streams_,
    const String & req_id,
    const String & executor_id,
    uint64_t fine_grained_shuffle_stream_count_,
    Int32 local_tunnel_version_,
    const std::vector<RequestAndRegionIDs> & disaggregated_dispatch_reqs_)
    : rpc_context(std::move(rpc_context_))
    , source_num(source_num_)
    , enable_fine_grained_shuffle_flag(enableFineGrainedShuffle(fine_grained_shuffle_stream_count_))
    , output_stream_count(enable_fine_grained_shuffle_flag ? std::min(max_streams_, fine_grained_shuffle_stream_count_) : max_streams_)
    , max_buffer_size(std::max<size_t>(batch_packet_count, std::max(source_num, max_streams_) * 2))
    , connection_uncreated_num(source_num)
    , thread_manager(newThreadManager())
    , live_local_connections(0)
    , live_connections(source_num)
    , state(ExchangeReceiverState::NORMAL)
    , exc_log(Logger::get(req_id, executor_id))
    , collected(false)
    , local_tunnel_version(local_tunnel_version_)
    , data_size_in_queue(0)
    , disaggregated_dispatch_reqs(disaggregated_dispatch_reqs_)
{
    try
    {
        prepareMsgChannels();
        if (isReceiverForTiFlashStorage())
            rpc_context->sendMPPTaskToTiFlashStorageNode(exc_log, disaggregated_dispatch_reqs);

        rpc_context->fillSchema(schema);
        setUpConnection();
    }
    catch (...)
    {
        try
        {
            handleConnectionAfterException();
            cancel();
            waitAllConnectionDone();
            thread_manager->wait();
        }
        catch (...)
        {
            tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
        }
        throw;
    }
}

template <typename RPCContext>
ExchangeReceiverBase<RPCContext>::~ExchangeReceiverBase()
{
    try
    {
        close();
        waitAllConnectionDone();
        thread_manager->wait();
        ExchangeReceiverMetric::clearDataSizeMetric(data_size_in_queue);
    }
    catch (...)
    {
        std::lock_guard lock(mu);
        RUNTIME_ASSERT(live_connections == 0, "We should wait the close of all connections");
        RUNTIME_ASSERT(live_local_connections == 0, "We should wait the close of local connection");
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::handleConnectionAfterException()
{
    std::lock_guard lock(mu);
    live_connections -= connection_uncreated_num;

    // some cv may have been blocked, wake them up and recheck the condition.
    cv.notify_all();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::waitAllConnectionDone()
{
    std::unique_lock lock(mu);
    auto pred = [&] {
        return live_connections == 0;
    };
    cv.wait(lock, pred);

    // The meaning of calling of connectionDone by local tunnel is to tell the receiver
    // to close channels and the local tunnel may still alive after it calls connectionDone.
    //
    // In order to ensure the destructions of local tunnels are
    // after the ExchangeReceiver, we need to wait at here.
    waitLocalConnectionDone(lock);
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::waitLocalConnectionDone(std::unique_lock<std::mutex> & lock)
{
    auto pred = [&] {
        return live_local_connections == 0;
    };
    cv.wait(lock, pred);
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::prepareMsgChannels()
{
    if (enable_fine_grained_shuffle_flag)
        for (size_t i = 0; i < output_stream_count; ++i)
            msg_channels.push_back(std::make_shared<ConcurrentIOQueue<std::shared_ptr<ReceivedMessage>>>(max_buffer_size));
    else
        msg_channels.push_back(std::make_shared<ConcurrentIOQueue<std::shared_ptr<ReceivedMessage>>>(max_buffer_size));
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancel()
{
    if (setEndState(ExchangeReceiverState::CANCELED))
    {
        if (isReceiverForTiFlashStorage())
            rpc_context->cancelMPPTaskOnTiFlashStorageNode(exc_log);
    }
    cancelAllMsgChannels();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::close()
{
    setEndState(ExchangeReceiverState::CLOSED);
    finishAllMsgChannels();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::addLocalConnectionNum()
{
    std::lock_guard lock(mu);
    ++live_local_connections;
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnection()
{
    mem_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
    std::vector<Request> async_requests;
    std::vector<Request> local_requests;
    bool has_remote_conn = false;

    for (size_t index = 0; index < source_num; ++index)
    {
        auto req = rpc_context->makeRequest(index);
        if (rpc_context->supportAsync(req))
        {
            async_requests.push_back(std::move(req));
            has_remote_conn = true;
        }
        else if (req.is_local)
        {
            local_requests.push_back(req);
        }
        else
        {
            setUpConnectionWithReadLoop(std::move(req));
            has_remote_conn = true;
        }
    }

    setUpLocalConnections(local_requests, has_remote_conn);

    // TODO: reduce this thread in the future.
    if (!async_requests.empty())
    {
        auto async_conn_num = async_requests.size();
        thread_manager->schedule(true, "RecvReactor", [this, async_requests = std::move(async_requests)] {
            if (enable_fine_grained_shuffle_flag)
                reactor<true>(async_requests);
            else
                reactor<false>(async_requests);
        });

        ++thread_count;
        connection_uncreated_num -= async_conn_num;
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpLocalConnections(std::vector<Request> & requests, bool has_remote_conn)
{
    for (auto & req : requests)
    {
        if (local_tunnel_version == 1)
        {
            setUpConnectionWithReadLoop(std::move(req));
        }
        else
        {
            LOG_DEBUG(exc_log, "refined local tunnel is enabled");
            String req_info = fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id);

            LocalRequestHandler local_request_handler(
                getMemoryTracker(),
                [this](bool meet_error, const String & local_err_msg) {
                    this->connectionDone(meet_error, local_err_msg, exc_log);
                },
                [this]() {
                    this->connectionLocalDone();
                },
                [this]() {
                    this->addLocalConnectionNum();
                },
                ReceiverChannelWriter(&(getMsgChannels()), req_info, exc_log, getDataSizeInQueue(), ReceiverMode::Local));

            rpc_context->establishMPPConnectionLocalV2(
                req,
                req.source_index,
                local_request_handler,
                enable_fine_grained_shuffle_flag,
                has_remote_conn);
            --connection_uncreated_num;
        }
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::setUpConnectionWithReadLoop(Request && req)
{
    thread_manager->schedule(true, "Receiver", [this, req = std::move(req)] {
        if (enable_fine_grained_shuffle_flag)
            readLoop<true>(req);
        else
            readLoop<false>(req);
    });

    ++thread_count;
    --connection_uncreated_num;
}

template <typename RPCContext>
template <bool enable_fine_grained_shuffle>
void ExchangeReceiverBase<RPCContext>::reactor(const std::vector<Request> & async_requests)
{
    using AsyncHandler = AsyncRequestHandler<RPCContext, enable_fine_grained_shuffle>;

    GET_METRIC(tiflash_thread_count, type_threads_of_receiver_reactor).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_threads_of_receiver_reactor).Decrement();
    });

    CPUAffinityManager::getInstance().bindSelfQueryThread();

    size_t alive_async_connections = async_requests.size();
    MPMCQueue<AsyncHandler *> ready_requests(alive_async_connections * 2);

    std::vector<std::unique_ptr<AsyncHandler>> handlers;
    handlers.reserve(alive_async_connections);
    for (const auto & req : async_requests)
        handlers.emplace_back(std::make_unique<AsyncHandler>(&ready_requests, &msg_channels, rpc_context, req, exc_log->identifier(), &data_size_in_queue));

    while (alive_async_connections > 0)
    {
        AsyncHandler * handler = nullptr;
        ready_requests.pop(handler);

        if (likely(handler != nullptr))
        {
            handler->handle();
            if (handler->finished())
            {
                --alive_async_connections;
                connectionDone(handler->meetError(), handler->getErrMsg(), handler->getLog());
            }
        }
        else
        {
            throw Exception("get a null pointer in reactor");
        }
    }
}

template <typename RPCContext>
template <bool enable_fine_grained_shuffle>
void ExchangeReceiverBase<RPCContext>::readLoop(const Request & req)
{
    GET_METRIC(tiflash_thread_count, type_threads_of_receiver_read_loop).Increment();
    SCOPE_EXIT({
        GET_METRIC(tiflash_thread_count, type_threads_of_receiver_read_loop).Decrement();
    });

    CPUAffinityManager::getInstance().bindSelfQueryThread();
    bool meet_error = false;
    String local_err_msg;
    String req_info = fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id);

    LoggerPtr log = exc_log->getChild(req_info);

    try
    {
        auto status = RPCContext::getStatusOK();
        ReceiverMode recv_mode = req.is_local ? ReceiverMode::Local : ReceiverMode::Sync;
        ReceiverChannelWriter channel_writer(&msg_channels, req_info, log, &data_size_in_queue, recv_mode);
        for (int i = 0; i < max_retry_times; ++i)
        {
            auto reader = rpc_context->makeReader(req);
            bool has_data = false;
            for (;;)
            {
                LOG_TRACE(log, "begin next ");
                TrackedMppDataPacketPtr packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
                bool success = reader->read(packet);
                if (!success)
                    break;
                has_data = true;
                if (packet->hasError())
                {
                    meet_error = true;
                    local_err_msg = fmt::format("Read error message from mpp packet: {}", packet->error());
                    break;
                }

                if (!channel_writer.write<enable_fine_grained_shuffle>(req.source_index, packet))
                {
                    meet_error = true;
                    local_err_msg = fmt::format("Push mpp packet failed. {}", getStatusString());
                    break;
                }
            }
            // if meet error, such as decode packet fails, it will not retry.
            if (meet_error)
            {
                reader->cancel(local_err_msg);
                break;
            }
            status = reader->finish();
            if (status.ok())
            {
                LOG_DEBUG(log, "finish read : {}", req.debugString());
                break;
            }
            else
            {
                bool retriable = !has_data && i + 1 < max_retry_times;
                LOG_WARNING(
                    log,
                    "EstablishMPPConnectionRequest meets rpc fail. Err code = {}, err msg = {}, retriable = {}",
                    status.error_code(),
                    status.error_message(),
                    retriable);
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
    catch (...)
    {
        meet_error = true;
        local_err_msg = getCurrentExceptionMessage(false);
    }
    connectionDone(meet_error, local_err_msg, log);
}

template <typename RPCContext>
DecodeDetail ExchangeReceiverBase<RPCContext>::decodeChunks(
    const std::shared_ptr<ReceivedMessage> & recv_msg,
    std::queue<Block> & block_queue,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    DecodeDetail detail;

    if (recv_msg->chunks.empty())
        return detail;
    auto & packet = recv_msg->packet->getPacket();

    // Record total packet size even if fine grained shuffle is enabled.
    detail.packet_bytes = packet.ByteSizeLong();

    switch (auto version = packet.version(); version)
    {
    case DB::MPPDataPacketV0:
    {
        for (const auto * chunk : recv_msg->chunks)
        {
            auto result = decoder_ptr->decodeAndSquash(*chunk);
            if (!result)
                continue;
            detail.rows += result->rows();
            if likely (result->rows() > 0)
            {
                block_queue.push(std::move(result.value()));
            }
        }
        return detail;
    }
    case DB::MPPDataPacketV1:
    {
        for (const auto * chunk : recv_msg->chunks)
        {
            auto && result = decoder_ptr->decodeAndSquashV1(*chunk);
            if (!result || !result->rows())
                continue;
            detail.rows += result->rows();
            block_queue.push(std::move(*result));
        }
        return detail;
    }
    default:
    {
        RUNTIME_CHECK_MSG(false, "Unknown mpp packet version {}, please update TiFlash instance", version);
        break;
    }
    }
    return detail;
}

template <typename RPCContext>
ReceiveResult ExchangeReceiverBase<RPCContext>::receive(size_t stream_id)
{
    return receive(
        stream_id,
        [&](size_t stream_id, std::shared_ptr<ReceivedMessage> & recv_msg) {
            return msg_channels[stream_id]->pop(recv_msg);
        });
}

template <typename RPCContext>
ReceiveResult ExchangeReceiverBase<RPCContext>::nonBlockingReceive(size_t stream_id)
{
    return receive(
        stream_id,
        [&](size_t stream_id, std::shared_ptr<ReceivedMessage> & recv_msg) {
            return msg_channels[stream_id]->tryPop(recv_msg);
        });
}

template <typename RPCContext>
ReceiveResult ExchangeReceiverBase<RPCContext>::receive(
    size_t stream_id,
    std::function<MPMCQueueResult(size_t, std::shared_ptr<ReceivedMessage> &)> recv_func)
{
    if (unlikely(stream_id >= msg_channels.size()))
    {
        auto err_msg = fmt::format("stream_id out of range, stream_id: {}, total_channel_count: {}", stream_id, msg_channels.size());
        LOG_ERROR(exc_log, err_msg);
        throw Exception(err_msg);
    }

    std::shared_ptr<ReceivedMessage> recv_msg;
    switch (recv_func(stream_id, recv_msg))
    {
    case MPMCQueueResult::OK:
        assert(recv_msg);
        return {ReceiveStatus::ok, std::move(recv_msg)};
    case MPMCQueueResult::EMPTY:
        return {ReceiveStatus::empty, nullptr};
    default:
        return {ReceiveStatus::eof, nullptr};
    }
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::toExchangeReceiveResult(
    ReceiveResult & recv_result,
    std::queue<Block> & block_queue,
    const Block & header,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    switch (recv_result.recv_status)
    {
    case ReceiveStatus::ok:
    {
        assert(recv_result.recv_msg != nullptr);
        if (unlikely(recv_result.recv_msg->error_ptr != nullptr))
            return ExchangeReceiverResult::newError(
                recv_result.recv_msg->source_index,
                recv_result.recv_msg->req_info,
                recv_result.recv_msg->error_ptr->msg());

        ExchangeReceiverMetric::subDataSizeMetric(
            data_size_in_queue,
            recv_result.recv_msg->packet->getPacket().ByteSizeLong());
        return toDecodeResult(block_queue, header, recv_result.recv_msg, decoder_ptr);
    }
    case ReceiveStatus::eof:
        return handleUnnormalChannel(block_queue, decoder_ptr);
    case ReceiveStatus::empty:
        throw Exception("Unexpected recv status: empty");
    }
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::nextResult(
    std::queue<Block> & block_queue,
    const Block & header,
    size_t stream_id,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    auto recv_res = receive(stream_id);
    return toExchangeReceiveResult(
        recv_res,
        block_queue,
        header,
        decoder_ptr);
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::handleUnnormalChannel(
    std::queue<Block> & block_queue,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    std::optional<Block> last_block = decoder_ptr->flush();
    std::lock_guard lock(mu);
    if (this->state != DB::ExchangeReceiverState::NORMAL)
    {
        return DB::ExchangeReceiverResult::newError(0, DB::ExchangeReceiverBase<RPCContext>::name, DB::constructStatusString(this->state, this->err_msg));
    }
    else
    {
        /// If there are cached data in squashDecoder, then just push the block and return EOF next iteration
        if (last_block && last_block->rows() > 0)
        {
            /// Can't get correct caller_index here, use 0 instead
            auto result = ExchangeReceiverResult::newOk(nullptr, 0, "");
            result.decode_detail.packets = 0;
            result.decode_detail.rows = last_block->rows();
            block_queue.push(std::move(last_block.value()));
            return result;
        }
        else
        {
            return DB::ExchangeReceiverResult::newEOF(DB::ExchangeReceiverBase<RPCContext>::name); /// live_connections == 0, msg_channel is finished, and state is NORMAL, that is the end.
        }
    }
}

template <typename RPCContext>
ExchangeReceiverResult ExchangeReceiverBase<RPCContext>::toDecodeResult(
    std::queue<Block> & block_queue,
    const Block & header,
    const std::shared_ptr<ReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    if (recv_msg->resp_ptr != nullptr) /// the data of the last packet is serialized from tipb::SelectResponse including execution summaries.
    {
        auto select_resp = std::make_shared<tipb::SelectResponse>();
        if (unlikely(!select_resp->ParseFromString(*(recv_msg->resp_ptr))))
        {
            return ExchangeReceiverResult::newError(recv_msg->source_index, recv_msg->req_info, "decode error");
        }
        else
        {
            auto result = ExchangeReceiverResult::newOk(select_resp, recv_msg->source_index, recv_msg->req_info);
            /// If mocking TiFlash as TiDB, we should decode chunks from select_resp.
            if (unlikely(!result.resp->chunks().empty()))
            {
                assert(recv_msg->chunks.empty());
                // Fine grained shuffle should only be enabled when sending data to TiFlash node.
                // So all data should be encoded into MPPDataPacket.chunks.
                RUNTIME_CHECK_MSG(!enable_fine_grained_shuffle_flag, "Data should not be encoded into tipb::SelectResponse.chunks when fine grained shuffle is enabled");
                result.decode_detail = CoprocessorReader::decodeChunks(select_resp, block_queue, header, schema);
            }
            else if (!recv_msg->chunks.empty())
            {
                result.decode_detail = decodeChunks(recv_msg, block_queue, decoder_ptr);
            }
            return result;
        }
    }
    else /// the non-last packets
    {
        auto result = ExchangeReceiverResult::newOk(nullptr, recv_msg->source_index, recv_msg->req_info);
        result.decode_detail = decodeChunks(recv_msg, block_queue, decoder_ptr);
        return result;
    }
}

template <typename RPCContext>
bool ExchangeReceiverBase<RPCContext>::setEndState(ExchangeReceiverState new_state)
{
    assert(new_state == ExchangeReceiverState::CANCELED || new_state == ExchangeReceiverState::CLOSED);
    std::lock_guard lock(mu);
    if (state == ExchangeReceiverState::CANCELED || state == ExchangeReceiverState::CLOSED)
    {
        return false;
    }
    state = new_state;
    return true;
}

template <typename RPCContext>
String ExchangeReceiverBase<RPCContext>::getStatusString()
{
    std::lock_guard lock(mu);
    return constructStatusString(state, err_msg);
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::connectionDone(
    bool meet_error,
    const String & local_err_msg,
    const LoggerPtr & log)
{
    Int32 copy_live_connections;
    String first_err_msg = local_err_msg;
    {
        std::lock_guard lock(mu);

        if (meet_error)
        {
            if (state == ExchangeReceiverState::NORMAL)
                state = ExchangeReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
            else
                first_err_msg = err_msg;
        }

        copy_live_connections = --live_connections;
    }

    if (meet_error)
    {
        LOG_WARNING(
            log,
            "connection end. meet error: {}, err msg: {}, current alive connections: {}",
            meet_error,
            local_err_msg,
            copy_live_connections);
    }
    else
    {
        LOG_DEBUG(
            log,
            "connection end. Current alive connections: {}",
            copy_live_connections);
    }
    assert(copy_live_connections >= 0);
    if (copy_live_connections == 0)
    {
        LOG_DEBUG(log, "All threads end in ExchangeReceiver");
        cv.notify_all();
    }

    if (meet_error || copy_live_connections == 0)
    {
        auto log_level = meet_error ? Poco::Message::PRIO_WARNING : Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(exc_log, log_level, "Finish receiver channels, meet error: {}, error message: {}", meet_error, first_err_msg);
        finishAllMsgChannels();
    }
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::connectionLocalDone()
{
    std::lock_guard lock(mu);
    --live_local_connections;
    cv.notify_all();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::finishAllMsgChannels()
{
    for (auto & msg_channel : msg_channels)
        msg_channel->finish();
}

template <typename RPCContext>
void ExchangeReceiverBase<RPCContext>::cancelAllMsgChannels()
{
    for (auto & msg_channel : msg_channels)
        msg_channel->cancel();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ExchangeReceiverBase<GRPCReceiverContext>;

} // namespace DB
