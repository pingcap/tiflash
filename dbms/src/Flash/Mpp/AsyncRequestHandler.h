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

#pragma once

#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/ReceiverChannelTryWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <common/defines.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_READ,
    WAIT_FINISH,
    WAIT_RETRY,
    WAIT_REWRITE,
    FINISHED,
};

using Clock = std::chrono::system_clock;

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
        std::vector<GRPCReceiveQueue<ReceivedMessage>> * grpc_recv_queues_,
        AsyncRequestHandlerWaitQueuePtr async_wait_rewrite_queue_,
        const std::shared_ptr<RPCContext> & context,
        Request && req,
        const String & req_id,
        std::atomic<Int64> * data_size_in_queue,
        std::function<void(bool, const String &, const LoggerPtr &)> && close_conn_)
        : cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , rpc_context(context)
        , request(std::move(req))
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , has_data(false)
        , retry_times(0)
        , stage(AsyncRequestStage::NEED_INIT)
        , finish_status(RPCContext::getStatusOK())
        , log(Logger::get(req_id, req_info))
        , channel_try_writer(grpc_recv_queues_, req_info, log, data_size_in_queue, ReceiverMode::Async)
        , async_wait_rewrite_queue(std::move(async_wait_rewrite_queue_))
        , kick_recv_tag(thisAsUnaryCallback())
        , close_conn(std::move(close_conn_))
    {
        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    // Do not do anything after processXXX functions.
    void execute(bool & ok) override
    {
        switch (stage)
        {
        case AsyncRequestStage::WAIT_RETRY:
            // debug
            LOG_INFO(log, "Profiling: enter WAIT_RETRY");
            start();
            break;
        case AsyncRequestStage::WAIT_MAKE_READER:
            // debug
            LOG_INFO(log, "Profiling: enter WAIT_MAKE_READER");
            processWaitMakeReader(ok);
            break;
        case AsyncRequestStage::WAIT_READ:
            // debug
            LOG_INFO(log, "Profiling: enter WAIT_READ");
            processWaitRead(ok);
            break;
        case AsyncRequestStage::WAIT_REWRITE:
            // debug
            LOG_INFO(log, "Profiling: enter WAIT_REWRITE");
            processWaitReWrite();
            break;
        case AsyncRequestStage::WAIT_FINISH:
            // debug
            LOG_INFO(log, "Profiling: enter WAIT_FINISH");
            processWaitFinish();
            break;
        default:
            __builtin_unreachable();
        }
    }

private:
    void processWaitMakeReader(bool ok)
    {
        // Use lock to ensure reader is created already in reactor thread
        std::lock_guard lock(mu);
        if (!ok)
        {
            reader.reset();
            LOG_WARNING(log, "MakeReader fail. retry time: {}", retry_times);
            retryOrDone("Exchange receiver meet error : send async stream request fail");
        }
        else
        {
            stage = AsyncRequestStage::WAIT_READ;
            startAsyncRead();
        }
    }

    void processWaitRead(bool ok)
    {
        if (unlikely(!ok))
        {
            stage = AsyncRequestStage::WAIT_FINISH;
            reader->finish(finish_status, thisAsUnaryCallback());
            return;
        }

        has_data = true;

        if (auto error_message = getErrorFromPacket(); unlikely(!error_message.empty()))
        {
            closeConnection(fmt::format("Exchange receiver meet error : {}", error_message));
            return;
        }

        GRPCReceiveQueueRes send_res = sendPacket();

        if (unlikely(sendPacketHasError(send_res)))
        {
            closeConnection("Exchange receiver meet error : push packets fail");
            return;
        }

        if (unlikely(isChannelFull(send_res)))
        {
            // debug
            LOG_INFO(log, "Profiling: channel full...");
            asyncWaitForRewrite();
            return;
        }

        startAsyncRead();
    }

    void processWaitReWrite()
    {
        GRPCReceiveQueueRes res = channel_try_writer.tryReWrite<enable_fine_grained_shuffle>();

        if (unlikely(sendPacketHasError(res)))
        {
            closeConnection("Exchange receiver meet error : push packets fail");
            return;
        }

        if (unlikely(isChannelFull(res)))
        {
            // debug
            LOG_INFO(log, "Profiling: rewrite full again");
            asyncWaitForRewrite();
            return;
        }

        // debug
        LOG_INFO(log, "Profiling: rewrite successfully");
        stage = AsyncRequestStage::WAIT_READ;
        startAsyncRead();
    }

    void processWaitFinish()
    {
        if (likely(finish_status.ok()))
            closeConnection("");
        else
        {
            // As AsyncRequestHandler may have been destructed after close_conn is called;
            // we need to copy some data for LOG_WARNING after a while.
            LoggerPtr copy_log = log;
            Status copy_finish_status = finish_status;
            int copy_retry_times = retry_times;

            if (!retryOrDone(fmt::format("Exchange receiver meet error : {}", finish_status.error_message())))
            {
                LOG_WARNING(
                    copy_log,
                    "Finish fail. err code: {}, err msg: {}, retry time {}",
                    copy_finish_status.error_code(),
                    copy_finish_status.error_message(),
                    copy_retry_times);
            }
        }
    }

    void startAsyncRead()
    {
        packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
        reader->read(packet, thisAsUnaryCallback());
    }

    String getErrorFromPacket()
    {
        if (unlikely(packet->hasError()))
            return packet->error();

        packet->recomputeTrackedMem();
        if (unlikely(packet->hasError()))
            return packet->error();

        return "";
    }

    bool retriable() const
    {
        return !has_data && retry_times + 1 < max_retry_times;
    }

    void closeConnection(String && msg)
    {
        stage = AsyncRequestStage::FINISHED;
        close_conn(!msg.empty(), msg, log);
    }

    void start()
    {
        stage = AsyncRequestStage::WAIT_MAKE_READER;

        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::lock_guard lock(mu);
        rpc_context->makeAsyncReader(request, reader, cq, thisAsUnaryCallback());
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
            closeConnection(std::move(done_msg));
            return false;
        }
    }

    GRPCReceiveQueueRes sendPacket()
    {
        return channel_try_writer.tryWrite<enable_fine_grained_shuffle>(request.source_index, packet);
    }

    bool sendPacketHasError(GRPCReceiveQueueRes res)
    {
        return (res == GRPCReceiveQueueRes::CANCELLED || res == GRPCReceiveQueueRes::FINISHED);
    }

    bool isChannelFull(GRPCReceiveQueueRes res)
    {
        return res == GRPCReceiveQueueRes::FULL;
    }

    // in case of potential multiple inheritances.
    UnaryCallback<bool> * thisAsUnaryCallback()
    {
        return static_cast<UnaryCallback<bool> *>(this);
    }

    // Do not do anything after this function
    void asyncWaitForRewrite()
    {
        // Do not change the order of these two clauses.
        // We must ensure that status is set before pushing async handler into queue
        stage = AsyncRequestStage::WAIT_REWRITE;
        async_wait_rewrite_queue->push(std::make_pair(&kick_recv_tag, reader->getClientContext()->c_call()));
    }

    // won't be null and do not delete this pointer
    grpc::CompletionQueue * cq;

    std::shared_ptr<RPCContext> rpc_context;
    grpc::Alarm alarm{};
    Request request;

    String req_info;
    bool has_data;
    int retry_times;
    AsyncRequestStage stage;

    std::shared_ptr<AsyncReader> reader;
    TrackedMppDataPacketPtr packet;
    Status finish_status;
    LoggerPtr log;

    ReceiverChannelTryWriter channel_try_writer;
    AsyncRequestHandlerWaitQueuePtr async_wait_rewrite_queue;
    KickReceiveTag kick_recv_tag;

    std::mutex mu;

    // Do not use any variable in AsyncRequestHandler after close_conn is called,
    // because AsyncRequestHandler may have been destructed by ExchangeReceiver at that time.
    std::function<void(bool, const String &, const LoggerPtr &)> close_conn;
};
} // namespace DB
