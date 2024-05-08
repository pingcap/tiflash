// Copyright 2023 PingCAP, Inc.
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
#include <Common/FailPoint.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/ReceivedMessageQueue.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/defines.h>
#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>

#include <condition_variable>
#include <memory>
#include <mutex>

namespace DB
{
namespace FailPoints
{
extern const char random_exception_when_construct_async_request_handler[];
} // namespace FailPoints

enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_READ,
    WAIT_PUSH_TO_QUEUE,
    WAIT_FINISH,
    WAIT_RETRY,
    FINISHED,
};

using SystemClock = std::chrono::system_clock;

constexpr Int32 max_retry_times = 10;
constexpr Int32 retry_interval_time = 1; // second

template <typename RPCContext>
class AsyncRequestHandler final : public GRPCKickTag
{
public:
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;
    using Self = AsyncRequestHandler<RPCContext>;

    AsyncRequestHandler(
        ReceivedMessageQueue * received_message_queue_,
        const std::shared_ptr<RPCContext> & context,
        Request && req,
        const String & req_id,
        std::function<void(bool, const String &, const LoggerPtr &)> && close_conn_)
        : cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , rpc_context(context)
        , request(std::move(req))
        , req_info(fmt::format("async tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , has_data(false)
        , retry_times(0)
        , stage(AsyncRequestStage::NEED_INIT)
        , finish_status(RPCContext::getStatusOK())
        , log(Logger::get(req_id, req_info))
        , message_queue(received_message_queue_)
        , close_conn(std::move(close_conn_))
        , is_close_conn_called(false)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_exception_when_construct_async_request_handler);
        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    // Do not do anything after processXXX functions.
    void execute(bool ok) override
    {
        try
        {
            switch (stage)
            {
            case AsyncRequestStage::WAIT_RETRY:
                start();
                break;
            case AsyncRequestStage::WAIT_MAKE_READER:
                processWaitMakeReader(ok);
                break;
            case AsyncRequestStage::WAIT_READ:
                processWaitRead(ok);
                break;
            case AsyncRequestStage::WAIT_PUSH_TO_QUEUE:
                processWaitPushToQueue(ok);
                break;
            case AsyncRequestStage::WAIT_FINISH:
                processWaitFinish();
                break;
            default:
                RUNTIME_ASSERT(false, "Unexpected stage {}", magic_enum::enum_name(stage));
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            closeConnection("Exception is thrown in AsyncRequestHandler");
        }
    }

    void wait()
    {
        std::unique_lock<std::mutex> ul(is_close_conn_called_mu);
        condition_cv.wait(ul, [this]() { return is_close_conn_called; });
    }

private:
    void processWaitMakeReader(bool ok)
    {
        // Use lock to ensure reader is created already in reactor thread
        std::lock_guard lock(make_reader_mu);
        if (!ok)
        {
            reader.reset();
            retryOrDone(
                "Exchange receiver meet error : send async stream request fail",
                fmt::format("Make reader fail. retry time: {}", retry_times));
        }
        else
        {
            setCall(reader->getClientContext()->c_call());
            startAsyncRead();
        }
    }

    void processWaitRead(bool ok)
    {
        if (!ok)
        {
            stage = AsyncRequestStage::WAIT_FINISH;
            reader->finish(finish_status, asGRPCKickTag());
            return;
        }

        has_data = true;
        // Check memory overflow error
        packet->recomputeTrackedMem();
        if unlikely (packet->hasError())
        {
            closeConnection(fmt::format("Exchange receiver meet error : {}", packet->error()));
            return;
        }

        // Need to set the stage first to avoid data race problem
        stage = AsyncRequestStage::WAIT_PUSH_TO_QUEUE;
        auto res = message_queue->pushAsyncGRPCPacket(request.source_index, req_info, packet, asGRPCKickTag());
        switch (res)
        {
        case MPMCQueueResult::OK:
            startAsyncRead();
            return;
        case MPMCQueueResult::FULL:
            // Do nothing and return immediately
            return;
        default:
            closeConnection("Exchange receiver meet error : push packet fail");
            return;
        }
    }

    void processWaitPushToQueue(bool ok)
    {
        if (!ok)
        {
            closeConnection("Exchange receiver meet error : push packet fail");
            return;
        }

        startAsyncRead();
    }

    void processWaitFinish()
    {
        if (likely(finish_status.ok()))
            closeConnection("");
        else
        {
            String done_msg = fmt::format("Exchange receiver meet error : {}", finish_status.error_message());
            String log_msg = fmt::format(
                "Finish fail. err code: {}, err msg: {}, retry time {}",
                magic_enum::enum_name(finish_status.error_code()),
                finish_status.error_message(),
                retry_times);
            retryOrDone(std::move(done_msg), log_msg);
        }
    }

    void startAsyncRead()
    {
        stage = AsyncRequestStage::WAIT_READ;
        packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
        reader->read(packet, asGRPCKickTag());
    }

    void closeConnection(String && msg)
    {
        std::lock_guard lock(is_close_conn_called_mu);
        if (!is_close_conn_called)
        {
            stage = AsyncRequestStage::FINISHED;
            close_conn(!msg.empty(), msg, log);
            reader.reset();
            is_close_conn_called = true;
        }
        condition_cv.notify_all();
    }

    void start()
    {
        stage = AsyncRequestStage::WAIT_MAKE_READER;

        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::lock_guard lock(make_reader_mu);
        reader = rpc_context->makeAsyncReader(request, cq, asGRPCKickTag());
    }

    void retryOrDone(String && done_msg, const String & log_msg)
    {
        LOG_WARNING(log, log_msg);
        if (!has_data && retry_times + 1 < max_retry_times)
        {
            ++retry_times;
            stage = AsyncRequestStage::WAIT_RETRY;

            // Let alarm put me into CompletionQueue after a while
            // , so that we can try to connect again.
            alarm.Set(cq, SystemClock::now() + std::chrono::seconds(retry_interval_time), asGRPCKickTag());
        }
        else
        {
            closeConnection(std::move(done_msg));
        }
    }

    // won't be null and do not delete this pointer
    grpc::CompletionQueue * cq;

    std::shared_ptr<RPCContext> rpc_context;
    grpc::Alarm alarm{};
    Request request;

    String req_info;
    bool has_data;
    size_t retry_times;
    AsyncRequestStage stage;

    std::unique_ptr<AsyncReader> reader;
    TrackedMppDataPacketPtr packet;
    Status finish_status;
    LoggerPtr log;

    ReceivedMessageQueue * message_queue;

    std::mutex make_reader_mu;

    // Do not use any variable in AsyncRequestHandler after close_conn is called,
    // because AsyncRequestHandler may have been destructed by ExchangeReceiver after close_conn is called.
    std::function<void(bool, const String &, const LoggerPtr &)> close_conn;

    // try-catch may call close_conn, and we need to ensure that close_conn is called for only once.
    bool is_close_conn_called;

    std::mutex is_close_conn_called_mu;
    std::condition_variable condition_cv;
};
} // namespace DB
