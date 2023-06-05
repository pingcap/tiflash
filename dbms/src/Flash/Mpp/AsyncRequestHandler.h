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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/GRPCReceiveQueue.h>
#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/ReceiverChannelTryWriter.h>
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
    WAIT_BATCH_READ,
    WAIT_FINISH,
    WAIT_RETRY,
    WAIT_REWRITE,
    FINISHED,
};

using SystemClock = std::chrono::system_clock;

constexpr Int32 max_retry_times = 10;
constexpr Int32 retry_interval_time = 1; // second
constexpr Int32 batch_packet_count = 16;

class AsyncRequestHandlerBase : public UnaryCallback<bool>
{
public:
    virtual void wait() = 0;
};

template <typename RPCContext>
class AsyncRequestHandler : public AsyncRequestHandlerBase
{
public:
    using Status = typename RPCContext::Status;
    using Request = typename RPCContext::Request;
    using AsyncReader = typename RPCContext::AsyncReader;
    using Self = AsyncRequestHandler<RPCContext>;

    AsyncRequestHandler(
        ReceivedMessageQueue * received_message_queue_,
        AsyncRequestHandlerWaitQueuePtr async_wait_rewrite_queue_,
        const std::shared_ptr<RPCContext> & context,
        Request && req,
        const String & req_id,
        std::atomic<Int64> * data_size_in_queue,
        std::function<void(bool, const String &, const LoggerPtr &)> && close_conn_)
        : cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , rpc_context(context)
        , request(std::move(req))
        , req_info(fmt::format("async tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , has_data(false)
        , retry_times(0)
        , stage(AsyncRequestStage::NEED_INIT)
        , read_packet_index(0)
        , received_packet_index(0)
        , finish_status(RPCContext::getStatusOK())
        , log(Logger::get(req_id, req_info))
        , channel_try_writer(received_message_queue_, req_info, log, data_size_in_queue, ReceiverMode::Async)
        , async_wait_rewrite_queue(std::move(async_wait_rewrite_queue_))
        , kick_recv_tag(thisAsUnaryCallback())
        , close_conn(std::move(close_conn_))
        , is_close_conn_called(false)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_exception_when_construct_async_request_handler);
        packets.resize(batch_packet_count);
        for (auto & p : packets)
            p = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
        start();
    }

    // execute will be called by RPC framework so it should be as light as possible.
    // Do not do anything after processXXX functions.
    void execute(bool & ok) override
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
            case AsyncRequestStage::WAIT_BATCH_READ:
                processWaitBatchRead(ok);
                break;
            case AsyncRequestStage::WAIT_REWRITE:
                processWaitReWrite();
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

    void wait() override
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
            startAsyncRead();
        }
    }

    void processWaitBatchRead(bool ok)
    {
        if (ok)
            ++read_packet_index;

        if (!ok || read_packet_index == batch_packet_count || packets[read_packet_index - 1]->hasError())
            processReceivedData();
        else
            reader->read(packets[read_packet_index], thisAsUnaryCallback());
    }

    void processWaitFinish()
    {
        if (likely(finish_status.ok()))
            closeConnection("");
        else
        {
            String done_msg = fmt::format("Exchange receiver meet error : {}", finish_status.error_message());
            String log_msg = fmt::format("Finish fail. err code: {}, err msg: {}, retry time {}", finish_status.error_code(), finish_status.error_message(), retry_times);
            retryOrDone(std::move(done_msg), log_msg);
        }
    }

    void startAsyncRead()
    {
        stage = AsyncRequestStage::WAIT_BATCH_READ;
        read_packet_index = 0;
        received_packet_index = 0;
        reader->read(packets[read_packet_index], thisAsUnaryCallback());
    }

    void processReceivedData()
    {
        LOG_TRACE(log, "Received {} packets.", read_packet_index);

        String err_info;
        if (read_packet_index > 0)
            has_data = true;

        if (auto error_message = getErrorFromPackets(); unlikely(!error_message.empty()))
        {
            closeConnection(fmt::format("Exchange receiver meet error : {}", error_message));
            return;
        }

        GRPCReceiveQueueRes res = sendPackets();

        if (!checkResultAfterSendingPackets(res))
            return;

        startAsyncRead();
    }

    void processWaitReWrite()
    {
        GRPCReceiveQueueRes res = reSendPackets();

        if (!checkResultAfterSendingPackets(res))
            return;

        stage = AsyncRequestStage::WAIT_BATCH_READ;
        startAsyncRead();
    }

    // return true if we still need to receive data from server.
    bool checkResultAfterSendingPackets(GRPCReceiveQueueRes res)
    {
        if (unlikely(sendPacketHasError(res)))
        {
            closeConnection("Exchange receiver meet error : push packets fail");
            return false;
        }

        if (unlikely(isChannelFull(res)))
        {
            asyncWaitForRewrite();
            return false;
        }

        if (read_packet_index < batch_packet_count)
        {
            stage = AsyncRequestStage::WAIT_FINISH;
            reader->finish(finish_status, thisAsUnaryCallback());
            return false;
        }

        return true;
    }

    String getErrorFromPackets()
    {
        // step 1: check if there is error packet
        // only the last packet may has error, see execute().
        if (read_packet_index != 0 && packets[read_packet_index - 1]->hasError())
            return packets[read_packet_index - 1]->error();
        // step 2: check memory overflow error
        for (Int32 i = 0; i < read_packet_index; ++i)
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

    void closeConnection(String && msg)
    {
        std::lock_guard lock(is_close_conn_called_mu);
        if (!is_close_conn_called)
        {
            stage = AsyncRequestStage::FINISHED;
            close_conn(!msg.empty(), msg, log);
            closeGrpcConnection();
            is_close_conn_called = true;
        }
        condition_cv.notify_all();
    }

    void start()
    {
        stage = AsyncRequestStage::WAIT_MAKE_READER;

        // Use lock to ensure async reader is unreachable from grpc thread before this function returns
        std::lock_guard lock(make_reader_mu);
        reader = rpc_context->makeAsyncReader(request, cq, thisAsUnaryCallback());
    }

    void retryOrDone(String && done_msg, const String & log_msg)
    {
        LOG_WARNING(log, log_msg);
        if (retriable())
        {
            ++retry_times;
            stage = AsyncRequestStage::WAIT_RETRY;

            // Let alarm put me into CompletionQueue after a while
            // , so that we can try to connect again.
            alarm.Set(cq, SystemClock::now() + std::chrono::seconds(retry_interval_time), this);
        }
        else
        {
            closeConnection(std::move(done_msg));
        }
    }

    GRPCReceiveQueueRes sendPackets()
    {
        try
        {
            // note: no exception should be thrown rudely, since it's called by a GRPC poller.
            while (received_packet_index < read_packet_index)
            {
                auto & p = packets[received_packet_index++];
                auto res = channel_try_writer.tryWrite(request.source_index, p);
                if (res == GRPCReceiveQueueRes::FULL)
                {
                    p = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
                    return res;
                }
                if (res != GRPCReceiveQueueRes::OK)
                    return res;

                // can't reuse packet since it is sent to readers.
                p = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
            }
            return GRPCReceiveQueueRes::OK;
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            RUNTIME_ASSERT(false, "Exception is thrown when sending packets in AsyncRequestHandler");
        }
    }

    GRPCReceiveQueueRes reSendPackets()
    {
        GRPCReceiveQueueRes res = channel_try_writer.tryReWrite();
        if (res != GRPCReceiveQueueRes::OK)
            return res;
        return sendPackets();
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
        bool res = async_wait_rewrite_queue->push(std::make_pair(&kick_recv_tag, reader->getClientContext()->c_call()));
        if (!res)
            closeConnection("AsyncRequestHandlerWaitQueue has been closed");
    }

    void closeGrpcConnection()
    {
        reader.reset();
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

    Int32 read_packet_index;
    Int32 received_packet_index;
    TrackedMppDataPacketPtrs packets;

    std::unique_ptr<AsyncReader> reader;
    TrackedMppDataPacketPtr packet;
    Status finish_status;
    LoggerPtr log;

    ReceiverChannelTryWriter channel_try_writer;
    AsyncRequestHandlerWaitQueuePtr async_wait_rewrite_queue;
    KickReceiveTag kick_recv_tag;

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
