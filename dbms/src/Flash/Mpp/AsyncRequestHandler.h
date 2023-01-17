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

#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Mpp/ReceiverChannelWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <grpcpp/alarm.h>
#include <grpcpp/completion_queue.h>
#include "common/defines.h"

namespace DB
{
enum class AsyncRequestStage
{
    NEED_INIT,
    WAIT_MAKE_READER,
    WAIT_READ,
    WAIT_FINISH,
    WAIT_RETRY,
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
        std::vector<MsgChannelPtr> * msg_channels_,
        const std::shared_ptr<RPCContext> & context,
        Request && req,
        const String & req_id,
        std::atomic<Int64> * data_size_in_queue,
        std::function<void()> && add_live_conn)
        : rpc_context(context)
        , cq(&(GRPCCompletionQueuePool::global_instance->pickQueue()))
        , request(std::move(req))
        , req_info(fmt::format("tunnel{}+{}", req.send_task_id, req.recv_task_id))
        , meet_error(false)
        , has_data(false)
        , retry_times(0)
        , stage(AsyncRequestStage::NEED_INIT)
        , finish_status(RPCContext::getStatusOK())
        , log(Logger::get(req_id, req_info))
        , channel_writer(msg_channels_, req_info, log, data_size_in_queue, ReceiverMode::Async)
    {
        packet = std::make_shared<TrackedMppDataPacket>();
        add_live_conn();
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
            processWaitMakeReader(ok);
            break;
        case AsyncRequestStage::WAIT_READ:
            processWaitRead(ok);
            break;
        case AsyncRequestStage::WAIT_FINISH:
            processWaitFinish();
            break;
        default:
            __builtin_unreachable();
        }
    }

    // TODO remove it
    // handle will be called by ExchangeReceiver::reactor.
    // void handle()
    // {
    //     std::string err_info;
    //     LOG_TRACE(log, "stage: {}", magic_enum::enum_name(stage));
    //     switch (stage)
    //     {
    //     case AsyncRequestStage::WAIT_READ:
    //         LOG_TRACE(log, "Received {} packets.", read_packet_index);
    //         if (read_packet_index > 0)
    //             has_data = true;
    //         if (auto error_message = getErrorFromPackets(); !error_message.empty())
    //             setDone(fmt::format("Exchange receiver meet error : {}", error_message));
    //         else if (!sendPackets())
    //             setDone("Exchange receiver meet error : push packets fail");
    //         else if (read_packet_index < batch_packet_count)
    //         {
    //             stage = AsyncRequestStage::WAIT_FINISH;
    //             reader->finish(finish_status, thisAsUnaryCallback());
    //         }
    //         else
    //         {
    //             read_packet_index = 0;
    //             reader->read(packets[0], thisAsUnaryCallback());
    //         }
    //         break;
    //     case AsyncRequestStage::WAIT_FINISH:
    //         if (finish_status.ok())
    //             setDone("");
    //         else
    //         {
    //             LOG_WARNING(
    //                 log,
    //                 "Finish fail. err code: {}, err msg: {}, retry time {}",
    //                 finish_status.error_code(),
    //                 finish_status.error_message(),
    //                 retry_times);
    //             retryOrDone(fmt::format("Exchange receiver meet error : {}", finish_status.error_message()));
    //         }
    //         break;
    //     default:
    //         __builtin_unreachable();
    //     }
    // }

    bool finished() const
    {
        return stage == AsyncRequestStage::FINISHED;
    }

    bool meetError() const { return meet_error; }
    const String & getErrMsg() const { return err_msg; }
    const LoggerPtr & getLog() const { return log; }

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
            reader->read(packet, thisAsUnaryCallback());
        }
    }

    void processWaitRead(bool ok)
    {
        if (!ok)
        {
            stage = AsyncRequestStage::WAIT_FINISH;
            reader->finish(finish_status, thisAsUnaryCallback());
            return;
        }

        has_data = true;

        if (auto error_message = getErrorFromPacket(); unlikely(!error_message.empty()))
        {
            setDone(fmt::format("Exchange receiver meet error : {}", error_message));
            return;
        }

        // TODO write here
        if (unlikely(!sendPacket()))
        {
            setDone("Exchange receiver meet error : push packets fail");
            return;
        }

        reader->read(packet, thisAsUnaryCallback());
        // TODO remove it
        // if (!ok packets[read_packet_index - 1]->hasError())
        //     notifyReactor();
        // else
        //     reader->read(packets[read_packet_index], thisAsUnaryCallback());
    }

    void processWaitFinish()
    {
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
    }

    String getErrorFromPacket()
    {
        if (unlikely(packet->hasError()))
            return packet->error();

        packet->recomputeTrackedMem();
        if (unlikely(packet->hasError()))
            return packet->error();

        // TODO remove it
        // // step 1: check if there is error packet
        // // only the last packet may has error, see execute().
        // if (read_packet_index != 0 && packets[read_packet_index - 1]->hasError())
        //     return packets[read_packet_index - 1]->error();
        // // step 2: check memory overflow error
        // for (size_t i = 0; i < read_packet_index; ++i)
        // {
        //     packets[i]->recomputeTrackedMem();
        //     if (packets[i]->hasError())
        //         return packets[i]->error();
        // }
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

    bool sendPacket()
    {
        // TODO write here
        packet = std::make_shared<TrackedMppDataPacket>();
        return true; // TODO reconsider it
    }

    // in case of potential multiple inheritances.
    UnaryCallback<bool> * thisAsUnaryCallback()
    {
        return static_cast<UnaryCallback<bool> *>(this);
    }

    // won't be null and do not delete this pointer
    grpc::CompletionQueue * cq;

    std::shared_ptr<RPCContext> rpc_context;
    grpc::Alarm alarm{};
    Request request;

    String req_info;
    bool meet_error;
    bool has_data;
    String err_msg;
    int retry_times;
    AsyncRequestStage stage;

    std::shared_ptr<AsyncReader> reader;
    TrackedMppDataPacketPtr packet;
    Status finish_status;
    LoggerPtr log;
    ReceiverChannelWriter channel_writer;
    std::mutex mu;
};
} // namespace DB
