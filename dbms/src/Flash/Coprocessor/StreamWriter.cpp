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

#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <common/logger_useful.h>

namespace DB
{
StreamWriter::StreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_, UInt64 queue_buffer_size)
    : writer(writer_)
    , connected(true)
    , finished(false)
    , send_queue(std::max(1, queue_buffer_size)) // min is 1
    , total_wait_push_channel_elapse_ms(0)
    , total_wait_pull_channel_elapse_ms(0)
    , total_wait_net_elapse_ms(0)
    , total_net_send_bytes(0)
    , thread_manager(newThreadManager())
    , log(Logger::get("StreamWriter"))
{
    thread_manager->schedule(true, "StreamWriter", [this] {
        sendJob();
    });
}

StreamWriter::~StreamWriter()
{
    try
    {
        {
            std::unique_lock lock(mu);
            if (finished)
            {
                LOG_FMT_TRACE(log, "already finished!");
                return;
            }

            // make sure to finish the stream writer after it is connected
            waitUntilConnectedOrFinished(lock);
            finishSendQueue();
        }
        LOG_FMT_TRACE(log, "waiting consumer finish!");
        waitForConsumerFinish(/*allow_throw=*/false);
        LOG_FMT_TRACE(log, "waiting child thread finished!");
        thread_manager->wait();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in destructor function of StreamWriter");
    }
    LOG_FMT_DEBUG(
        log,
        "done, wait_pull_channel_ms={} wait_push_channel_ms={} wait_net_ms={} net_send_bytes={}",
        total_wait_pull_channel_elapse_ms,
        total_wait_push_channel_elapse_ms,
        total_wait_net_elapse_ms,
        total_net_send_bytes);
}

void StreamWriter::write(tipb::SelectResponse & response, [[maybe_unused]] uint16_t id)
{
    auto rsp = std::make_shared<::coprocessor::BatchResponse>();
    if (!response.SerializeToString(rsp->mutable_data()))
        throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));

    {
        {
            std::unique_lock lock(mu);
            waitUntilConnectedOrFinished(lock);
        }
        if (finished)
            throw Exception("write to StreamWriter which is already closed, " + consumer_state.getError());

        watch.restart();
        if (send_queue.push(rsp))
        {
            connection_profile_info.bytes += rsp->ByteSizeLong();
            connection_profile_info.packets += 1;
            return;
        }
        total_wait_push_channel_elapse_ms += watch.elapsedMilliseconds();
    }
    // push failed, wait consumer for the final state
    waitForConsumerFinish(/*allow_throw=*/true);
}

void StreamWriter::writeDone()
{
    LOG_FMT_TRACE(log, "ready to finish");
    {
        std::unique_lock lock(mu);
        if (finished)
            throw Exception("write to StreamWriter which is already closed, " + consumer_state.getError());
        waitUntilConnectedOrFinished(lock);
        finishSendQueue();
    }
    waitForConsumerFinish(/*allow_throw=*/true);
}

void StreamWriter::sendJob()
{
    Stopwatch send_watch;
    String err_msg;
    try
    {
        BatchResponsePtr rsp;
        send_watch.restart();
        while (send_queue.pop(rsp))
        {
            total_wait_pull_channel_elapse_ms += send_watch.elapsedMilliseconds();
            total_net_send_bytes += rsp->ByteSizeLong();
            send_watch.restart();
            if (!writer->Write(*rsp))
            {
                err_msg = "grpc writes failed.";
                break;
            }
            total_wait_net_elapse_ms += send_watch.elapsedMilliseconds();
        }
    }
    catch (Exception & e)
    {
        err_msg = e.message();
    }
    catch (std::exception & e)
    {
        err_msg = e.what();
    }
    catch (...)
    {
        err_msg = fmt::format("fatal error in {}", __PRETTY_FUNCTION__);
    }
    if (!err_msg.empty())
        LOG_ERROR(log, err_msg);
    consumerFinish(err_msg);
}

void StreamWriter::waitForConsumerFinish(bool allow_throw)
{
    LOG_FMT_TRACE(log, "start wait for consumer finish!");
    String err_msg = consumer_state.getError(); // may blocking
    if (allow_throw && !err_msg.empty())
        throw Exception("Consumer exits unexpected, " + err_msg);
    LOG_FMT_TRACE(log, "end wait for consumer finish!");
}

void StreamWriter::consumerFinish(const String & err_msg)
{
    // must finish send_queue outside of the critical area to avoid deadlock with write.
    LOG_FMT_TRACE(log, "calling consumer finish");
    send_queue.finish();
    {
        std::unique_lock lock(mu);
        if (finished && consumer_state.errHasSet())
            return;
        finished = true;
        consumer_state.setError(err_msg);
        cv_for_finished.notify_all();
    }
}

void StreamWriter::waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk)
{
    LOG_FMT_TRACE(log, "start waitUntilConnectedOrFinished");
    cv_for_finished.wait(lk, [&] { return connected || finished; });
    LOG_FMT_TRACE(log, "end waitUntilConnectedOrFinished");
}
} // namespace DB
