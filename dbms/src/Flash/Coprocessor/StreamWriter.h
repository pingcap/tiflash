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
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Common/nocopyable.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>

#include <condition_variable>
#include <exception>
#include <future>
#include <mutex>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/impl/codegen/sync_stream.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace mpp
{
class MPPDataPacket;
} // namespace mpp

namespace DB
{
struct StreamWriter final
{
    explicit StreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_, UInt64 queue_buffer_size);

    ~StreamWriter();

    static void write(mpp::MPPDataPacket &)
    {
        throw Exception("StreamWriter::write(mpp::MPPDataPacket &) do not support writing MPPDataPacket!");
    }

    static void write(mpp::MPPDataPacket &, uint16_t)
    {
<<<<<<< HEAD
        throw Exception("StreamWriter::write(mpp::MPPDataPacket &, uint16_t) do not support writing MPPDataPacket!");
=======
        throw Exception("StreamWriter::write(mpp::MPPDataPacket &, [[maybe_unused]] uint16_t) do not support writing MPPDataPacket!");
    }
    void write(tipb::SelectResponse & response, [[maybe_unused]] uint16_t id = 0)
    {
        ::coprocessor::BatchResponse resp;
        if (!response.SerializeToString(resp.mutable_data()))
            throw Exception("[StreamWriter]Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
        std::lock_guard lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
>>>>>>> 67b5e876eb945dea5fbbd94a2e114d6b0b763dcc
    }

    void write(tipb::SelectResponse & response, uint16_t id = 0);

    void writeDone();

    // a helper function
    static uint16_t getPartitionNum() { return 0; }

    DISALLOW_COPY_AND_MOVE(StreamWriter);

private:
    // work as a background task to keep sending packets until done.
    void sendJob();

    void waitForConsumerFinish(bool allow_throw);

    void consumerFinish(const String & err_msg);

    void finishSendQueue()
    {
        send_queue.finish();
    }

    void waitUntilConnectedOrFinished(std::unique_lock<std::mutex> & lk);

private:
    std::mutex mu;
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    std::condition_variable cv_for_finished;
    bool connected;
    bool finished;
    using BatchResponsePtr = std::shared_ptr<::coprocessor::BatchResponse>;
    MPMCQueue<BatchResponsePtr> send_queue;
    Stopwatch watch;
    size_t total_wait_push_channel_elapse_ms;
    size_t total_wait_pull_channel_elapse_ms;
    size_t total_wait_net_elapse_ms;
    size_t total_net_send_bytes;

    std::shared_ptr<ThreadManager> thread_manager;

    /// Consumer can be sendLoop or local receiver.
    class ConsumerState
    {
    public:
        ConsumerState()
            : future(promise.get_future())
        {
        }

        // before finished, must be called without protection of mu
        String getError()
        {
            future.wait();
            return future.get();
        }

        void setError(const String & err_msg)
        {
            promise.set_value(err_msg);
            err_has_set = true;
        }

        bool errHasSet() const
        {
            return err_has_set.load();
        }

    private:
        std::promise<String> promise;
        std::shared_future<String> future;
        std::atomic<bool> err_has_set{false};
    };
    ConsumerState consumer_state;

    ConnectionProfileInfo connection_profile_info;

    LoggerPtr log;
};

using StreamWriterPtr = std::shared_ptr<StreamWriter>;
} // namespace DB
