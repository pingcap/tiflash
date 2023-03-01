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

#include <Common/ConcurrentIOQueue.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/grpcpp.h>
#include <common/logger_useful.h>

#include <functional>
#include <magic_enum.hpp>

namespace DB
{
namespace tests
{
class TestGRPCSendQueue;
} // namespace tests

/// In grpc cpp framework, the tag that is pushed into grpc completion
/// queue must be inherited from `CompletionQueueTag`.
class KickTag : public grpc::internal::CompletionQueueTag
{
public:
    explicit KickTag(std::function<void *()> a)
        : action(std::move(a))
    {}

    bool FinalizeResult(void ** tag_, bool * /*status*/) override
    {
        *tag_ = action();
        return true;
    }

private:
    /// `action` is called before the `tag` is popped from completion queue
    /// in `FinalizeResult`.
    std::function<void *()> action;
};

using GRPCKickFunc = std::function<grpc_call_error(KickTag *)>;

enum class GRPCSendQueueRes
{
    OK,
    FINISHED,
    EMPTY,
    CANCELLED,
};

/// A multi-producer-single-consumer queue dedicated to async grpc streaming send work.
///
/// In streaming rpc, a client/server may send messages continuous.
/// However, async grpc is only allowed to have one outstanding write on the
/// same side of the same stream without waiting for the completion queue.
/// Further more, the message usually is generated from another thread which
/// introduce a race between this thread and grpc threads.
/// The grpc cpp framework provides a tool named `Alarm` can be used to push a tag into
/// completion queue thus the write can be done in grpc threads. But `Alarm` must need
/// a timeout and it uses a timer to trigger the notification, which is wasteful if we want
/// to trigger it immediately. So we can say `kickCompletionQueue` function is a
/// immediately-triggered `Alarm`.
template <typename T>
class GRPCSendQueue
{
public:
    GRPCSendQueue(size_t queue_size, grpc_call * call, const LoggerPtr & l)
        : send_queue(queue_size)
        , log(l)
        , kick_tag([this]() { return kickTagAction(); })
    {
        RUNTIME_ASSERT(call != nullptr, log, "call is null");
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        kick_func = [call](void * t) {
            return grpc_call_start_batch(call, nullptr, 0, t, nullptr);
        };
    }

    // For gtest usage.
    GRPCSendQueue(size_t queue_size, GRPCKickFunc func)
        : send_queue(queue_size)
        , log(Logger::get())
        , kick_func(func)
        , kick_tag([this]() { return kickTagAction(); })
    {}

    ~GRPCSendQueue()
    {
        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));
    }

    /// Push the data from queue and kick the grpc completion queue.
    ///
    /// Return true if push succeed.
    /// Else return false.
    bool push(T && data)
    {
        auto ret = send_queue.push(std::move(data)) == MPMCQueueResult::OK;
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    bool nonBlockingPush(T && data)
    {
        auto ret = send_queue.nonBlockingPush(std::move(data)) == MPMCQueueResult::OK;
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    /// Cancel the send queue, and set the cancel reason
    bool cancelWith(const String & reason)
    {
        auto ret = send_queue.cancelWith(reason);
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    const String & getCancelReason() const
    {
        return send_queue.getCancelReason();
    }

    /// Pop the data from queue.
    ///
    /// Return OK if pop is done.
    /// Return FINISHED if the queue is finished and empty.
    /// Return EMPTY if there is no data in queue and `new_tag` is saved.
    /// When the next push/finish is called, the `new_tag` will be pushed
    /// into grpc completion queue.
    /// Note that any data in `new_tag` mustn't be touched if this function
    /// returns EMPTY because this `new_tag` may be popped out in another
    /// grpc thread immediately. By the way, if this completion queue is only
    /// tied to one grpc thread, this data race will not happen.
    GRPCSendQueueRes pop(T & data, void * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");

        auto res = send_queue.tryPop(data);
        if (res == MPMCQueueResult::EMPTY)
        {
            // Double check if this queue is empty.
            std::unique_lock lock(mu);
            RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));
            res = send_queue.tryPop(data);
            if (res == MPMCQueueResult::EMPTY)
            {
                // If empty, change status to WAITING.
                status = Status::WAITING;
                tag = new_tag;
            }
        }
        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCSendQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCSendQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCSendQueueRes::CANCELLED;
        case MPMCQueueResult::EMPTY:
            return GRPCSendQueueRes::EMPTY;
        default:
            RUNTIME_ASSERT(false, log, "Result {} is invalid", magic_enum::enum_name(res));
        }
    }

    /// Finish the queue and kick the grpc completion queue.
    ///
    /// For return value meanings, see `MPMCQueue::finish`.
    bool finish()
    {
        auto ret = send_queue.finish();
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    bool isFull() const
    {
        return send_queue.isFull();
    }

private:
    friend class tests::TestGRPCSendQueue;

    void * kickTagAction()
    {
        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::QUEUING, log, "status {} is not queuing", magic_enum::enum_name(status));
        status = Status::NONE;

        return std::exchange(tag, nullptr);
    }

    /// Wake up its completion queue.
    void kickCompletionQueue()
    {
        {
            std::unique_lock lock(mu);
            if (status != Status::WAITING)
            {
                return;
            }
            RUNTIME_ASSERT(tag != nullptr, log, "status is waiting but tag is nullptr");
            status = Status::QUEUING;
        }

        grpc_call_error error = kick_func(&kick_tag);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    ConcurrentIOQueue<T> send_queue;

    const LoggerPtr log;

    /// The mutex is used to synchronize the concurrent calls between push/finish and pop.
    /// It protects `status` and `tag`.
    /// The concurrency problem we want to prevent here is LOST NOTIFICATION which is
    /// similar to `condition_variable`.
    /// Note that this mutex is necessary. It's useless to just change the `tag` to atomic.
    ///
    /// Imagine this case:
    /// Thread 1: want to pop the data from queue but find no data there.
    /// Thread 2: push/finish the data in queue.
    /// Thread 2: do not kick the completion queue because tag is nullptr.
    /// Thread 1: set the tag.
    ///
    /// If there is no more data, this connection will get stuck forever.
    std::mutex mu;

    enum class Status
    {
        /// No tag.
        NONE,
        /// Waiting for kicking.
        WAITING,
        /// Queuing in the grpc completion queue.
        QUEUING,
    };

    Status status = Status::NONE;
    void * tag = nullptr;

    GRPCKickFunc kick_func;

    KickTag kick_tag;
};

} // namespace DB
