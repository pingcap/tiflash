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
#include <Common/grpcpp.h>
#include <common/logger_useful.h>
#include <Flash/Mpp/GRPCQueue.h>

#include <functional>
#include <magic_enum.hpp>

namespace DB
{
namespace tests
{
class TestGRPCSendQueue;
} // namespace tests

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
class GRPCSendQueue : public GRPCQueue
{
public:
    GRPCSendQueue(size_t queue_size, grpc_call * call, const LoggerPtr & log)
        : GRPCQueue(call, log)
        , send_queue(queue_size)
    {}

    // For gtest usage.
    GRPCSendQueue(size_t queue_size, GRPCKickFunc func)
        : GRPCQueue(func)
        , send_queue(queue_size)
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
    template <typename U>
    bool push(U && u)
    {
        auto ret = send_queue.push(std::forward<U>(u)) == MPMCQueueResult::OK;
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
            kickCompletionQueue();
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
        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCSendQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCSendQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCSendQueueRes::CANCELLED;
        case MPMCQueueResult::EMPTY:
            // Handle this case later.
            break;
        default:
            RUNTIME_ASSERT(false, log, "Result {} is invalid", static_cast<Int32>(res));
        }

        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));

        // Double check if this queue is empty.
        res = send_queue.tryPop(data);
        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCSendQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCSendQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCSendQueueRes::CANCELLED;
        case MPMCQueueResult::EMPTY:
        {
            // If empty, change status to WAITING.
            status = Status::WAITING;
            tag = new_tag;
            return GRPCSendQueueRes::EMPTY;
        }
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

private:
    friend class tests::TestGRPCSendQueue;

    MPMCQueue<T> send_queue;
};

} // namespace DB
