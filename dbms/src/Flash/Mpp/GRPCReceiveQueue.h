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
#include <Flash/Mpp/GRPCQueue.h>
#include <common/logger_useful.h>

#include <functional>
#include <magic_enum.hpp>
#include <utility>

namespace DB
{
namespace tests
{
class TestGRPCReceiveQueue;
} // namespace tests

enum class GRPCReceiveQueueRes
{
    OK,
    FINISHED,
    FULL,
    CANCELLED,
};

// When MPMCQueue is full, thread that pushes data into MPMCQueue will be blocked which
// is unexpected when that's a grpc thread. Because tiflash's grpc threads responsible for
// receiving data are limited and the available grpc threads will be less and less when more
// and more push operation are blocked. In order to avoid the above case, we create this
// non-blocking queue so that push operation will not be blocked when the queue is full.
template <typename T>
class GRPCReceiveQueue : public GRPCQueue
{
public:
    GRPCReceiveQueue(size_t queue_size, grpc_call * call, const LoggerPtr & log)
        : GRPCQueue(call, log)
        , recv_queue(queue_size)
    {}

    // For gtest usage.
    GRPCReceiveQueue(size_t queue_size, GRPCKickFunc func)
        : GRPCQueue(func)
        , recv_queue(queue_size)
    {}

    ~GRPCReceiveQueue()
    {
        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));
    }

    // Cancel the send queue, and set the cancel reason
    bool cancelWith(const String & reason)
    {
        auto ret = recv_queue.cancelWith(reason);
        if (ret)
            kickCompletionQueue();
        return ret;
    }

    const String & getCancelReason() const
    {
        return recv_queue.getCancelReason();
    }

    bool finish()
    {
        auto ret = recv_queue.finish();
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    // Pop data from queue and kick the CompletionQueue to re-push the data
    // if there is data failing to be pushed before because the queue was full.
    template <typename U>
    bool pop(U && data)
    {
        auto ret = recv_queue.pop(std::forward<U>(data)) == MPMCQueueResult::OK;
        if (ret)
            kickCompletionQueue();
        return ret;
    }

    // Push data into the queue.
    //
    // Return FULL if the queue is full and `new_tag` is saved.
    // When the next pop/finish is called, the `new_tag` will be pushed
    // into grpc completion queue.
    template <typename U>
    GRPCReceiveQueueRes push(U && data, void * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");

        auto res = recv_queue.tryPush(data);
        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCReceiveQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCReceiveQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCReceiveQueueRes::CANCELLED;
        case MPMCQueueResult::FULL:
            // Handle this case later.
            break;
        default:
            RUNTIME_ASSERT(false, log, "Result {} is invalid", static_cast<Int32>(res));
        }

        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));

        // Double check if this queue is full.
        res = recv_queue.tryPush(data);
        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCReceiveQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCReceiveQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCReceiveQueueRes::CANCELLED;
        case MPMCQueueResult::FULL:
        {
            // If empty, change status to WAITING.
            status = Status::WAITING;
            tag = new_tag;
            return GRPCReceiveQueueRes::FULL;
        }
        default:
            RUNTIME_ASSERT(false, log, "Result {} is invalid", magic_enum::enum_name(res));
        }
    }

private:
    friend class tests::TestGRPCReceiveQueue;

    MPMCQueue<T> recv_queue;
};

} // namespace DB
