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
#include <utility>

namespace DB
{

enum class GRPCReceiveQueueRes
{
    OK,
    FINISHED,
    FULL,
    CANCELLED,
};

template <typename T>
class GRPCReceiveQueue : public GRPCQueue
{
public:
    GRPCReceiveQueue(size_t queue_size, grpc_call * call, const LoggerPtr & log)
        : GRPCQueue(call, log)
        , send_queue(queue_size)
    {}

    // For gtest usage.
    GRPCReceiveQueue(size_t queue_size, GRPCKickFunc func)
        : GRPCQueue(func)
        , send_queue(queue_size)
    {}

    ~GRPCReceiveQueue()
    {
        std::unique_lock lock(mu);

        RUNTIME_ASSERT(status == Status::NONE, log, "status {} is not none", magic_enum::enum_name(status));
    }

    // Cancel the send queue, and set the cancel reason
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

    bool finish()
    {
        auto ret = send_queue.finish();
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    template <typename U>
    bool pop(U && data)
    {
        auto ret = send_queue.pop(std::forward<U>(data)) == MPMCQueueResult::OK;
        if (ret)
            kickCompletionQueue();
        return ret;
    }

    GRPCReceiveQueueRes push(T & data, void * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");

        auto res = send_queue.tryPush(data);
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
        res = send_queue.tryPush(data);
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
    MPMCQueue<T> send_queue;
};

} // namespace DB
