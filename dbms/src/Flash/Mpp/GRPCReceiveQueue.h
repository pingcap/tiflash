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
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/grpcpp.h>
#include <common/logger_useful.h>

#include <deque>
#include <functional>
#include <magic_enum.hpp>
#include <mutex>
#include <set>
#include <utility>

namespace DB
{
namespace tests
{
class TestGRPCReceiveQueue;
} // namespace tests

/// In grpc cpp framework, the tag that is pushed into grpc completion
/// queue must be inherited from `CompletionQueueTag`.
class KickReceiveTag : public grpc::internal::CompletionQueueTag
{
public:
    explicit KickReceiveTag(const LoggerPtr & log_)
        : log(log_)
    {}

    void pushTag(void * tag)
    {
        std::lock_guard lock(mu);
        tags.push_back(tag);
    }

    bool FinalizeResult(void ** tag, bool * /*status*/) override
    {
        std::lock_guard lock(mu);
        RUNTIME_ASSERT(!tags.empty(), log, "tags shouldn't be empty in KickReceiveTag");
        *tag = tags.front();
        tags.pop_front();
        return true;
    }

private:
    std::mutex mu;
    std::deque<void *> tags;
    const LoggerPtr log;
};

using GRPCKickFunc = std::function<grpc_call_error(KickReceiveTag *)>;

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
class GRPCReceiveQueue
{
public:
    GRPCReceiveQueue(size_t queue_size, grpc_call * call, const LoggerPtr & log_)
        : recv_queue(queue_size)
        , log(log_)
        , kick_recv_tag(log)
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
    GRPCReceiveQueue(size_t queue_size, GRPCKickFunc func)
        : recv_queue(queue_size)
        , log(Logger::get())
        , kick_func(func)
        , kick_recv_tag(log)
    {}

    const String & getCancelReason() const
    {
        return recv_queue.getCancelReason();
    }

    // Cancel the send queue, and set the cancel reason
    bool cancelWith(const String & reason)
    {
        auto ret = recv_queue.cancelWith(reason);
        if (ret)
            handleTheRemainingTags();
        return ret;
    }

    bool finish()
    {
        auto ret = recv_queue.finish();
        if (ret)
            handleTheRemainingTags();
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
        MPMCQueueResult res = recv_queue.tryPush(data);
        if (res == MPMCQueueResult::FULL)
        {
            // tryPush and push_back must be protected by lock at the same time.
            // Because if we don't contain the second tryPush in the lock, the
            // following bug will happen:
            //   step1. Thread1: tryPush fail at first.
            //   step2. Thread1: tryPush fail at second.
            //   step3. Thread2: pop successfully and find that it's needless to
            //                   kick CompletionQueue.
            //   step4. Thread1: lock and set tags. However, the pop of the Thread2
            //                   is the last pop operation, no more pop operation
            //                   will be executed. So the tag will not be kicked
            //                   into completion queue forever.
            std::lock_guard lock(mu);
            res = recv_queue.tryPush(data);
            if (res == MPMCQueueResult::FULL)
                tags.push_back(new_tag);
        }

        switch (res)
        {
        case MPMCQueueResult::OK:
            return GRPCReceiveQueueRes::OK;
        case MPMCQueueResult::FINISHED:
            return GRPCReceiveQueueRes::FINISHED;
        case MPMCQueueResult::CANCELLED:
            return GRPCReceiveQueueRes::CANCELLED;
        case MPMCQueueResult::FULL:
            return GRPCReceiveQueueRes::FULL;
        default:
            RUNTIME_ASSERT(false, log, "Result {} is invalid", magic_enum::enum_name(res));
        }
    }

private:
    friend class tests::TestGRPCReceiveQueue;

    /// Wake up its completion queue.
    void kickCompletionQueue()
    {
        {
            std::lock_guard lock(mu);
            if (tags.empty())
                return;

            transferTagToKickReceiveTagWithNoLock();
        }

        callKickFunc();
    }

    void handleTheRemainingTags()
    {
        std::lock_guard lock(mu);
        while (!tags.empty())
        {
            transferTagToKickReceiveTagWithNoLock();
            callKickFunc();
        }
    }

    void transferTagToKickReceiveTagWithNoLock()
    {
        kick_recv_tag.pushTag(tags.front());
        tags.pop_front();
    }

    void callKickFunc()
    {
        grpc_call_error error = kick_func(&kick_recv_tag);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    MPMCQueue<T> recv_queue;
    std::mutex mu;
    std::deque<void *> tags;
    const LoggerPtr log;
    GRPCKickFunc kick_func;
    KickReceiveTag kick_recv_tag;
};

} // namespace DB
