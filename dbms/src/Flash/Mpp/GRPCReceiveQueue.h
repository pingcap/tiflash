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

#include <atomic>
#include <functional>
#include <magic_enum.hpp>
#include <mutex>
#include <queue>
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
    explicit KickReceiveTag(void * tag_)
        : tag(tag_)
    {}

    bool FinalizeResult(void ** tag_, bool * /*status*/) override
    {
        *tag_ = tag;
        return true;
    }

private:
    void * tag;
};

using GRPCReceiveKickFunc = std::function<grpc_call_error(KickReceiveTag *, grpc_call *)>;
using AsyncRetryConnection = std::pair<KickReceiveTag *, grpc_call *>;

// This is a FIFO data struct.
//
// When MPMCQueue is full, put the information related with AsyncRequestHandler
// into this queue so that we can kick it into GRPCCompletionQueue when the
// MPMCQueue is not full and try to re-write the data.
class AsyncRequestHandlerWaitQueue
{
public:
    bool empty()
    {
        std::lock_guard lock(mu);
        return wait_retry_queue.empty();
    }

    void push(AsyncRetryConnection conn)
    {
        std::lock_guard lock(mu);
        wait_retry_queue.push(conn);
    }

    AsyncRetryConnection pop()
    {
        std::lock_guard lock(mu);
        if (wait_retry_queue.empty())
            return AsyncRetryConnection(nullptr, nullptr);

        AsyncRetryConnection ret_conn = wait_retry_queue.front();
        wait_retry_queue.pop();

        return ret_conn;
    }

    std::queue<AsyncRetryConnection> popAll()
    {
        std::lock_guard lock(mu);
        if (wait_retry_queue.empty())
            return std::queue<AsyncRetryConnection>{};

        std::queue<AsyncRetryConnection> ret_conn = std::move(wait_retry_queue);
        return ret_conn;
    }

private:
    std::mutex mu;
    std::queue<AsyncRetryConnection> wait_retry_queue;
};

using AsyncRequestHandlerWaitQueuePtr = std::shared_ptr<AsyncRequestHandlerWaitQueue>;

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
private:
    using MsgChannelPtr = std::shared_ptr<MPMCQueue<std::shared_ptr<T>>>;

public:
    GRPCReceiveQueue(MsgChannelPtr recv_queue_, AsyncRequestHandlerWaitQueuePtr conn_wait_queue_, const LoggerPtr & log_)
        : recv_queue(std::move(recv_queue_))
        , conn_wait_queue(std::move(conn_wait_queue_))
        , log(log_)
    {}

    // For gtest usage.
    GRPCReceiveQueue(MsgChannelPtr & recv_queue_, GRPCReceiveKickFunc func)
        : recv_queue(recv_queue_)
        , log(Logger::get())
        , kick_func(func)
    {}

    const String & getCancelReason() const
    {
        return recv_queue->getCancelReason();
    }

    // Cancel the send queue, and set the cancel reason
    bool cancelWith(const String & reason)
    {
        auto ret = recv_queue->cancelWith(reason);
        if (ret)
            handleRemainingTags();
        return ret;
    }

    bool cancel()
    {
        return cancelWith("");
    }

    bool finish()
    {
        auto ret = recv_queue->finish();
        if (ret)
            handleRemainingTags();
        return ret;
    }

    // Pop data from queue and kick the CompletionQueue to re-push the data
    // if there is data failing to be pushed before because the queue was full.
    template <typename U>
    MPMCQueueResult pop(U && data)
    {
        auto ret = recv_queue->pop(std::forward<U>(data));
        if (ret == MPMCQueueResult::OK)
            kickCompletionQueue();
        return ret;
    }

    // When the queue is full GRPCReceiveQueue will not save the tag.
    // This action should be taken by the caller.
    template <typename U>
    GRPCReceiveQueueRes push(U && data)
    {
        MPMCQueueResult res = recv_queue->tryPush(data);
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

    // Wake up its completion queue.
    void kickCompletionQueue()
    {
        AsyncRetryConnection retry_conn = conn_wait_queue->pop();
        if (retry_conn.second == nullptr)
            return;

        putTagIntoCompletionQueue(retry_conn);
    }

    void handleRemainingTags()
    {
        std::queue<AsyncRetryConnection> remaining_tags = conn_wait_queue->popAll();
        while (!remaining_tags.empty())
        {
            AsyncRetryConnection conn = remaining_tags.front();
            remaining_tags.pop();
            putTagIntoCompletionQueue(conn);
        }
    }

    void putTagIntoCompletionQueue(AsyncRetryConnection retry_conn)
    {
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        grpc_call_error error = grpc_call_start_batch(retry_conn.second, nullptr, 0, retry_conn.first, nullptr);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    MsgChannelPtr recv_queue;
    GRPCReceiveKickFunc kick_func;
    AsyncRequestHandlerWaitQueuePtr conn_wait_queue;
    const LoggerPtr log;
};

} // namespace DB
