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
#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/grpcpp.h>
#include <common/logger_useful.h>

#include <functional>
#include <magic_enum.hpp>
#include <queue>

namespace DB
{
namespace tests
{
class TestGRPCSendQueue;
} // namespace tests

/// In grpc cpp framework, the tag that is pushed into grpc completion
/// queue must be inherited from `CompletionQueueTag`.
class GRPCKickTag : public grpc::internal::CompletionQueueTag
{
public:
    explicit GRPCKickTag(void * tag_)
        : call(nullptr)
        , tag(tag_)
        , status(true)
    {}

    bool FinalizeResult(void ** tag_, bool * status_) override
    {
        *tag_ = tag;
        *status_ = status;
        return true;
    }

    void setCall(grpc_call * call_)
    {
        call = call_;
    }

    grpc_call * getCall()
    {
        return call;
    }

    void setStatus(bool status_)
    {
        status = status_;
    }

private:
    grpc_call * call;
    void * tag;
    bool status;
};

using GRPCKickFunc = std::function<grpc_call_error(GRPCKickTag *)>;

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
/// to trigger it immediately. So we can say `kickOneTag` function is a
/// immediately-triggered `Alarm`.
template <typename T>
class GRPCSendQueue
{
public:
    template <typename... Args>
    GRPCSendQueue(const LoggerPtr & l, Args &&... args)
        : log(l)
        , send_queue(std::forward<Args>(args)...)
    {
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        kick_func = [](GRPCKickTag * t) {
            return grpc_call_start_batch(t->getCall(), nullptr, 0, t, nullptr);
        };
    }

    // For gtest usage.
    template <typename... Args>
    GRPCSendQueue(GRPCKickFunc && func, Args &&... args)
        : log(Logger::get())
        , send_queue(std::forward<Args>(args)...)
        , kick_func(std::move(func))
    {}

    ~GRPCSendQueue()
    {
        RUNTIME_ASSERT(tag == nullptr, log, "tag is not nullptr");
    }

    /// Push the data from the local node and kick the grpc completion queue.
    ///
    /// Return true if push succeed.
    /// Else return false.
    bool push(T && data)
    {
        auto ret = send_queue.push(std::move(data)) == MPMCQueueResult::OK;
        if (ret)
            kickOneTag(true);

        return ret;
    }

    bool forcePush(T && data)
    {
        auto ret = send_queue.forcePush(std::move(data)) == MPMCQueueResult::OK;
        if (ret)
            kickOneTag(true);

        return ret;
    }

    /// Pop the data from queue in grpc thread.
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
    MPMCQueueResult pop(T & data, GRPCKickTag * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");
        RUNTIME_ASSERT(new_tag->getCall() != nullptr, log, "call is null");

        auto res = send_queue.tryPop(data);
        if (res == MPMCQueueResult::EMPTY)
        {
            // Add lock and double check if this queue is empty.
            std::unique_lock lock(mu);
            res = send_queue.tryPop(data);
            if (res == MPMCQueueResult::EMPTY)
            {
                RUNTIME_ASSERT(tag == nullptr, log, "tag is not nullptr");
                tag = new_tag;
            }
        }
        return res;
    }

    /// Cancel the send queue, and set the cancel reason.
    bool cancelWith(const String & reason)
    {
        auto ret = send_queue.cancelWith(reason);
        if (ret)
            kickOneTag(false);

        return ret;
    }

    const String & getCancelReason() const
    {
        return send_queue.getCancelReason();
    }

    /// Finish the queue and kick the grpc completion queue.
    ///
    /// For return value meanings, see `MPMCQueue::finish`.
    bool finish()
    {
        auto ret = send_queue.finish();
        if (ret)
            kickOneTag(true);

        return ret;
    }

    bool isFull() const
    {
        return send_queue.isFull();
    }

private:
    friend class tests::TestGRPCSendQueue;

    void kickOneTag(bool status)
    {
        GRPCKickTag * t;
        {
            std::lock_guard lock(mu);
            if (tag == nullptr)
                return;
            t = tag;
            tag = nullptr;
        }

        t->setStatus(status);
        grpc_call_error error = kick_func(t);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    const LoggerPtr log;

    LooseBoundedMPMCQueue<T> send_queue;

    /// The mutex is used to synchronize the concurrent calls between push/finish and pop.
    /// The concurrency problem we want to prevent here is LOST NOTIFICATION which is very
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

    GRPCKickFunc kick_func;
    GRPCKickTag * tag = nullptr;
};

template <typename T>
class GRPCRecvQueue
{
public:
    template <typename... Args>
    GRPCRecvQueue(const LoggerPtr & log_, Args &&... args)
        : log(log_)
        , recv_queue(std::forward<Args>(args)...)
    {
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is pushed into the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        kick_func = [](GRPCKickTag * t) {
            return grpc_call_start_batch(t->getCall(), nullptr, 0, t, nullptr);
        };
    }

    // For gtest usage.
    template <typename... Args>
    GRPCRecvQueue(GRPCKickFunc func, Args &&... args)
        : log(Logger::get())
        , recv_queue(std::forward<Args>(args)...)
        , kick_func(std::move(func))
    {}

    ~GRPCRecvQueue()
    {
        RUNTIME_ASSERT(data_tags.empty(), log, "data_tags is not empty");
    }

    MPMCQueueResult pop(T & data)
    {
        auto ret = recv_queue.pop(data);
        if (ret == MPMCQueueResult::OK)
            kickOneTagWithSuccess();
        return ret;
    }

    MPMCQueueResult tryPop(T & data)
    {
        auto ret = recv_queue.tryPop(data);
        if (ret == MPMCQueueResult::OK)
            kickOneTagWithSuccess();
        return ret;
    }

    /// Push the data from the remote node in grpc thread.
    MPMCQueueResult push(T && data, GRPCKickTag * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");
        RUNTIME_ASSERT(new_tag->getCall() != nullptr, log, "call is null");

        auto res = recv_queue.tryPush(std::move(data));
        if (res == MPMCQueueResult::FULL)
        {
            // Add lock and double check if this queue is full.
            std::unique_lock lock(mu);
            res = recv_queue.tryPush(data);
            if (res == MPMCQueueResult::FULL)
                data_tags.push(std::make_pair(std::move(data), new_tag));
        }
        return res;
    }

    /// Push the data from the local node.
    MPMCQueueResult push(T && data)
    {
        return recv_queue.push(std::move(data));
    }

    /// Force push the data from the local node.
    MPMCQueueResult forcePush(T && data)
    {
        return recv_queue.forcePush(std::move(data));
    }

    bool cancel()
    {
        return cancelWith("");
    }

    /// Cancel the recv queue, and set the cancel reason.
    /// Then kick all tags with false status.
    bool cancelWith(const String & reason)
    {
        std::unique_lock lock(mu);
        auto ret = recv_queue.cancelWith(reason);
        if (ret)
        {
            while (!data_tags.empty())
            {
                GRPCKickTag * t = data_tags.front().second;
                data_tags.pop();
                t->setStatus(false);
                kick(t);
            }
        }

        return ret;
    }

    const String & getCancelReason() const
    {
        return recv_queue.getCancelReason();
    }

    /// Finish the queue.
    ///
    /// For return value meanings, see `MPMCQueue::finish`.
    bool finish()
    {
        RUNTIME_ASSERT(data_tags.empty(), log, "data_tags is not empty, size {}", data_tags.size());

        return recv_queue.finish();
    }

    bool isFull() const
    {
        return recv_queue.isFull();
    }

private:
    void kickOneTagWithSuccess()
    {
        GRPCKickTag * t;
        {
            std::lock_guard lock(mu);
            if (data_tags.empty())
                return;
            auto res = recv_queue.tryPush(std::move(data_tags.front().first));
            if (res != MPMCQueueResult::OK)
                return;
            t = data_tags.front().second;
            data_tags.pop();
        }

        t->setStatus(true);
        kick(t);
    }

    void kick(GRPCKickTag * t)
    {
        grpc_call_error error = kick_func(t);
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    const LoggerPtr log;

    LooseBoundedMPMCQueue<T> recv_queue;

    std::mutex mu;

    GRPCKickFunc kick_func;
    std::queue<std::pair<T, GRPCKickTag *>> data_tags;
};

} // namespace DB
