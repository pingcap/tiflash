// Copyright 2023 PingCAP, Inc.
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
#include <Common/GRPCKickTag.h>
#include <Common/Logger.h>
#include <Common/LooseBoundedMPMCQueue.h>
#include <Common/grpcpp.h>

#include <functional>
#include <magic_enum.hpp>
#include <queue>

namespace DB
{
namespace tests
{
class TestGRPCSendQueue;
class TestGRPCRecvQueue;
} // namespace tests

/// A multi-producer-single-consumer queue dedicated to async grpc streaming send work.
///
/// In streaming rpc, a client/server may send(write) messages continuous.
/// However, async grpc is only allowed to have one outstanding write on the same side
/// of the same stream. Furthermore, when queue is empty, the process of writing messages
/// is stopped in grpc thread and the next message generated from compute thread should
/// be writen by itself in the previous implementation.
/// The synchronization between grpc thread and compute thread is pretty error-prone.
///
/// GRPCSendQueue solves this issue and makes all write happen in grpc thread by
/// 1. for pop function calling in grpc thread, save the tag when queue is empty.
/// 2. for push function calling in compute thread, push the tag into completion queue if any.
template <typename T>
class GRPCSendQueue
{
public:
    template <typename... Args>
    explicit GRPCSendQueue(const LoggerPtr & l, Args &&... args)
        : log(l)
        , send_queue(std::forward<Args>(args)...)
    {}

    ~GRPCSendQueue() { RUNTIME_ASSERT(tag == nullptr, log, "tag is not nullptr"); }

    /// For test usage only.
    void setKickFuncForTest(GRPCKickFunc && func) { test_kick_func = std::move(func); }

    /// Blocking push the data from the local node and kick the grpc completion queue.
    MPMCQueueResult push(T && data)
    {
        auto ret = send_queue.push(std::move(data));
        if (ret == MPMCQueueResult::OK)
            kickOneTag(true);

        return ret;
    }

    /// Non-blocking force to push data into queue.
    MPMCQueueResult forcePush(T && data)
    {
        auto ret = send_queue.forcePush(std::move(data));
        if (ret == MPMCQueueResult::OK)
            kickOneTag(true);

        return ret;
    }

    /// Non-blocking pop the data from queue in grpc thread.
    ///
    /// Return OK if pop is done.
    /// Return FINISHED if the queue is finished and empty.
    /// Return CANCELED if the queue is canceled.
    /// Return EMPTY if there is no data in queue and `new_tag` is saved.
    /// When the next push/finish is called, the `new_tag` will be pushed
    /// into grpc completion queue.
    /// Note that any data in `new_tag` mustn't be touched if this function
    /// returns EMPTY because this `new_tag` may be popped out in another
    /// grpc thread immediately. By the way, if this completion queue is only
    /// tied to one grpc thread, this data race will not happen.
    MPMCQueueResult popWithTag(T & data, GRPCKickTag * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");
        RUNTIME_ASSERT(new_tag->getCall() != nullptr || test_kick_func, log, "call is null");

        auto res = send_queue.tryPop(data);
        if (res == MPMCQueueResult::EMPTY)
        {
            // Add lock and double check if this queue is empty.
            std::lock_guard<std::mutex> lock(mu);
            res = send_queue.tryPop(data);
            if (res == MPMCQueueResult::EMPTY)
            {
                RUNTIME_ASSERT(tag == nullptr, log, "tag is not nullptr");
                tag = new_tag;
            }
        }
        return res;
    }

    MPMCQueueStatus getStatus() const { return send_queue.getStatus(); }

    /// Cancel the send queue, and set the cancel reason.
    bool cancelWith(const String & reason)
    {
        auto ret = send_queue.cancelWith(reason);
        if (ret)
            kickOneTag(false);

        return ret;
    }

    const String & getCancelReason() const { return send_queue.getCancelReason(); }

    /// Finish the queue and kick the grpc completion queue.
    ///
    /// For return value meanings, see `MPMCQueue::finish`.
    bool finish()
    {
        auto ret = send_queue.finish();
        if (ret)
            kickOneTag(false);

        return ret;
    }

    bool isWritable() const { return send_queue.isWritable(); }
    void triggerPipelineNotify() { send_queue.triggerPipelineNotify(); }

    void registerPipeReadTask(TaskPtr && task) { send_queue.registerPipeReadTask(std::move(task)); }
    void registerPipeWriteTask(TaskPtr && task) { send_queue.registerPipeWriteTask(std::move(task)); }

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
        t->kick(test_kick_func);
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
    /// Thread 2: do not kick the completion queue because the tag is nullptr.
    /// Thread 1: set the tag.
    ///
    /// If there is no more data, this connection will get stuck forever.
    std::mutex mu;

    GRPCKickFunc test_kick_func;
    GRPCKickTag * tag = nullptr;
};

/// A multi-producer-multi-consumer queue dedicated to async grpc streaming receive work.
///
/// In streaming rpc, a client/server may receive(read) messages continuous.
/// However, async grpc is only allowed to have one outstanding read on the same side
/// of the same stream. Furthermore, when queue is full, the process of reading messages
/// is stopped in grpc thread and compute thread should read new message by itself after
/// handling messages in the previous implementation.
/// The synchronization between grpc thread and compute thread is pretty error-prone.
///
/// GRPCRecvQueue solves this issue and makes all read happen in grpc thread by
/// 1. for push function calling in grpc thread, save the tag(s) when queue is full.
/// 2. for pop function calling in compute thread, push one tag into completion queue if any.
template <typename T>
class GRPCRecvQueue
{
public:
    template <typename... Args>
    explicit GRPCRecvQueue(const LoggerPtr & log_, Args &&... args)
        : log(log_)
        , recv_queue(std::forward<Args>(args)...)
    {}

    ~GRPCRecvQueue() { RUNTIME_ASSERT(data_tags.empty(), log, "data_tags is not empty"); }

    /// For test usage only.
    void setKickFuncForTest(GRPCKickFunc && func) { test_kick_func = std::move(func); }

    /// Blocking pop the data from the queue.
    MPMCQueueResult pop(T & data)
    {
        auto ret = recv_queue.pop(data);
        if (ret == MPMCQueueResult::OK)
            kickOneTagWithSuccess();
        return ret;
    }

    /// Non-blocking pop the data from the queue.
    MPMCQueueResult tryPop(T & data)
    {
        auto ret = recv_queue.tryPop(data);
        if (ret == MPMCQueueResult::OK)
            kickOneTagWithSuccess();
        return ret;
    }

    /// Non-blocking dequeue the queue.
    MPMCQueueResult tryDequeue()
    {
        auto ret = recv_queue.tryDequeue();
        if (ret == MPMCQueueResult::OK)
            kickOneTagWithSuccess();
        return ret;
    }

    /// Non-blocking push the data from the remote node in grpc thread.
    ///
    /// Return OK if push is done.
    /// Return FINISHED if the queue is finished and empty.
    /// Return CANCELED if the queue is canceled.
    /// Return FULL if the queue is full and `new_tag` is saved.
    /// When the next pop is called, the `new_tag` will be pushed
    /// into grpc completion queue.
    /// Note that any data in `new_tag` mustn't be touched if this function
    /// returns FULL because this `new_tag` may be popped out in another
    /// grpc thread immediately. By the way, if this completion queue is only
    /// tied to one grpc thread, this data race will not happen.
    MPMCQueueResult pushWithTag(T && data, GRPCKickTag * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr");
        RUNTIME_ASSERT(new_tag->getCall() != nullptr || test_kick_func, log, "call is null");

        auto res = recv_queue.tryPush(std::move(data));
        if (res == MPMCQueueResult::FULL)
        {
            // Add lock and double check if this queue is full.
            std::lock_guard<std::mutex> lock(mu);
            res = recv_queue.tryPush(data);
            if (res == MPMCQueueResult::FULL)
                data_tags.push_back(std::make_pair(std::move(data), new_tag));
        }
        return res;
    }

    /// Blocking push the data from the local node.
    MPMCQueueResult push(T && data) { return recv_queue.push(std::move(data)); }

    /// Non-blocking force to push the data from the local node.
    MPMCQueueResult forcePush(T && data) { return recv_queue.forcePush(std::move(data)); }

    MPMCQueueStatus getStatus() const { return recv_queue.getStatus(); }

    bool cancel() { return cancelWith(""); }

    /// Cancel the recv queue and set the cancel reason.
    /// Then kick all tags with false status.
    bool cancelWith(const String & reason)
    {
        auto ret = recv_queue.cancelWith(reason);
        if (ret)
            kickAllTagsWithFailure();

        return ret;
    }

    const String & getCancelReason() const { return recv_queue.getCancelReason(); }

    /// Finish the recv queue.
    /// Then kick all tags with false status.
    bool finish()
    {
        auto ret = recv_queue.finish();
        if (ret)
            kickAllTagsWithFailure();
        return ret;
    }

    bool isWritable() const { return recv_queue.isWritable(); }
    void triggerPipelineNotify() { return recv_queue.triggerPipelineNotify(); }

    void registerPipeReadTask(TaskPtr && task) { recv_queue.registerPipeReadTask(std::move(task)); }
    void registerPipeWriteTask(TaskPtr && task) { recv_queue.registerPipeWriteTask(std::move(task)); }

private:
    friend class tests::TestGRPCRecvQueue;

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
            data_tags.pop_front();
        }

        t->setStatus(true);
        t->kick(test_kick_func);
    }

    void kickAllTagsWithFailure()
    {
        std::lock_guard lock(mu);
        while (!data_tags.empty())
        {
            GRPCKickTag * t = data_tags.front().second;
            data_tags.pop_front();
            t->setStatus(false);
            t->kick(test_kick_func);
        }
    }

    const LoggerPtr log;

    LooseBoundedMPMCQueue<T> recv_queue;

    /// The mutex is used to protect data_tags and avoid LOST NOTIFICATION like the one in GRPCSendQueue.
    ///
    /// Imagine this case:
    /// Thread 1: want to push the data but find the queue is full.
    /// Thread 2: pop all of the data from queue.
    /// Thread 2: do not kick the completion queue because data_tags is empty.
    /// Thread 2: wait for popping the new data from queue.
    /// Thread 1: push the data and tag into data_tags.
    ///
    /// Thread 1 and Thread 2 get stuck forever.
    std::mutex mu;

    GRPCKickFunc test_kick_func;
    std::deque<std::pair<T, GRPCKickTag *>> data_tags;
};

} // namespace DB
