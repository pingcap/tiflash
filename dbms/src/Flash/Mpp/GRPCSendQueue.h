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
#include <common/logger_useful.h>
#include <grpcpp/grpcpp.h>

#include <functional>

namespace DB
{

/// A multi-producer-single-consumer queue dedicated to async grpc send work.
template <typename T>
class GRPCSendQueue
{
public:
    using KickFunc = std::function<grpc_call_error(grpc_call *, void *)>;

    GRPCSendQueue(size_t queue_size, grpc_call * call_, const LoggerPtr log_)
        : send_queue(MPMCQueue<T>(queue_size))
        , call(call_)
        , tag(nullptr)
        , log(log_)
    {
        RUNTIME_ASSERT(call != nullptr, log, "call is null");
        // If a call to `grpc_call_start_batch` with an empty batch returns
        // `GRPC_CALL_OK`, the tag is put in the completion queue immediately.
        // This behavior is well-defined. See https://github.com/grpc/grpc/issues/16357.
        kick_func = [](grpc_call * c, void * t) {
            return grpc_call_start_batch(c, nullptr, 0, t, nullptr);
        };
    }

    // For test purpose.
    GRPCSendQueue(size_t queue_size, grpc_call * call_, KickFunc func, const LoggerPtr log_)
        : send_queue(MPMCQueue<T>(queue_size))
        , call(call_)
        , tag(nullptr)
        , kick_func(func)
        , log(log_)
    {}

    ~GRPCSendQueue()
    {
        if (tag != nullptr)
        {
            LOG_ERROR(log, "tag is not null in deconstruction, tag's memory may leak");
#ifndef NDEBUG
            assert(false);
#endif
        }
    }

    /// Push the data from queue and kick the completion queue.
    /// For return value meanings, see `MPMCQueue::finish`.
    template <typename U>
    bool push(U && u)
    {
        auto ret = send_queue.push(u);
        if (ret)
        {
            kickCompletionQueue();
        }
        return ret;
    }

    /// Pop the data from queue.
    /// Return true if pop is done and `ok` means the if there is
    /// new data from queue(see `MPMCQueue::pop`).
    /// Return false if pop can't be done due to blocking and `new_tag`
    /// is saved. When the next push/finish is called, the `new_tag` will
    /// be pushed into grpc completion queue.
    bool pop(T & data, bool & ok, void * new_tag)
    {
        RUNTIME_ASSERT(new_tag != nullptr, log, "new_tag is nullptr when popping");
        if (!send_queue.isNextPopNonBlocking())
        {
            // Next pop is blocking.
            std::unique_lock lock(mu);
            RUNTIME_ASSERT(tag == nullptr, log, "tag is not nullptr when popping");
            // Double check if next pop is blocking.
            if (!send_queue.isNextPopNonBlocking())
            {
                // If blocking, set the tag and return false.
                tag = new_tag;
                return false;
            }
            // If not blocking, pop will be called.
        }
        ok = send_queue.pop(data);
        return true;
    }

    /// Finish the queue and kick the completion queue.
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
    /// In grpc cpp framework, the tag that is pushed into grpc completion
    /// queue must be inherited from `CompletionQueueTag`.
    class KickTag : public ::grpc::internal::CompletionQueueTag
    {
    public:
        explicit KickTag(void * tag_)
            : tag(tag_)
        {}

        bool FinalizeResult(void ** tag_, bool * /*status*/) override
        {
            *tag_ = tag;
            // After calling this function, this `KickTag` will not be pointed
            // to by any pointer. So it can be directly deleted here.
            delete this;
            return true;
        }

    private:
        void * tag;
    };

    /// Wake up its completion queue.
    void kickCompletionQueue()
    {
        void * old_tag;
        {
            std::unique_lock lock(mu);
            old_tag = tag;
            tag = nullptr;
        }
        if (!old_tag)
        {
            return;
        }
        grpc_call_error error = kick_func(call, new KickTag(old_tag));
        // If an error occur, there must be something wrong about shutdown process.
        RUNTIME_ASSERT(error == grpc_call_error::GRPC_CALL_OK, log, "grpc_call_start_batch returns {} != GRPC_CALL_OK, memory of tag may leak", error);
    }

    MPMCQueue<T> send_queue;

    /// The mutex is used to synchronize the concurrent calls between push/finish and pop.
    /// The concurrency problem we want to prevent here is LOST NOTIFICATION.
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

    grpc_call * call;
    void * tag;

    KickFunc kick_func;
    const LoggerPtr log;
};

} // namespace DB