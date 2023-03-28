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

#include <Common/MPMCQueue.h>
#include <Core/Block.h>

#include <mutex>

namespace DB
{
class ResultQueue
{
public:
    ResultQueue(size_t queue_size, bool is_test_)
        : queue(queue_size)
        , is_test(is_test_)
    {}

    MPMCQueueResult tryPush(Block && block)
    {
        return queue.tryPush(std::move(block));
    }

    MPMCQueueResult pop(Block & block)
    {
        // In test mode, a single query should take no more than 15 seconds to execute.
        static std::chrono::seconds timeout(15);
        if unlikely (is_test)
            return queue.popTimeout(block, timeout);
        else
            return queue.pop(block);
    }

    void finish()
    {
        queue.finish();
    }

    void cancel()
    {
        queue.cancel();
    }

private:
    MPMCQueue<Block> queue;
    bool is_test;
};
using ResultQueuePtr = std::shared_ptr<ResultQueue>;

class ResultQueueHolder
{
public:
    void set(const ResultQueuePtr & queue_)
    {
        assert(queue_);
        std::lock_guard lock(mu);
        assert(!queue);
        queue = queue_;
    }

    std::optional<ResultQueue *> tryGet()
    {
        std::optional<ResultQueue *> res;
        std::lock_guard lock(mu);
        if (queue != nullptr)
            res.emplace(queue.get());
        return res;
    }

    ResultQueue * operator->()
    {
        std::lock_guard lock(mu);
        assert(queue != nullptr);
        return queue.get();
    }

private:
    std::mutex mu;
    ResultQueuePtr queue;
};
} // namespace DB
