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

#include <Common/LooseBoundedMPMCQueue.h>
#include <Core/Block.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>

#include <memory>

namespace DB
{
class ResultQueue : public NotifyFuture
{
public:
    explicit ResultQueue(size_t queue_size)
        : queue(queue_size)
    {}

    // read
    MPMCQueueResult pop(Block & block) { return queue.pop(block); }

    // write
    MPMCQueueResult push(Block && block) { return queue.push(block); }
    MPMCQueueResult tryPush(Block && block) { return queue.tryPush(block); }
    void registerTask(TaskPtr && task) override { queue.registerPipeWriteTask(std::move(task)); }

    // finish/cancel
    bool finish() { return queue.finish(); }
    bool cancel() { return queue.cancel(); }

private:
    LooseBoundedMPMCQueue<Block> queue;
};
using ResultQueuePtr = std::shared_ptr<ResultQueue>;
} // namespace DB
