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

#include <Common/MPMCQueue.h>
#include <Operators/Operator.h>

#include <atomic>

namespace DB
{
/**
 * SharedQueueSourceOp <────┐                    ┌───── SharedQueueSinkOp
 * SharedQueueSourceOp <────┼─── SharedQueue <───┼───── SharedQueueSinkOp
 * SharedQueueSourceOp <────┘                    └───── SharedQueueSinkOp
*/
class SharedQueue;
using SharedQueuePtr = std::shared_ptr<SharedQueue>;
class SharedQueue
{
public:
    static SharedQueuePtr build(size_t producer, size_t consumer, Int64 max_buffered_bytes);

    SharedQueue(CapacityLimits queue_limits, size_t init_producer);

    MPMCQueueResult tryPush(Block && block);
    MPMCQueueResult tryPop(Block & block);

    void producerFinish();

private:
    MPMCQueue<Block> queue;
    std::atomic_int32_t active_producer = -1;
};

class SharedQueueSinkOp : public SinkOp
{
public:
    explicit SharedQueueSinkOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        const SharedQueuePtr & shared_queue_)
        : SinkOp(exec_status_, req_id)
        , shared_queue(shared_queue_)
    {
    }

    ~SharedQueueSinkOp() override
    {
        shared_queue->producerFinish();
    }

    String getName() const override
    {
        return "SharedQueueSinkOp";
    }

    OperatorStatus prepareImpl() override;

    OperatorStatus writeImpl(Block && block) override;

    OperatorStatus awaitImpl() override;

private:
    std::optional<Block> res;
    SharedQueuePtr shared_queue;
};

class SharedQueueSourceOp : public SourceOp
{
public:
    SharedQueueSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        const Block & header_,
        const SharedQueuePtr & shared_queue_)
        : SourceOp(exec_status_, req_id)
        , shared_queue(shared_queue_)
    {
        setHeader(header_);
    }

    String getName() const override
    {
        return "SharedQueueSourceOp";
    }

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

private:
    std::optional<Block> res;
    SharedQueuePtr shared_queue;
};
} // namespace DB
