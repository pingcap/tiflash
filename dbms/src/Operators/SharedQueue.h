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
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
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

class SharedQueueSinkHolder;
using SharedQueueSinkHolderPtr = std::shared_ptr<SharedQueueSinkHolder>;

class SharedQueueSourceHolder;
using SharedQueueSourceHolderPtr = std::shared_ptr<SharedQueueSourceHolder>;

class SharedQueue
{
public:
    static SharedQueuePtr buildInternal(
        size_t producer,
        size_t consumer,
        Int64 max_buffered_bytes = -1,
        Int64 max_queue_size = -1);

    static std::pair<SharedQueueSinkHolderPtr, SharedQueueSourceHolderPtr> build(
        PipelineExecutorContext & exec_context,
        size_t producer,
        size_t consumer,
        Int64 max_buffered_bytes = -1,
        Int64 max_queue_size = -1);

    SharedQueue(CapacityLimits queue_limits, size_t init_producer);

    MPMCQueueResult tryPush(Block && block);
    MPMCQueueResult tryPop(Block & block);

    void producerFinish();

    void cancel();

    void registerReadTask(TaskPtr && task) { queue.registerPipeReadTask(std::move(task)); }
    void registerWriteTask(TaskPtr && task) { queue.registerPipeWriteTask(std::move(task)); }

private:
    LooseBoundedMPMCQueue<Block> queue;
    std::atomic_int32_t active_producer = -1;
};

class SharedQueueSinkHolder : public NotifyFuture
{
public:
    explicit SharedQueueSinkHolder(const SharedQueuePtr & queue_)
        : queue(queue_)
    {}
    MPMCQueueResult tryPush(Block && block) { return queue->tryPush(std::move(block)); }
    void finish() { queue->producerFinish(); }

    void registerTask(TaskPtr && task) override { queue->registerWriteTask(std::move(task)); }

private:
    SharedQueuePtr queue;
};

class SharedQueueSinkOp : public SinkOp
{
public:
    explicit SharedQueueSinkOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const SharedQueueSinkHolderPtr & shared_queue_)
        : SinkOp(exec_context_, req_id)
        , shared_queue(shared_queue_)
    {}

    ~SharedQueueSinkOp() override { shared_queue->finish(); }

    String getName() const override { return "SharedQueueSinkOp"; }

    OperatorStatus prepareImpl() override;

    OperatorStatus writeImpl(Block && block) override;

private:
    OperatorStatus tryFlush();

private:
    std::optional<Block> buffer;
    SharedQueueSinkHolderPtr shared_queue;
};

class SharedQueueSourceHolder : public NotifyFuture
{
public:
    explicit SharedQueueSourceHolder(const SharedQueuePtr & queue_)
        : queue(queue_)
    {}
    MPMCQueueResult tryPop(Block & block) { return queue->tryPop(block); }

    void registerTask(TaskPtr && task) override { queue->registerReadTask(std::move(task)); }

private:
    SharedQueuePtr queue;
};

class SharedQueueSourceOp : public SourceOp
{
public:
    SharedQueueSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const Block & header_,
        const SharedQueueSourceHolderPtr & shared_queue_)
        : SourceOp(exec_context_, req_id)
        , shared_queue(shared_queue_)
    {
        setHeader(header_);
    }

    String getName() const override { return "SharedQueueSourceOp"; }

    OperatorStatus readImpl(Block & block) override;

private:
    SharedQueueSourceHolderPtr shared_queue;
};
} // namespace DB
