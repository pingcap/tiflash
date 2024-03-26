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

#include <Common/Exception.h>
#include <Operators/SharedQueue.h>

#include <magic_enum.hpp>

namespace DB
{
SharedQueuePtr SharedQueue::build(size_t producer, size_t consumer, Int64 max_buffered_bytes)
{
    RUNTIME_CHECK(producer > 0 && consumer > 0);
    // The queue size is same as UnionBlockInputStream = concurrency * 5.
    CapacityLimits queue_limits(std::max(producer, consumer) * 5, max_buffered_bytes);
    return std::make_shared<SharedQueue>(queue_limits, producer);
}

SharedQueue::SharedQueue(CapacityLimits queue_limits, size_t init_producer)
    : queue(queue_limits, [](const Block & block) { return block.allocatedBytes(); })
    , active_producer(init_producer)
{}

MPMCQueueResult SharedQueue::tryPush(Block && block)
{
    return queue.tryPush(std::move(block));
}

MPMCQueueResult SharedQueue::tryPop(Block & block)
{
    return queue.tryPop(block);
}

void SharedQueue::producerFinish()
{
    auto cur_value = active_producer.fetch_sub(1);
    RUNTIME_CHECK(cur_value >= 1);
    if (1 == cur_value)
        queue.finish();
}

OperatorStatus SharedQueueSinkOp::writeImpl(Block && block)
{
    if unlikely (!block)
        return OperatorStatus::FINISHED;

    assert(!res);
    res.emplace(std::move(block));
    return awaitImpl();
}

OperatorStatus SharedQueueSinkOp::prepareImpl()
{
    return awaitImpl();
}

OperatorStatus SharedQueueSinkOp::awaitImpl()
{
    if (!res)
        return OperatorStatus::NEED_INPUT;

    auto queue_result = shared_queue->tryPush(std::move(*res));
    switch (queue_result)
    {
    case MPMCQueueResult::FULL:
        return OperatorStatus::WAITING;
    case MPMCQueueResult::OK:
        res.reset();
        return OperatorStatus::NEED_INPUT;
    default:
        // queue result can not be finish/cancelled/empty here.
        RUNTIME_CHECK_MSG(
            false,
            "Unexpected queue result for SharedQueueSinkOp: {}",
            magic_enum::enum_name(queue_result));
    }
}

OperatorStatus SharedQueueSourceOp::readImpl(Block & block)
{
    auto await_status = awaitImpl();
    if (await_status.status == OperatorStatus::HAS_OUTPUT && res)
    {
        block = std::move(*res);
        res.reset();
    }
    return await_status;
}

OperatorStatus SharedQueueSourceOp::awaitImpl()
{
    if (res)
        return OperatorStatus::HAS_OUTPUT;

    Block block;
    auto queue_result = shared_queue->tryPop(block);
    switch (queue_result)
    {
    case MPMCQueueResult::EMPTY:
        return OperatorStatus::WAITING;
    case MPMCQueueResult::OK:
        res.emplace(std::move(block));
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::FINISHED:
        // Even after queue has finished, source op still needs to return HAS_OUTPUT.
        return OperatorStatus::HAS_OUTPUT;
    default:
        // queue result can not be cancelled/full here.
        RUNTIME_CHECK_MSG(
            false,
            "Unexpected queue result for SharedQueueSourceOp: {}",
            magic_enum::enum_name(queue_result));
    }
}
} // namespace DB
