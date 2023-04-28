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

#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Events/LoadBucketEvent.h>
#include <Interpreters/Aggregator.h>
#include <Operators/SharedAggregateRestorer.h>

namespace DB
{
SharedSpilledBucketDataLoader::SharedSpilledBucketDataLoader(
    PipelineExecutorStatus & exec_status_,
    const BlockInputStreams & bucket_streams,
    const String & req_id,
    size_t max_queue_size_)
    : exec_status(exec_status_)
    , log(Logger::get(req_id))
    , max_queue_size(std::max(1, max_queue_size_))
{
    for (const auto & bucket_stream : bucket_streams)
        bucket_inputs.emplace_back(bucket_stream);
    RUNTIME_CHECK(!bucket_inputs.empty());

    exec_status.onEventSchedule();
}

SharedSpilledBucketDataLoader::~SharedSpilledBucketDataLoader()
{
    bucket_data_queue = {};
    bucket_inputs.clear();
    // In order to ensure that `PipelineExecutorStatus` will not be destructed before `SharedSpilledBucketDataLoader` is destructed.
    exec_status.onEventFinish();
}

bool SharedSpilledBucketDataLoader::switchStatus(SharedLoaderStatus from, SharedLoaderStatus to)
{
    return status.compare_exchange_strong(from, to);
}

std::vector<SpilledBucketInput *> SharedSpilledBucketDataLoader::getNeedLoadInputs()
{
    RUNTIME_CHECK(!bucket_inputs.empty());
    std::vector<SpilledBucketInput *> load_inputs;
    for (auto & bucket_input : bucket_inputs)
    {
        if (bucket_input.needLoad())
            load_inputs.push_back(&bucket_input);
    }
    RUNTIME_CHECK(!load_inputs.empty());
    return load_inputs;
}

void SharedSpilledBucketDataLoader::storeBucketData()
{
    RUNTIME_CHECK(status == SharedLoaderStatus::loading);

    // Although the status will always stay at `SharedLoaderStatus::loading`, but because the query has stopped, it doesn't matter.
    if unlikely (exec_status.isCancelled())
        return;

    // get min bucket num.
    Int32 min_bucket_num = SpilledBucketInput::getMinBucketNum(bucket_inputs);
    if unlikely (min_bucket_num >= NUM_BUCKETS)
    {
        RUNTIME_CHECK(switchStatus(SharedLoaderStatus::loading, SharedLoaderStatus::finished));
        LOG_DEBUG(log, "shared agg restore finished");
        return;
    }

    // store bucket data of min bucket num.
    BlocksList bucket_data = SpilledBucketInput::popOutputs(bucket_inputs, min_bucket_num);
    {
        std::lock_guard lock(queue_mu);
        bucket_data_queue.push(std::move(bucket_data));
        if (bucket_data_queue.size() >= max_queue_size)
        {
            RUNTIME_CHECK(switchStatus(SharedLoaderStatus::loading, SharedLoaderStatus::idle));
            return;
        }
    }
    loadBucket();
}

void SharedSpilledBucketDataLoader::loadBucket()
{
    RUNTIME_CHECK(status == SharedLoaderStatus::loading);
    // Although the status will always stay at `SharedLoaderStatus::loading`, but because the query has stopped, it doesn't matter.
    if unlikely (exec_status.isCancelled())
        return;

    RUNTIME_CHECK(!bucket_inputs.empty());
    auto mem_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
    auto event = std::make_shared<LoadBucketEvent>(exec_status, mem_tracker, log->identifier(), shared_from_this());
    RUNTIME_CHECK(event->prepare());
    event->schedule();
}

bool SharedSpilledBucketDataLoader::tryPop(BlocksList & bucket_data)
{
    if unlikely (exec_status.isCancelled())
        return true;

    bool result = true;
    {
        std::lock_guard lock(queue_mu);
        // If `SharedSpilledBucketDataLoader` is finished, return true after the bucket_data_queue is exhausted.
        // When `tryPop` returns true and `bucket_data` is still empty, the caller knows that `SharedSpilledBucketDataLoader` is finished.
        if (bucket_data_queue.empty())
        {
            if unlikely (status == SharedLoaderStatus::finished)
                return true;
            result = false;
        }
        else
        {
            bucket_data = std::move(bucket_data_queue.front());
            bucket_data_queue.pop();
        }
    }
    if (switchStatus(SharedLoaderStatus::idle, SharedLoaderStatus::loading))
        loadBucket();
    return result;
}

SharedAggregateRestorer::SharedAggregateRestorer(
    Aggregator & aggregator_,
    SharedSpilledBucketDataLoaderPtr loader_)
    : aggregator(aggregator_)
    , loader(std::move(loader_))
{
    RUNTIME_CHECK(loader);
}

Block SharedAggregateRestorer::popFromRestoredBlocks()
{
    assert(!restored_blocks.empty());
    Block block = std::move(restored_blocks.front());
    restored_blocks.pop_front();
    return block;
}

bool SharedAggregateRestorer::tryPop(Block & block)
{
    if (!restored_blocks.empty())
    {
        block = popFromRestoredBlocks();
        return true;
    }

    auto load_res = tryLoadBucketData();
    switch (load_res)
    {
    case SharedLoadResult::SUCCESS:
    {
        BlocksList tmp;
        assert(!bucket_data.empty() && restored_blocks.empty());
        restored_blocks = aggregator.vstackBlocks(bucket_data, true);
        bucket_data = {};
        RUNTIME_CHECK(!restored_blocks.empty());
        block = popFromRestoredBlocks();
    }
    case SharedLoadResult::FINISHED:
        return true;
    case SharedLoadResult::RETRY:
        return false;
    }
}

SharedLoadResult SharedAggregateRestorer::tryLoadBucketData()
{
    if (!bucket_data.empty() || !restored_blocks.empty())
        return SharedLoadResult::SUCCESS;
    if (!loader->tryPop(bucket_data))
        return SharedLoadResult::RETRY;
    return bucket_data.empty()
        ? SharedLoadResult::FINISHED
        : SharedLoadResult::SUCCESS;
}

} // namespace DB
