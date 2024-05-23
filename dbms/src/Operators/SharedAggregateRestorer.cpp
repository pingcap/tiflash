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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Events/LoadBucketEvent.h>
#include <Interpreters/Aggregator.h>
#include <Operators/SharedAggregateRestorer.h>

namespace DB
{
SharedSpilledBucketDataLoader::SharedSpilledBucketDataLoader(
    PipelineExecutorContext & exec_context_,
    const BlockInputStreams & bucket_streams,
    const String & req_id,
    size_t max_queue_size_)
    : exec_context(exec_context_)
    , log(Logger::get(req_id))
    , max_queue_size(std::max(1, max_queue_size_))
{
    for (const auto & bucket_stream : bucket_streams)
        bucket_inputs.emplace_back(bucket_stream);
    RUNTIME_CHECK(!bucket_inputs.empty());

    exec_context.incActiveRefCount();
}

SharedSpilledBucketDataLoader::~SharedSpilledBucketDataLoader()
{
    bucket_data_queue = {};
    bucket_inputs.clear();
    // In order to ensure that `PipelineExecutorContext` will not be destructed before `SharedSpilledBucketDataLoader` is destructed.
    exec_context.decActiveRefCount();
}

bool SharedSpilledBucketDataLoader::switchStatus(SharedLoaderStatus from, SharedLoaderStatus to)
{
    return status.compare_exchange_strong(from, to);
}

bool SharedSpilledBucketDataLoader::checkCancelled()
{
    if unlikely (exec_context.isCancelled())
    {
        {
            std::lock_guard lock(mu);
            if (is_cancelled)
                return true;
            is_cancelled = true;
        }
        pipe_read_cv.notifyAll();
        return true;
    }
    return false;
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
    if (checkCancelled())
        return;

    // get min bucket num.
    Int32 min_bucket_num = SpilledBucketInput::getMinBucketNum(bucket_inputs);
    if unlikely (min_bucket_num >= NUM_BUCKETS)
    {
        RUNTIME_CHECK(switchStatus(SharedLoaderStatus::loading, SharedLoaderStatus::finished));
        LOG_DEBUG(log, "shared agg restore finished");
        pipe_read_cv.notifyAll();
        return;
    }

    // store bucket data of min bucket num.
    BlocksList bucket_data = SpilledBucketInput::popOutputs(bucket_inputs, min_bucket_num);
    bool should_load_bucket{true};
    {
        std::lock_guard lock(mu);
        bucket_data_queue.push(std::move(bucket_data));
        if (bucket_data_queue.size() >= max_queue_size)
        {
            RUNTIME_CHECK(switchStatus(SharedLoaderStatus::loading, SharedLoaderStatus::idle));
            should_load_bucket = false;
        }
    }
    pipe_read_cv.notifyOne();
    if (should_load_bucket)
        loadBucket();
}

void SharedSpilledBucketDataLoader::loadBucket()
{
    RUNTIME_CHECK(status == SharedLoaderStatus::loading);
    // Although the status will always stay at `SharedLoaderStatus::loading`, but because the query has stopped, it doesn't matter.
    if (checkCancelled())
        return;


    RUNTIME_CHECK(!bucket_inputs.empty());
    auto event = std::make_shared<LoadBucketEvent>(exec_context, log->identifier(), shared_from_this());
    RUNTIME_CHECK(event->prepare());
    event->schedule();
}

bool SharedSpilledBucketDataLoader::tryPop(BlocksList & bucket_data)
{
    if (checkCancelled())
        return true;

    bool result = true;
    {
        std::lock_guard lock(mu);
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

void SharedSpilledBucketDataLoader::registerTask(TaskPtr && task)
{
    {
        std::lock_guard lock(mu);
        if (!is_cancelled && bucket_data_queue.empty() && status != SharedLoaderStatus::finished)
        {
            pipe_read_cv.registerTask(std::move(task));
            return;
        }
    }
    PipeConditionVariable::notifyTaskDirectly(std::move(task));
}

SharedAggregateRestorer::SharedAggregateRestorer(Aggregator & aggregator_, SharedSpilledBucketDataLoaderPtr loader_)
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
    case SharedLoadResult::WAIT:
        setNotifyFuture(loader);
        return false;
    }
}

SharedLoadResult SharedAggregateRestorer::tryLoadBucketData()
{
    if (!bucket_data.empty() || !restored_blocks.empty())
        return SharedLoadResult::SUCCESS;
    if (!loader->tryPop(bucket_data))
        return SharedLoadResult::WAIT;
    return bucket_data.empty() ? SharedLoadResult::FINISHED : SharedLoadResult::SUCCESS;
}

} // namespace DB
