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

#include <Interpreters/Aggregator.h>
#include <Operators/LocalAggregateRestorer.h>

namespace DB
{
LocalAggregateRestorer::LocalAggregateRestorer(
    const BlockInputStreams & bucket_streams,
    Aggregator & aggregator_,
    std::function<bool()> is_cancelled_,
    const String & req_id)
    : aggregator(aggregator_)
    , is_cancelled(std::move(is_cancelled_))
    , log(Logger::get(req_id))
{
    for (const auto & bucket_stream : bucket_streams)
        bucket_inputs.emplace_back(bucket_stream);
    assert(!bucket_inputs.empty());
}

void LocalAggregateRestorer::storeToBucketData()
{
    assert(!finished);
    assert(!bucket_inputs.empty());

    // get min bucket num.
    Int32 min_bucket_num = SpilledBucketInput::getMinBucketNum(bucket_inputs);
    if unlikely (min_bucket_num >= NUM_BUCKETS)
    {
        assert(!finished);
        finished = true;
        LOG_DEBUG(log, "local agg restore finished");
        return;
    }

    // store bucket data of min bucket num.
    bucket_data = SpilledBucketInput::popOutputs(bucket_inputs, min_bucket_num);
}

void LocalAggregateRestorer::loadBucketData()
{
    if unlikely (finished || is_cancelled())
        return;

    // load bucket data from inputs.
    assert(bucket_data.empty());
    for (auto & bucket_input : bucket_inputs)
    {
        if unlikely (is_cancelled())
            return;
        if (bucket_input.needLoad())
            bucket_input.load();
    }
    if unlikely (is_cancelled())
        return;

    storeToBucketData();
}

bool LocalAggregateRestorer::tryPop(Block & block)
{
    if unlikely (finished || is_cancelled())
        return true;

    if (restored_blocks.empty())
    {
        if (bucket_data.empty())
            return false;

        BlocksList tmp;
        std::swap(tmp, bucket_data);
        restored_blocks = aggregator.vstackBlocks(tmp, true);
        assert(!restored_blocks.empty());
    }
    block = std::move(restored_blocks.front());
    restored_blocks.pop_front();
    return true;
}
} // namespace DB
