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

void LocalAggregateRestorer::finish()
{
    assert(!finished);
    finished = true;
    bucket_inputs.clear();
    LOG_INFO(log, "local agg restore finished");
}

bool LocalAggregateRestorer::loadFromInputs()
{
    assert(!bucket_inputs.empty());
    for (auto it = bucket_inputs.begin(); it != bucket_inputs.end() && unlikely(!is_cancelled());)
        it = it->load() ? std::next(it) : bucket_inputs.erase(it);
    if unlikely (is_cancelled() || bucket_inputs.empty())
    {
        finish();
        return false;
    }
    return true;
}

void LocalAggregateRestorer::storeFromInputToBucketData()
{
    assert(!bucket_inputs.empty());

    // get min bucket num.
    Int32 min_bucket_num = NUM_BUCKETS;
    for (auto & bucket_input : bucket_inputs)
        min_bucket_num = std::min(bucket_input.bucketNum(), min_bucket_num);
    if unlikely (min_bucket_num >= NUM_BUCKETS)
    {
        finish();
        return;
    }

    // store bucket data of min bucket num.
    for (auto & bucket_input : bucket_inputs)
    {
        if (min_bucket_num == bucket_input.bucketNum())
            bucket_data.push_back(bucket_input.moveOutput());
    }
    assert(!bucket_data.empty());
}

void LocalAggregateRestorer::loadBucketData()
{
    if unlikely (finished || is_cancelled())
        return;

    assert(bucket_data.empty());
    if (loadFromInputs())
        storeFromInputToBucketData();
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

LocalAggregateRestorer::Input::Input(const BlockInputStreamPtr & stream_)
    : stream(stream_)
{
    stream->readPrefix();
}

Block LocalAggregateRestorer::Input::moveOutput()
{
    assert(output.has_value());
    Block ret = std::move(*output);
    output.reset();
    return ret;
}

Int32 LocalAggregateRestorer::Input::bucketNum() const
{
    assert(output.has_value());
    return output->info.bucket_num;
}

bool LocalAggregateRestorer::Input::load()
{
    if unlikely (is_exhausted)
        return false;

    if (output.has_value())
        return true;

    Block ret = stream->read();
    if unlikely (!ret)
    {
        is_exhausted = true;
        stream->readSuffix();
        return false;
    }
    else
    {
        /// Only two level data can be spilled.
        assert(ret.info.bucket_num != -1);
        output.emplace(std::move(ret));
        return true;
    }
}
} // namespace DB
