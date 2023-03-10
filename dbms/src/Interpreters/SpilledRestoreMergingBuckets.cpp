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

#include <Interpreters/SpilledRestoreMergingBuckets.h>

namespace DB
{
SpilledRestoreMergingBuckets::SpilledRestoreMergingBuckets(
    std::vector<BlockInputStreams> && bucket_restore_streams_,
    const Aggregator::Params & params,
    bool final_,
    size_t restore_concurrency_,
    const String & req_id)
    : log(Logger::get(req_id))
    , aggregator(params, req_id)
    , final(final_)
    , restore_concurrency(restore_concurrency_)
    , is_local_agg(params.is_local_agg)
    , restore_inputs(bucket_restore_streams_.begin(), bucket_restore_streams_.end())
{
    RUNTIME_CHECK(!restore_inputs.empty() && restore_concurrency > 0);
}

Block SpilledRestoreMergingBuckets::getHeader() const
{
    return aggregator.getHeader(final);
}

BlocksList SpilledRestoreMergingBuckets::restoreBucketDataToMergeForNonLocalAgg(std::function<bool()> && is_cancelled)
{
    if (current_bucket_num >= restore_inputs.size())
        return {};

    while (true)
    {
        auto local_bucket_num = current_bucket_num.fetch_add(1);
        if (local_bucket_num >= restore_inputs.size())
            return {};

        BlocksList ret;
        const auto & cur_input = restore_inputs[local_bucket_num];
        assert(!restore_inputs.is_exhausted);
        
        
        for (const auto & bucket_restore_stream : bucket_restore_streams[local_bucket_num])
        {
            if unlikely (is_cancelled())
                return {};
            bucket_restore_stream->readPrefix();
            while (Block block = bucket_restore_stream->read())
            {
                // Only two level data can be spilled.
                assert(block.info.bucket_num != -1);
                if unlikely (is_cancelled())
                    return {};
                ret.push_back(std::move(block));
            }
            bucket_restore_stream->readSuffix();
        }
        if unlikely (is_cancelled())
            return {};
        if (ret.empty())
            continue;
        return ret;
    }
}

BlocksList SpilledRestoreMergingBuckets::restoreBucketDataToMergeForLocalAgg(std::function<bool()> && is_cancelled)
{
    assert(bucket_restore_streams.size() == 1);
    const auto & restore_streams = bucket_restore_streams.back();
    if unlikely (restore_streams.empty())
        return {};

    if unlikely (current_bucket_num == -1)
    {

    }
    while (true)
    {
        if unlikely (is_cancelled())
            return {};

        if (concurrent_stream_num >= static_cast<int32_t>(restore_streams.size()))
            return {};

        if unlikely (concurrent_stream_num == -1)
        {
            concurrent_stream_num = 0;
            restore_streams[0]->readPrefix();
        }

        const auto & restore_stream = restore_streams[concurrent_stream_num];
        if (Block block = restore_stream->read(); block)
        {
            return {std::move(block)};
        }
        else
        {
            restore_stream->readSuffix();
            ++concurrent_stream_num;
        }
    }
}

BlocksList SpilledRestoreMergingBuckets::restoreBucketDataToMerge(std::function<bool()> && is_cancelled)
{
    return is_local_agg ? restoreBucketDataToMergeForLocalAgg(std::move(is_cancelled)) : restoreBucketDataToMergeForNonLocalAgg(std::move(is_cancelled));
}

BlocksList SpilledRestoreMergingBuckets::mergeBucketData(BlocksList && bucket_data_to_merge)
{
    auto ret = aggregator.vstackBlocks(bucket_data_to_merge, final);
    bucket_data_to_merge.clear();
    return ret;
}
} // namespace DB
