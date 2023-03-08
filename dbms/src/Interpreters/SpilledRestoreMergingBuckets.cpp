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
    const String & req_id)
    : log(Logger::get(req_id))
    , aggregator(params, req_id)
    , final(final_)
    , bucket_restore_streams(std::move(bucket_restore_streams_))
{
}

Block SpilledRestoreMergingBuckets::getHeader() const
{
    return aggregator.getHeader(final);
}

BlocksList SpilledRestoreMergingBuckets::restoreBucketDataToMerge(std::function<bool()> && is_cancelled)
{
    if (current_bucket_num >= bucket_restore_streams.size())
        return {};

    while (true)
    {
        auto local_bucket_num = current_bucket_num.fetch_add(1);
        if (local_bucket_num >= bucket_restore_streams.size())
            return {};

        if (bucket_restore_streams[local_bucket_num].empty())
            continue;

        BlocksList ret;
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

BlocksList SpilledRestoreMergingBuckets::mergeBucketData(BlocksList && bucket_data_to_merge)
{
    auto ret = aggregator.vstackBlocks(bucket_data_to_merge, final);
    bucket_data_to_merge.clear();
    return ret;
}
} // namespace DB
