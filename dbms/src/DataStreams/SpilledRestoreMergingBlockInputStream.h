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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/SpilledRestoreMergingBuckets.h>

namespace DB
{
class SpilledRestoreMergingBlockInputStream : public IProfilingBlockInputStream
{
public:
    SpilledRestoreMergingBlockInputStream(const SpilledRestoreMergingBucketsPtr & merging_buckets_, const String & req_id)
        : merging_buckets(merging_buckets_)
        , log(Logger::get(req_id))
    {
    }

    String getName() const override { return "SpilledRestoreMerging"; }

    Block getHeader() const override { return merging_buckets->getHeader(); }

protected:
    Block readImpl() override
    {
        while (true)
        {
            Block out_block = popBlocksListFront(cur_block_list);
            if (out_block)
                return out_block;
    
            auto bucket_data_to_merge = merging_buckets->restoreBucketDataToMerge([&]() { return isCancelled(); });
            if (bucket_data_to_merge.empty())
                return {};
            cur_block_list = merging_buckets->mergeBucketData(std::move(bucket_data_to_merge));
        }
    }

private:
    SpilledRestoreMergingBucketsPtr merging_buckets;
    BlocksList cur_block_list;
    const LoggerPtr log;
};
} // namespace DB
