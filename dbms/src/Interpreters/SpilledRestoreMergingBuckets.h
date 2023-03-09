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

#include <Common/Logger.h>
#include <Interpreters/Aggregator.h>

#include <atomic>

namespace DB
{
class SpilledRestoreMergingBuckets
{
public:
    SpilledRestoreMergingBuckets(
        std::vector<BlockInputStreams> && bucket_restore_streams_,
        const Aggregator::Params & params,
        bool final_,
        const String & req_id);

    Block getHeader() const;

    BlocksList restoreBucketDataToMerge(std::function<bool()> && is_cancelled);

    BlocksList mergeBucketData(BlocksList && bucket_data_to_merge);

    size_t getConcurrency() const { return std::min(concurrency, bucket_restore_streams.size()); }

private:
    BlocksList restoreBucketDataToMergeInConcurrency(std::function<bool()> && is_cancelled);
    BlocksList restoreBucketDataToMergeNotConcurrency(std::function<bool()> && is_cancelled);

private:
    const LoggerPtr log;

    Aggregator aggregator;
    bool final;

    size_t concurrency;

    std::vector<BlockInputStreams> bucket_restore_streams;

    // for concurrency > 1
    std::atomic_uint32_t current_bucket_num = 0;
    // for concurrency == 1
    int32_t concurrent_stream_num = -1;
};
using SpilledRestoreMergingBucketsPtr = std::shared_ptr<SpilledRestoreMergingBuckets>;

} // namespace DB
