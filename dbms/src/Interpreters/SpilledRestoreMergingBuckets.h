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
#include <DatStreams/ConcatBlockInputStream.h>
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
        size_t restore_concurrency_,
        const String & req_id);

    Block getHeader() const;

    BlocksList restoreBucketDataToMerge(std::function<bool()> && is_cancelled);

    BlocksList mergeBucketData(BlocksList && bucket_data_to_merge);

    size_t getConcurrency() const { return restore_concurrency; }

private:
    BlocksList restoreBucketDataToMergeForLocalAgg(std::function<bool()> && is_cancelled);
    BlocksList restoreBucketDataToMergeForNonLocalAgg(std::function<bool()> && is_cancelled);

private:
    const LoggerPtr log;

    Aggregator aggregator;
    bool final;

    size_t restore_concurrency;

    bool is_local_agg;

    std::atomic_int32_t current_bucket_num = -1;
    struct Input
    {
        BlockInputStreamPtr stream;
        Block block;
        bool is_exhausted = false;

        Input(BlockInputStreams && streams, const String & req_id)
            : stream(std::make_shared<ConcatBlockInputStream>(std::move(streams), req_id))
        {}
    };
    std::vector<Input> restore_inputs;
};
using SpilledRestoreMergingBucketsPtr = std::shared_ptr<SpilledRestoreMergingBuckets>;

} // namespace DB
