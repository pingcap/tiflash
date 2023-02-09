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
#include <Core/Block.h>

#include <atomic>
#include <memory>
#include <vector>

namespace DB
{
struct AggregatedDataVariants;
using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;

class Aggregator;

/// Combines aggregation states together, turns them into blocks, and outputs.
class MergingBuckets
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingBuckets(const Aggregator & aggregator_, const ManyAggregatedDataVariants & data_, bool final_, size_t concurrency_);

    Block getHeader() const;

    Block getData(size_t concurrency_index);

private:
    Block getDataForSingleLevel();

    Block getDataForTwoLevel(size_t concurrency_index);

    void doLevelMerge(Int32 bucket_num, size_t concurrency_index);

private:
    const LoggerPtr log;
    const Aggregator & aggregator;
    ManyAggregatedDataVariants data;
    bool final;
    size_t concurrency;

    bool is_two_level = false;

    BlocksList single_level_blocks;

    // use unique_ptr to avoid false sharing.
    std::vector<std::unique_ptr<BlocksList>> two_level_parallel_merge_data;

    std::atomic<Int32> current_bucket_num = -1;
    static constexpr Int32 NUM_BUCKETS = 256;
};
} // namespace DB
