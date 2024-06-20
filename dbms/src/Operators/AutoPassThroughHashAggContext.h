// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/Aggregator.h>

namespace DB
{
class AutoPassThroughHashAggContext
{
public:
    explicit AutoPassThroughHashAggContext(const Aggregator::Params & params_, const String & req_id_)
        : state(State::Init)
        , many_data(std::vector<AggregatedDataVariantsPtr>(1, nullptr))
    {
        aggregator = std::make_unique<Aggregator>(params_, req_id_, 1, nullptr);
        // todo make unique?
        many_data[0] = std::make_shared<AggregatedDataVariants>();
        // todo cancel hook
        // init threshold by aggregaed data variants
        agg_process_info = std::make_unique<Aggregator::AggProcessInfo>(aggregator.get());
    }

    void onBlock(Block & block);
    Block getData();

    bool passThroughBufferEmpty() const
    {
        return pass_through_block_buffer.empty();
    }

    Block popPassThroughBuffer()
    {
        Block res = pass_through_block_buffer.front();
        pass_through_block_buffer.pop_front();
        return res;
    }

    Block getHeader()
    {
        return aggregator->getHeader(/*final=*/true);
    }

    enum class State
    {
        Init,
        Adjust,
        PreHashAgg,
        PassThrough,
        Selective,
    };

    State getCurState() const
    {
        return state;
    }

    // todo change name
    size_t getNonAdjustRowLimit() const
    {
        return non_adjust_row_limit;
    }
    size_t getAdjustRowLimit() const
    {
        return adjust_row_limit;
    }
private:
    void trySwitchFromInitState();
    void trySwitchFromAdjustState(size_t total_rows, size_t hit_rows);
    void trySwitchBackAdjustState(size_t block_rows);

    void passThrough(Aggregator::AggProcessInfo & agg_process_info);

    static constexpr float PassThroughRateLimit = 0.2;
    static constexpr float PreHashAggRateLimit = 0.9;

    State state;
    // Make sure data variants after aggregator because it needs aggregator in dtor.
    // Check ~AggregatedDataVariants.
    std::unique_ptr<Aggregator> aggregator;
    ManyAggregatedDataVariants many_data;
    std::unique_ptr<Aggregator::AggProcessInfo> agg_process_info;

    size_t adjust_processed_rows = 0;
    size_t adjust_hit_rows = 0;
    size_t adjust_row_limit = 20000; // todo refine limit

    size_t state_processed_rows = 0;
    size_t non_adjust_row_limit = 70000; // todo refine limit

    BlocksList pass_through_block_buffer{};
    MergingBucketsPtr merging_buckets = nullptr;
};

using AutoPassThroughHashAggContextPtr = std::shared_ptr<AutoPassThroughHashAggContext>;
} // namespace DB
