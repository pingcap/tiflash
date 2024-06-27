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
    explicit AutoPassThroughHashAggContext(
        const Aggregator::Params & params_,
        Aggregator::CancellationHook && hook,
        const String & req_id_,
        UInt64 row_limit_unit_,
        UInt64 adjust_state_unit_num = 1,
        UInt64 other_state_unit_num = 5)
        : state(State::Init)
        , many_data(std::vector<AggregatedDataVariantsPtr>(1, nullptr))
        , adjust_row_limit(row_limit_unit_ * adjust_state_unit_num)
        , other_state_row_limit(row_limit_unit_ * other_state_unit_num)
        , row_limit_unit(row_limit_unit_)
    {
        aggregator = std::make_unique<Aggregator>(params_, req_id_, 1, nullptr);
        aggregator->setCancellationHook(hook);
        aggregator->initThresholdByAggregatedDataVariantsSize(1);
        many_data[0] = std::make_shared<AggregatedDataVariants>();
        agg_process_info = std::make_unique<Aggregator::AggProcessInfo>(aggregator.get());
        RUNTIME_CHECK(adjust_row_limit > 1024 && other_state_row_limit > 1024);
    }

    void onBlock(Block & block);
    Block getData();

    bool passThroughBufferEmpty() const { return pass_through_block_buffer.empty(); }

    Block popPassThroughBuffer()
    {
        Block res = pass_through_block_buffer.front();
        pass_through_block_buffer.pop_front();
        return res;
    }

    Block getHeader() { return aggregator->getHeader(/*final=*/true); }

    enum class State
    {
        Init,
        Adjust,
        PreHashAgg,
        PassThrough,
        Selective,
    };

    State getCurState() const { return state; }

    size_t getAdjustRowLimit() const { return adjust_row_limit; }
    size_t getOtherStateRowLimit() const { return other_state_row_limit; }

    void updateOtherStateRowLimitUnitNum(UInt64 u)
    {
        other_state_row_limit = row_limit_unit * u;
    }
    void updateAdjustStateRowLimitUnitNum(UInt64 u)
    {
        adjust_row_limit = row_limit_unit * u;
    }

private:
    void trySwitchFromInitState();
    void trySwitchFromAdjustState(size_t total_rows, size_t hit_rows);
    void trySwitchBackAdjustState(size_t block_rows);

    void passThrough(const Block & block);
    Block getPassThroughBlock(const Block & block);

    void forceSwitchToPassThroughIfSpill();
    static void makeFullSelective(Block & block);

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

    size_t state_processed_rows = 0;

    BlocksList pass_through_block_buffer{};
    bool already_start_to_get_data = false;
    MergingBucketsPtr merging_buckets = nullptr;

    size_t adjust_row_limit;
    size_t other_state_row_limit;
    size_t row_limit_unit;
};

using AutoPassThroughHashAggContextPtr = std::shared_ptr<AutoPassThroughHashAggContext>;

Block checkSelective(Block block);
} // namespace DB
