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

#include <Interpreters/Aggregator.h>

namespace DB
{
class AutoPassThroughHashAggContext
{
public:
    explicit AutoPassThroughHashAggContext(std::unique_ptr<Aggregator> aggregator_, size_t thread_num_)
        : aggregator(std::move(aggregator_))
        , state(State::Init)
        , thread_num(thread_num_)
    {}

    void onBlock(Aggregator::AggProcessInfo & agg_process_info, Block & block, AggregatedDataVariantsPtr data);
    Block getData(AggregatedDataVariantsPtr data);

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

private:
    enum class State
    {
        Init,
        Adjust,
        PreHashAgg,
        PassThrough,
        Selective,
    };

    void trySwitchFromInitState(const AggregatedDataVariants & data);
    void trySwitchFromAdjustState(size_t total_rows, size_t hit_rows);
    void trySwitchBackAdjustState(size_t block_rows);

    // todo del
    // void execute(
    //         Aggregator::AggProcessInfo & agg_process_info,
    //         AggregatedDataVariants & data,
    //         bool collect_hit_rate);
    void passThrough(Aggregator::AggProcessInfo & agg_process_info);

    static constexpr float PassThroughRateLimit = 0.2;
    static constexpr float PreHashAggRateLimit = 0.9;

    std::unique_ptr<Aggregator> aggregator;
    State state;
    size_t thread_num;

    size_t adjust_processed_rows = 0;
    size_t adjust_hit_rows = 0;
    size_t adjust_row_limit = 20000; // todo refine limit

    size_t non_adjust_processed_rows = 0;
    size_t non_adjust_row_limit = 600000; // todo refine limit

    BlocksList pass_through_block_buffer{};
    MergingBucketsPtr merging_buckets = nullptr;
};

using AutoPassThroughHashAggContextPtr = std::shared_ptr<AutoPassThroughHashAggContext>;
} // namespace DB
