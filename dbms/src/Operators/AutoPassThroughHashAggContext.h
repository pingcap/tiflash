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
#include <Operators/AutoPassThroughHashAggHelper.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct AutoPassThroughSwitcher
{
    explicit AutoPassThroughSwitcher(const ::tipb::Aggregation & aggregation)
    {
        if (aggregation.has_pre_agg_mode())
        {
            has_set = true;
            mode = aggregation.pre_agg_mode();
        }
    }
    AutoPassThroughSwitcher(bool has_set_, ::tipb::TiFlashPreAggMode mode_)
        : has_set(has_set_)
        , mode(mode_)
    {}

    bool enabled() const { return has_set && !forcePreAgg(); }

    bool forcePreAgg() const { return mode == ::tipb::TiFlashPreAggMode::ForcePreAgg; }

    bool forceStreaming() const { return mode == ::tipb::TiFlashPreAggMode::ForceStreaming; }

    bool isAuto() const { return mode == ::tipb::TiFlashPreAggMode::Auto; }

    bool has_set = false;
    ::tipb::TiFlashPreAggMode mode;
};

class AutoPassThroughHashAggContext
{
public:
    AutoPassThroughHashAggContext(
        const Block & child_header_,
        const Aggregator::Params & params_,
        Aggregator::CancellationHook && hook,
        const String & req_id_,
        UInt64 row_limit_unit_,
        UInt64 normal_unit_num_ = DEF_NORMAL_UNIT_NUM,
        UInt64 dynamic_unit_num_ = DEF_DYNAMIC_UNIT_NUM)
        : state(State::Init)
        , many_data(std::vector<AggregatedDataVariantsPtr>(1, nullptr))
        , normal_row_limit(row_limit_unit_ * normal_unit_num_)
        , dynamic_row_limit(row_limit_unit_ * dynamic_unit_num_)
        , row_limit_unit(row_limit_unit_)
        , max_dynamic_row_limit(row_limit_unit_ * MAX_DYNAMIC_UNIT_LIMIT)
        , log(Logger::get(req_id_))
    {
        aggregator
            = std::make_unique<Aggregator>(params_, req_id_, /*concurrency=*/1, nullptr, /*is_auto_pass_through=*/true);
        aggregator->setCancellationHook(hook);
        aggregator->initThresholdByAggregatedDataVariantsSize(1);
        RUNTIME_CHECK(aggregator->getParams().keys_size > 0);

        many_data[0] = std::make_shared<AggregatedDataVariants>();
        agg_process_info = std::make_unique<Aggregator::AggProcessInfo>(aggregator.get());

        auto header = aggregator->getHeader(/*final=*/true);
        const auto & aggregate_descriptions = aggregator->getParams().aggregates;
        column_generators = setupAutoPassThroughColumnGenerator(header, child_header_, aggregate_descriptions, log);
        RUNTIME_CHECK(header.columns() == column_generators.size());
    }

    ~AutoPassThroughHashAggContext() { statistics.log(log); }

    template <bool force_streaming>
    void onBlock(Block & block)
    {
        if constexpr (force_streaming)
        {
            statistics.update(State::PassThrough, block.rows());
            onBlockForceStreaming(block);
        }
        else
        {
            forceState();
            statistics.update(state, block.rows());
            onBlockAuto(block);
        }
    }

    Block tryGetDataInAdvance();
    Block getDataFromHashTable();

    Block getHeader() { return aggregator->getHeader(/*final=*/true); }

    // TODO: Stop insert new rows into HashTable when start converting HashTable to two level
    enum class State
    {
        Init,
        Adjust,
        PreHashAgg,
        PassThrough,
        Selective,
    };

    State getCurState() const { return state; }

    size_t getAdjustRowLimit() const { return normal_row_limit; }
    size_t getDynamicRowLimit() const { return dynamic_row_limit; }
    void updateDynamicRowLimitUnitNum(UInt64 u) { dynamic_row_limit = row_limit_unit * u; }
    void updateAdjustStateRowLimitUnitNum(UInt64 u) { normal_row_limit = row_limit_unit * u; }

private:
    void onBlockAuto(Block & block);
    void onBlockForceStreaming(Block & block);

    void trySwitchFromInitState();
    void trySwitchFromAdjustState(size_t total_rows, size_t hit_rows);
    void trySwitchBackAdjustState(size_t block_rows);

    void pushPassThroughBuffer(const Block & block);
    Block getPassThroughBlock(const Block & block);

    void forceState();
    static void makeFullSelective(Block & block);

    Block popPassThroughBuffer()
    {
        Block res = pass_through_block_buffer.front();
        pass_through_block_buffer.pop_front();
        return res;
    }

    static constexpr double PassThroughRateLimit = 0.2;
    static constexpr double PreHashAggRateLimit = 0.9;

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
    // True when:
    // 1. HashTable needs to spill. So we need to pass through all blocks from HashTable.
    // 2. All children blocks are handled and we start to return blocks.
    bool already_get_data_from_hash_table = false;
    MergingBucketsPtr merging_buckets = nullptr;

    struct Statistics
    {
        void update(const State & state, size_t rows)
        {
            switch (state)
            {
            case State::Init:
                init_rows += rows;
                break;
            case State::Adjust:
                adjust_rows += rows;
                break;
            case State::PreHashAgg:
                pre_hashagg_rows += rows;
                break;
            case State::PassThrough:
                pass_through_rows += rows;
                break;
            case State::Selective:
                selective_rows += rows;
                break;
            default:
                __builtin_unreachable();
            };
            total_handled_rows += rows;
        }

        void log(LoggerPtr log)
        {
            LOG_DEBUG(
                log,
                "auto pass through hash agg info: total: {}, init: {}, adjust: {}, pre hashagg: {}, pass through: {}, "
                "selective: {}",
                total_handled_rows,
                init_rows,
                adjust_rows,
                pre_hashagg_rows,
                pass_through_rows,
                selective_rows);
        }

        size_t init_rows = 0;
        size_t adjust_rows = 0;
        size_t selective_rows = 0;
        size_t pre_hashagg_rows = 0;
        size_t pass_through_rows = 0;
        size_t total_handled_rows = 0;
    };
    Statistics statistics;

    // Row limit for adjust and preagg state. It's fixed(PS: gtest may change it).
    size_t normal_row_limit;
    // Row limit for selective and pass through. It will get larger in runtime.
    size_t dynamic_row_limit;
    size_t row_limit_unit;
    const size_t max_dynamic_row_limit;

    LoggerPtr log;

    static constexpr size_t INIT_STATE_HASHMAP_THRESHOLD = 2 * 1024 * 1024;
    static constexpr size_t MAX_DYNAMIC_UNIT_LIMIT = 100;
    static constexpr size_t DEF_NORMAL_UNIT_NUM = 1;
    static constexpr size_t DEF_DYNAMIC_UNIT_NUM = 5;

    std::vector<AutoPassThroughColumnGenerator> column_generators;
};

using AutoPassThroughHashAggContextPtr = std::shared_ptr<AutoPassThroughHashAggContext>;
} // namespace DB
