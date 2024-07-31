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

#include <Columns/ColumnDecimal.h>
#include <Operators/AutoPassThroughHashAggContext.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>

namespace DB
{
void AutoPassThroughHashAggContext::onBlockAuto(Block & block)
{
    RUNTIME_CHECK_MSG(!already_start_to_get_data, "Shouldn't insert into HashMap if already start to get data");

    agg_process_info->resetBlock(block);
    switch (state)
    {
    case State::Init:
    {
        aggregator->executeOnBlock(*agg_process_info, *many_data[0], 0);
        trySwitchFromInitState();
        break;
    }
    case State::Adjust:
    {
        aggregator->executeOnBlockCollectHitRate(*agg_process_info, *many_data[0], 0);
        trySwitchFromAdjustState(agg_process_info->block.rows(), agg_process_info->hit_row_cnt);
        break;
    }
    case State::PreHashAgg:
    {
        aggregator->executeOnBlock(*agg_process_info, *many_data[0], 0);
        trySwitchBackAdjustState(agg_process_info->block.rows());
        break;
    }
    case State::PassThrough:
    {
        const auto total_rows = agg_process_info->block.rows();
        onBlockForceStreaming(agg_process_info->block);
        trySwitchBackAdjustState(total_rows);
        break;
    }
    case State::Selective:
    {
        aggregator->executeOnBlockOnlyLookup(*agg_process_info, *many_data[0], 0);
        auto pass_through_rows = agg_process_info->not_found_rows;
        const auto total_rows = agg_process_info->block.rows();
        if (!pass_through_rows.empty())
        {
            RUNTIME_CHECK(!agg_process_info->block.info.selective);
            auto new_block = getPassThroughBlock(agg_process_info->block);
            new_block.info.selective = std::make_shared<std::vector<UInt64>>(std::move(pass_through_rows));
            pushPassThroughBuffer(new_block);
        }
        trySwitchBackAdjustState(total_rows);
        break;
    }
    default:
    {
        __builtin_unreachable();
    }
    };
    RUNTIME_CHECK(agg_process_info->allBlockDataHandled());
}

void AutoPassThroughHashAggContext::onBlockForceStreaming(Block & block)
{
    auto new_block = getPassThroughBlock(block);
    pushPassThroughBuffer(new_block);
}

void AutoPassThroughHashAggContext::forceState()
{
    if (many_data[0]->need_spill)
        state = State::PassThrough;
}

Block AutoPassThroughHashAggContext::getData()
{
    if unlikely (!already_start_to_get_data)
    {
        // No need to handle situation when agg keys_size is zero.
        // Shouldn't use auto pass through hashagg in that case.
        already_start_to_get_data = true;
        RUNTIME_CHECK(!merging_buckets);
        merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
    }

    // merging_buckets still can be nullptr when HashMap is empty.
    if (merging_buckets)
    {
        return merging_buckets->getData(/*concurrency_index=*/0);
    }
    return {};
}

void AutoPassThroughHashAggContext::trySwitchFromInitState()
{
    const auto & ht = many_data[0];
    if (ht->bytesCount() > INIT_STATE_HASHMAP_THRESHOLD)
    {
        LOG_DEBUG(log, "init state transfer to adjust state, hash map bytes: {}, rows: {}", ht->bytesCount(), ht->size());
        state = State::Adjust;
    }
}

void AutoPassThroughHashAggContext::trySwitchFromAdjustState(size_t total_rows, size_t hit_rows)
{
    adjust_processed_rows += total_rows;
    adjust_hit_rows += hit_rows;

    if (adjust_processed_rows < adjust_row_limit)
        return;

    double hit_rate = static_cast<double>(adjust_hit_rows) / adjust_processed_rows;
    RUNTIME_CHECK(std::isnormal(hit_rate) || hit_rate == 0.0);
    if (hit_rate >= PreHashAggRateLimit)
    {
        state = State::PreHashAgg;
    }
    else if (hit_rate <= PassThroughRateLimit)
    {
        state = State::PassThrough;
    }
    else
    {
        state = State::Selective;
    }

    LOG_DEBUG(
        log,
        "adjust state info transfer to other state. state: {}, processed: {}, hit: {}, limit: {}",
        magic_enum::enum_name(state),
        adjust_processed_rows,
        adjust_hit_rows,
        adjust_row_limit);

    adjust_processed_rows = 0;
    adjust_hit_rows = 0;
}

void AutoPassThroughHashAggContext::trySwitchBackAdjustState(size_t block_rows)
{
    state_processed_rows += block_rows;
    if (state_processed_rows >= other_state_row_limit)
    {
        LOG_DEBUG(
                log,
                "other state back to adjust state: state: {}, processed: {}, limit: {}",
                magic_enum::enum_name(state),
                state_processed_rows,
                other_state_row_limit);

        state = State::Adjust;
        state_processed_rows = 0;
    }
}

void AutoPassThroughHashAggContext::pushPassThroughBuffer(const Block & block)
{
    pass_through_block_buffer.push_back(block);
}

Block AutoPassThroughHashAggContext::getPassThroughBlock(const Block & block)
{
    auto new_block = aggregator->getHeader(/*final=*/true);
    // header.columns() should be equal to  column_generators.size(), already checked in ctor.
    for (size_t i = 0; i < new_block.columns(); ++i)
    {
        auto res = column_generators[i](block);
        RUNTIME_CHECK(res);
        new_block.getByPosition(i).column = res;
    }
    return new_block;
}
} // namespace DB
