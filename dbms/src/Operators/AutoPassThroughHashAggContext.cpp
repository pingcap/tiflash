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

#include <Operators/AutoPassThroughHashAggContext.h>

namespace DB
{
// todo: need data arg or not?
void AutoPassThroughHashAggContext::onBlock(Block & block)
{
    // todo check if spilled to force passthrough?
    // todo check if two level to force passthrough?
    // todo assert getData() is not called!
    RUNTIME_CHECK_MSG(!merging_buckets, "Shouldn't insert into HashMap if start to get data");

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
            passThrough(*agg_process_info);
            trySwitchBackAdjustState(agg_process_info->block.rows());
            break;
        }
        case State::Selective:
        {
            aggregator->executeOnBlockOnlyLookup(*agg_process_info, *many_data[0], 0);
            auto pass_through_rows = agg_process_info->getNotFoundRows();
            if (!pass_through_rows.empty())
            {
                RUNTIME_CHECK(!agg_process_info->block.info.selective);
                agg_process_info->block.info.selective = std::make_shared<std::vector<UInt64>>(std::move(pass_through_rows));
                passThrough(*agg_process_info);
            }
            trySwitchBackAdjustState(agg_process_info->block.rows());
            break;
        }
        default:
        {
            __builtin_unreachable();
        }
    };
    // todo: maybe not true??
    RUNTIME_CHECK(agg_process_info->allBlockDataHandled());
}

Block AutoPassThroughHashAggContext::getData()
{
    if (!merging_buckets)
    {
        merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
        // todo: how and why force one level?
        RUNTIME_CHECK(!merging_buckets->isTwoLevel());
    }
    return merging_buckets->getData(/*concurrency_index=*/0);
}

void AutoPassThroughHashAggContext::trySwitchFromInitState()
{
    // todo check data.size?
    // todo check if expand happened?
    if (many_data[0]->bytesCount() > 1024 * 1024)
    {
        state = State::Adjust;
        state_processed_rows = 0;
    }
}

void AutoPassThroughHashAggContext::trySwitchFromAdjustState(size_t total_rows, size_t hit_rows)
{
    adjust_processed_rows += total_rows;
    adjust_hit_rows += hit_rows;

    if (adjust_processed_rows < adjust_row_limit)
        return;

    float hit_rate = static_cast<double>(adjust_hit_rows) / adjust_processed_rows;
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

    adjust_processed_rows = 0;
    adjust_hit_rows = 0;
}

void AutoPassThroughHashAggContext::trySwitchBackAdjustState(size_t block_rows)
{
    state_processed_rows += block_rows;
    if (state_processed_rows >= non_adjust_row_limit)
    {
        state = State::Adjust;
        state_processed_rows = 0;
    }
}

void AutoPassThroughHashAggContext::passThrough(Aggregator::AggProcessInfo & agg_process_info)
{
    pass_through_block_buffer.push_back(agg_process_info.block);
    // todo is necessary?
    agg_process_info.start_row = agg_process_info.end_row;
}

} // namespace DB
