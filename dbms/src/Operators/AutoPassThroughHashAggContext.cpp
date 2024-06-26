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
void AutoPassThroughHashAggContext::onBlock(Block & block)
{
    RUNTIME_CHECK_MSG(!already_start_to_get_data, "Shouldn't insert into HashMap if already start to get data");

    forceSwitchToPassThroughIfSpill();
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
        auto new_block = getPassThroughBlock(agg_process_info->block);
        makeFullSelective(new_block);
        passThrough(new_block);
        trySwitchBackAdjustState(total_rows);
        break;
    }
    case State::Selective:
    {
        aggregator->executeOnBlockOnlyLookup(*agg_process_info, *many_data[0], 0);
        auto pass_through_rows = agg_process_info->getNotFoundRows();
        // todo assert allBLockDataHandled?
        const auto total_rows = agg_process_info->block.rows();
        if (!pass_through_rows.empty())
        {
            RUNTIME_CHECK(!agg_process_info->block.info.selective);
            auto new_block = getPassThroughBlock(agg_process_info->block);
            new_block.info.selective = std::make_shared<std::vector<UInt64>>(std::move(pass_through_rows));
            passThrough(new_block);
        }
        trySwitchBackAdjustState(total_rows);
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

void AutoPassThroughHashAggContext::forceSwitchToPassThroughIfSpill()
{
    if (many_data[0]->need_spill)
        state = State::PassThrough;
}

Block AutoPassThroughHashAggContext::getData()
{
    if unlikely (!already_start_to_get_data)
    {
        already_start_to_get_data = true;
        RUNTIME_CHECK(!merging_buckets);
        merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
    }

    // merging_buckets still can be nullptr when HashMap is empty.
    if (merging_buckets)
    {
        auto block = merging_buckets->getData(/*concurrency_index=*/0);
        makeFullSelective(block);
        return block;
    }
    return {};
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

void AutoPassThroughHashAggContext::passThrough(const Block & block)
{
    pass_through_block_buffer.push_back(block);
}

Block AutoPassThroughHashAggContext::getPassThroughBlock(const Block & block)
{
    auto header = aggregator->getHeader(/*final*/ true);
    Block new_block;
    const auto & aggregate_descriptions = aggregator->getParams().aggregates;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto col_name = header.getByPosition(i).name;
        if (block.has(col_name))
        {
            new_block.insert(i, block.getByName(col_name));
            continue;
        }

        bool agg_func_col_found = false;
        for (const auto & desc : aggregate_descriptions)
        {
            if (desc.column_name == col_name)
            {
                auto new_col = desc.function->getReturnType()->createColumn();
                new_col->reserve(block.rows());
                auto * place = new char[desc.function->sizeOfData()];
                desc.function->create(place);
                for (size_t i = 0; i < block.rows(); ++i)
                {
                    // todo arena nullptr ok?
                    desc.function->insertResultInto(place, *new_col, /*arena*/ nullptr);
                }
                delete[] place;
                new_block.insert(
                    i,
                    ColumnWithTypeAndName{std::move(new_col), desc.function->getReturnType(), desc.column_name});
                agg_func_col_found = true;
                break;
            }
        }
        RUNTIME_CHECK_MSG(agg_func_col_found, "cannot find agg func column({}) from aggregate descriptions", col_name);
    }
    return new_block;
}

void AutoPassThroughHashAggContext::makeFullSelective(Block & block)
{
    if (!block)
        return;

    RUNTIME_CHECK(!block.info.selective);
    auto selective = std::make_shared<std::vector<UInt64>>(block.rows());
    block.info.selective = selective;
    // todo maybe better impl
    for (size_t i = 0; i < selective->size(); ++i)
    {
        (*selective)[i] = i;
    }
}

Block checkSelective(Block block)
{
    RUNTIME_CHECK(!block || block.info.selective);
    return block;
}
} // namespace DB
