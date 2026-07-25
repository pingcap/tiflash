// Copyright 2026 PingCAP, Inc.
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

#include <Columns/ColumnsCommon.h>
#include <Common/Exception.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationBlockInputStream.h>

namespace DB::DM
{
namespace
{
constexpr size_t MIN_SAMPLE_BLOCKS = 4;
constexpr size_t MIN_SAMPLE_ROWS = 16384;

void filterBlock(Block & block, const IColumn::Filter & filter, size_t passed_count)
{
    if (!block)
        return;

    for (auto & col : block)
    {
        if (col.column->isColumnConst())
            col.column = col.column->cut(0, passed_count);
        else
            col.column = col.column->filter(filter, passed_count);
    }
}

const char * modeToString(MultiStageLateMaterializationBlockInputStream::AdaptiveMode mode)
{
    switch (mode)
    {
    case MultiStageLateMaterializationBlockInputStream::AdaptiveMode::Sampling:
        return "sampling";
    case MultiStageLateMaterializationBlockInputStream::AdaptiveMode::Late:
        return "late";
    case MultiStageLateMaterializationBlockInputStream::AdaptiveMode::Direct:
        return "direct";
    }
    return "unknown";
}

} // namespace

MultiStageLateMaterializationBlockInputStream::MultiStageLateMaterializationBlockInputStream(
    const ColumnDefines & columns_to_read,
    BlockInputStreamPtr stage0_filter_stream_,
    SkippableBlockInputStreamPtr stage1_filter_stream_,
    SkippableBlockInputStreamPtr final_rest_stream_,
    const PushDownFilterPtr & residual_filter_,
    const BitmapFilterPtr & bitmap_filter_,
    const String & req_id_)
    : header(toEmptyBlock(columns_to_read))
    , stage0_filter_stream(std::move(stage0_filter_stream_))
    , stage1_filter_stream(std::move(stage1_filter_stream_))
    , final_rest_stream(std::move(final_rest_stream_))
    , residual_filter(residual_filter_)
    , bitmap_filter(bitmap_filter_)
    , residual_filter_action(
          buildResidualFilterHeader(residual_filter_),
          residual_filter_->before_where,
          residual_filter_->filter_column_name)
    , log(Logger::get(NAME, req_id_))
{
    RUNTIME_CHECK(residual_filter != nullptr);
    RUNTIME_CHECK(residual_filter->before_where != nullptr);
    RUNTIME_CHECK(residual_filter->filter_columns != nullptr);
}

Block MultiStageLateMaterializationBlockInputStream::buildResidualFilterHeader(
    const PushDownFilterPtr & residual_filter)
{
    RUNTIME_CHECK(residual_filter != nullptr);
    RUNTIME_CHECK(residual_filter->filter_columns != nullptr);

    auto filter_header = toEmptyBlock(*residual_filter->filter_columns);
    if (residual_filter->extra_cast)
        residual_filter->extra_cast->execute(filter_header);
    return filter_header;
}

IColumn::Filter MultiStageLateMaterializationBlockInputStream::composeFilters(
    const IColumn::Filter * stage0_filter,
    size_t stage0_rows,
    const IColumn::Filter & residual_filter)
{
    if (stage0_filter == nullptr)
    {
        RUNTIME_CHECK_MSG(
            residual_filter.size() == stage0_rows,
            "Unexpected residual filter size, residual_filter_size={}, stage0_rows={}",
            residual_filter.size(),
            stage0_rows);
        IColumn::Filter combined_filter;
        combined_filter.assign(residual_filter.begin(), residual_filter.end());
        return combined_filter;
    }

    RUNTIME_CHECK_MSG(
        stage0_filter->size() == stage0_rows,
        "Unexpected stage0 filter size, stage0_filter_size={}, stage0_rows={}",
        stage0_filter->size(),
        stage0_rows);

    IColumn::Filter combined_filter(stage0_rows, 0);
    size_t residual_pos = 0;
    for (size_t i = 0; i < stage0_rows; ++i)
    {
        if ((*stage0_filter)[i] == 0)
            continue;

        RUNTIME_CHECK_MSG(
            residual_pos < residual_filter.size(),
            "Residual filter is shorter than stage0 passed rows, residual_pos={}, residual_filter_size={}",
            residual_pos,
            residual_filter.size());
        combined_filter[i] = residual_filter[residual_pos];
        ++residual_pos;
    }

    RUNTIME_CHECK_MSG(
        residual_pos == residual_filter.size(),
        "Residual filter is longer than stage0 passed rows, residual_pos={}, residual_filter_size={}",
        residual_pos,
        residual_filter.size());
    return combined_filter;
}

MultiStageLateMaterializationBlockInputStream::EffectiveFilter
MultiStageLateMaterializationBlockInputStream::buildStage0EffectiveFilter(Block & stage0_block, FilterPtr stage0_filter)
{
    EffectiveFilter effective_filter;
    const auto rows = stage0_block.rows();

    if (stage0_filter != nullptr)
    {
        RUNTIME_CHECK_MSG(
            stage0_filter->size() == rows,
            "Unexpected stage0 filter size, filter_size={}, rows={}",
            stage0_filter->size(),
            rows);
        bitmap_filter->rangeAnd(*stage0_filter, stage0_block.startOffset(), rows);
        effective_filter.filter = stage0_filter;
        effective_filter.passed_count = countBytesInFilter(*stage0_filter);
        return effective_filter;
    }

    effective_filter.holder.resize(rows);
    if (bitmap_filter->get(effective_filter.holder, stage0_block.startOffset(), rows))
    {
        effective_filter.filter = nullptr;
        effective_filter.passed_count = rows;
        return effective_filter;
    }

    effective_filter.use_holder = true;
    effective_filter.passed_count = countBytesInFilter(effective_filter.holder);
    return effective_filter;
}

void MultiStageLateMaterializationBlockInputStream::skipNextBlockOrRead(
    SkippableBlockInputStreamPtr & stream,
    const char * stream_name)
{
    if (size_t skipped_rows = stream->skipNextBlock(); skipped_rows == 0)
    {
        stream->read();
        LOG_ERROR(log, "Multi-stage late materialization skip block failed, stream={}", stream_name);
    }
}

Block MultiStageLateMaterializationBlockInputStream::readWithOptionalFilter(
    SkippableBlockInputStreamPtr & stream,
    const IColumn::Filter * filter,
    size_t passed_count,
    const char * stream_name)
{
    if (filter == nullptr)
        return stream->read();

    const auto rows = filter->size();
    if (passed_count == 0)
    {
        skipNextBlockOrRead(stream, stream_name);
        return {};
    }

    if (passed_count == rows)
        return stream->read();

    Block block;
    const auto filter_out_count = rows - passed_count;
    if (filter_out_count >= DEFAULT_MERGE_BLOCK_SIZE * 2)
    {
        block = stream->readWithFilter(*filter);
    }
    else
    {
        block = stream->read();
        filterBlock(block, *filter, passed_count);
    }
    return block;
}

size_t MultiStageLateMaterializationBlockInputStream::executeResidualFilter(
    Block & stage1_block,
    Block & filter_eval_block,
    FilterPtr & residual_filter_ptr)
{
    residual_filter_ptr = nullptr;

    if (residual_filter_action.alwaysFalse())
    {
        return 0;
    }

    filter_eval_block = stage1_block;
    if (residual_filter->extra_cast)
        residual_filter->extra_cast->execute(filter_eval_block);

    residual_filter_action.transform(filter_eval_block, residual_filter_ptr, true);
    if (!filter_eval_block)
        return 0;

    if (residual_filter_ptr == nullptr)
        return stage1_block.rows();

    RUNTIME_CHECK_MSG(
        residual_filter_ptr->size() == stage1_block.rows(),
        "Unexpected residual filter size, filter_size={}, stage1_rows={}",
        residual_filter_ptr->size(),
        stage1_block.rows());
    return countBytesInFilter(*residual_filter_ptr);
}

void MultiStageLateMaterializationBlockInputStream::updateAdaptiveState(
    size_t stage0_passed_rows,
    size_t residual_passed_rows)
{
    if (adaptive_mode != AdaptiveMode::Sampling)
        return;

    ++sample_blocks;
    sample_stage0_rows += stage0_passed_rows;
    sample_residual_passed_rows += residual_passed_rows;

    if (sample_blocks < MIN_SAMPLE_BLOCKS || sample_stage0_rows < MIN_SAMPLE_ROWS)
        return;

    adaptive_mode = sample_residual_passed_rows * 2 < sample_stage0_rows ? AdaptiveMode::Late : AdaptiveMode::Direct;
}

bool MultiStageLateMaterializationBlockInputStream::shouldUseLateMode(
    size_t stage0_passed_rows,
    size_t residual_passed_rows) const
{
    if (residual_passed_rows == 0)
        return true;
    if (residual_passed_rows == stage0_passed_rows)
        return false;

    switch (adaptive_mode)
    {
    case AdaptiveMode::Late:
        return true;
    case AdaptiveMode::Direct:
        return false;
    case AdaptiveMode::Sampling:
        return residual_passed_rows * 2 < stage0_passed_rows;
    }
    return false;
}

Block MultiStageLateMaterializationBlockInputStream::buildLateModeBlock(
    Block & stage1_block,
    const IColumn::Filter * stage0_filter,
    size_t stage0_rows,
    const IColumn::Filter & residual_filter_,
    size_t residual_passed_rows)
{
    ++late_mode_blocks;
    auto combined_filter = composeFilters(stage0_filter, stage0_rows, residual_filter_);
    auto final_rest_block
        = readWithOptionalFilter(final_rest_stream, &combined_filter, residual_passed_rows, "final_rest");
    filterBlock(stage1_block, residual_filter_, residual_passed_rows);

    RUNTIME_CHECK_MSG(
        final_rest_block.startOffset() == stage1_block.startOffset(),
        "Multi-stage late materialization meets unexpected block unmatched in late mode, stage1_block: "
        "[start_offset={}, rows={}], final_rest_block: [start_offset={}, rows={}], pass_count={}",
        stage1_block.startOffset(),
        stage1_block.rows(),
        final_rest_block.startOffset(),
        final_rest_block.rows(),
        residual_passed_rows);
    return hstackBlocks({std::move(stage1_block), std::move(final_rest_block)}, header);
}

Block MultiStageLateMaterializationBlockInputStream::buildDirectModeBlock(
    Block & stage1_block,
    const IColumn::Filter * stage0_filter,
    size_t stage0_passed_rows,
    const IColumn::Filter * residual_filter_,
    size_t residual_passed_rows)
{
    ++direct_mode_blocks;
    auto final_rest_block = readWithOptionalFilter(final_rest_stream, stage0_filter, stage0_passed_rows, "final_rest");

    RUNTIME_CHECK_MSG(
        final_rest_block.startOffset() == stage1_block.startOffset(),
        "Multi-stage late materialization meets unexpected block unmatched in direct mode, stage1_block: "
        "[start_offset={}, rows={}], final_rest_block: [start_offset={}, rows={}]",
        stage1_block.startOffset(),
        stage1_block.rows(),
        final_rest_block.startOffset(),
        final_rest_block.rows());

    auto full_block = hstackBlocks({std::move(stage1_block), std::move(final_rest_block)}, header);
    if (residual_filter_ != nullptr && residual_passed_rows != full_block.rows())
        filterBlock(full_block, *residual_filter_, residual_passed_rows);
    return full_block;
}

Block MultiStageLateMaterializationBlockInputStream::read()
{
    while (true)
    {
        Block stage0_block;
        FilterPtr stage0_filter = nullptr;
        stage0_block = stage0_filter_stream->read(stage0_filter, true);
        if (!stage0_block)
        {
            logSummary();
            return {};
        }

        auto effective_stage0_filter = buildStage0EffectiveFilter(stage0_block, stage0_filter);
        if (effective_stage0_filter.passed_count == 0)
        {
            skipNextBlockOrRead(stage1_filter_stream, "stage1_filter");
            skipNextBlockOrRead(final_rest_stream, "final_rest");
            continue;
        }

        const auto * stage0_filter_ptr = effective_stage0_filter.getFilter();
        auto stage1_block = readWithOptionalFilter(
            stage1_filter_stream,
            stage0_filter_ptr,
            effective_stage0_filter.passed_count,
            "stage1_filter");

        RUNTIME_CHECK_MSG(
            stage1_block.startOffset() == stage0_block.startOffset(),
            "Multi-stage late materialization meets unexpected block unmatched after stage1 read, stage0_block: "
            "[start_offset={}, rows={}], stage1_block: [start_offset={}, rows={}], stage0_pass_count={}",
            stage0_block.startOffset(),
            stage0_block.rows(),
            stage1_block.startOffset(),
            stage1_block.rows(),
            effective_stage0_filter.passed_count);

        Block filter_eval_block;
        FilterPtr residual_filter_ptr = nullptr;
        const auto residual_passed_rows = executeResidualFilter(stage1_block, filter_eval_block, residual_filter_ptr);
        updateAdaptiveState(effective_stage0_filter.passed_count, residual_passed_rows);

        if (residual_passed_rows == 0)
        {
            skipNextBlockOrRead(final_rest_stream, "final_rest");
            continue;
        }

        if (residual_filter_ptr == nullptr)
        {
            return buildDirectModeBlock(
                stage1_block,
                stage0_filter_ptr,
                effective_stage0_filter.passed_count,
                nullptr,
                residual_passed_rows);
        }

        if (shouldUseLateMode(effective_stage0_filter.passed_count, residual_passed_rows))
        {
            return buildLateModeBlock(
                stage1_block,
                stage0_filter_ptr,
                stage0_block.rows(),
                *residual_filter_ptr,
                residual_passed_rows);
        }

        return buildDirectModeBlock(
            stage1_block,
            stage0_filter_ptr,
            effective_stage0_filter.passed_count,
            residual_filter_ptr,
            residual_passed_rows);
    }
}

void MultiStageLateMaterializationBlockInputStream::logSummary()
{
    if (summary_logged)
        return;
    summary_logged = true;

    const double residual_filtered_ratio = sample_stage0_rows == 0
        ? 0.0
        : 1.0 - static_cast<double>(sample_residual_passed_rows) / static_cast<double>(sample_stage0_rows);
    LOG_INFO(
        log,
        "Multi-stage late materialization finished, adaptive_mode={} sample_blocks={} sample_stage0_rows={} "
        "sample_residual_passed_rows={} residual_filtered_ratio={:.3f} late_mode_blocks={} direct_mode_blocks={}",
        modeToString(adaptive_mode),
        sample_blocks,
        sample_stage0_rows,
        sample_residual_passed_rows,
        residual_filtered_ratio,
        late_mode_blocks,
        direct_mode_blocks);
}

} // namespace DB::DM
