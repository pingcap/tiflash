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

#pragma once

#include <DataStreams/FilterTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationRuntimeStats.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

class MultiStageLateMaterializationBlockInputStream : public IBlockInputStream
{
    static constexpr auto NAME = "MultiStageLateMaterializationBlockInputStream";

public:
    enum class AdaptiveMode
    {
        Sampling,
        Late,
        Direct,
    };

    MultiStageLateMaterializationBlockInputStream(
        const ColumnDefines & columns_to_read,
        BlockInputStreamPtr stage0_filter_stream_,
        SkippableBlockInputStreamPtr stage1_filter_stream_,
        SkippableBlockInputStreamPtr final_rest_stream_,
        const PushDownFilterPtr & residual_filter_,
        const BitmapFilterPtr & bitmap_filter_,
        const String & req_id_,
        const MultiStageLateMaterializationRuntimeStatsPtr & runtime_stats_ = nullptr);

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block read() override;

    static IColumn::Filter composeFilters(
        const IColumn::Filter * stage0_filter,
        size_t stage0_rows,
        const IColumn::Filter & residual_filter);

private:
    struct EffectiveFilter
    {
        const IColumn::Filter * filter = nullptr;
        IColumn::Filter holder;
        size_t passed_count = 0;
        bool use_holder = false;

        const IColumn::Filter * getFilter() const { return use_holder ? &holder : filter; }
    };

    static Block buildResidualFilterHeader(const PushDownFilterPtr & residual_filter);

    EffectiveFilter buildStage0EffectiveFilter(Block & stage0_block, FilterPtr stage0_filter);

    Block readWithOptionalFilter(
        SkippableBlockInputStreamPtr & stream,
        const IColumn::Filter * filter,
        size_t passed_count,
        const char * stream_name);

    void skipNextBlockOrRead(SkippableBlockInputStreamPtr & stream, const char * stream_name);

    size_t executeResidualFilter(Block & stage1_block, Block & filter_eval_block, FilterPtr & residual_filter);

    void updateAdaptiveState(size_t stage0_passed_rows, size_t residual_passed_rows);

    bool shouldUseLateMode(size_t stage0_passed_rows, size_t residual_passed_rows) const;

    Block buildLateModeBlock(
        Block & stage1_block,
        const IColumn::Filter * stage0_filter,
        size_t stage0_rows,
        const IColumn::Filter & residual_filter,
        size_t residual_passed_rows);

    Block buildDirectModeBlock(
        Block & stage1_block,
        const IColumn::Filter * stage0_filter,
        size_t stage0_passed_rows,
        const IColumn::Filter * residual_filter,
        size_t residual_passed_rows);

    void logSummary();

private:
    Block header;
    BlockInputStreamPtr stage0_filter_stream;
    SkippableBlockInputStreamPtr stage1_filter_stream;
    SkippableBlockInputStreamPtr final_rest_stream;
    PushDownFilterPtr residual_filter;
    BitmapFilterPtr bitmap_filter;
    MultiStageLateMaterializationRuntimeStatsPtr runtime_stats;
    FilterTransformAction residual_filter_action;

    AdaptiveMode adaptive_mode = AdaptiveMode::Sampling;
    size_t sample_blocks = 0;
    size_t sample_stage0_rows = 0;
    size_t sample_residual_passed_rows = 0;
    size_t late_mode_blocks = 0;
    size_t direct_mode_blocks = 0;
    bool summary_logged = false;

    const LoggerPtr log;
};

} // namespace DB::DM
