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

#include <common/logger_useful.h>
#include <common/types.h>

#include <atomic>
#include <memory>

namespace DB::DM
{

struct MultiStageLateMaterializationRuntimeStatsDelta
{
    void reset() { *this = {}; }

    bool empty() const
    {
        return pushed_filter_input_rows == 0 && residual_filter_input_rows == 0 && final_rest_input_rows == 0
            && running_topn_input_rows == 0;
    }

    void recordPushedFilter(UInt64 input_rows, UInt64 selected_rows)
    {
        pushed_filter_input_rows += input_rows;
        pushed_filter_selected_rows += selected_rows;
        pushed_filter_filtered_rows += input_rows - selected_rows;

        // Keep the old counter name for existing actRows/tests.
        stage0_output_rows += selected_rows;
    }

    void recordResidualFilter(UInt64 input_rows, UInt64 selected_rows)
    {
        residual_filter_input_rows += input_rows;
        residual_filter_selected_rows += selected_rows;
        residual_filter_filtered_rows += input_rows - selected_rows;

        // Keep the old counter name for existing actRows/tests.
        stage1_output_rows += selected_rows;
    }

    void recordFinalRestInputRows(UInt64 rows)
    {
        final_rest_input_rows += rows;

        // Keep the old counter name for existing actRows/tests.
        topn_candidate_rows += rows;
    }

    void recordRunningTopN(UInt64 input_rows, UInt64 selected_rows)
    {
        running_topn_input_rows += input_rows;
        running_topn_selected_rows += selected_rows;
        running_topn_filtered_rows += input_rows - selected_rows;
        recordFinalRestInputRows(selected_rows);
    }

    void recordRunningTopNBypass(UInt64 rows)
    {
        running_topn_input_rows += rows;
        running_topn_bypass_rows += rows;
        recordFinalRestInputRows(rows);
    }

    UInt64 pushed_filter_input_rows = 0;
    UInt64 pushed_filter_selected_rows = 0;
    UInt64 pushed_filter_filtered_rows = 0;
    UInt64 residual_filter_input_rows = 0;
    UInt64 residual_filter_selected_rows = 0;
    UInt64 residual_filter_filtered_rows = 0;
    UInt64 final_rest_input_rows = 0;
    UInt64 running_topn_input_rows = 0;
    UInt64 running_topn_selected_rows = 0;
    UInt64 running_topn_bypass_rows = 0;
    UInt64 running_topn_filtered_rows = 0;

    // Legacy names kept because actRows overrides and existing unit tests still refer to them.
    UInt64 stage0_output_rows = 0;
    UInt64 stage1_output_rows = 0;
    UInt64 topn_candidate_rows = 0;
};

struct MultiStageLateMaterializationRuntimeStats
{
    MultiStageLateMaterializationRuntimeStats() = default;

    explicit MultiStageLateMaterializationRuntimeStats(const String & log_id_)
        : log(Logger::get("MultiStageLateMaterialization", log_id_))
    {}

    ~MultiStageLateMaterializationRuntimeStats()
    {
        try
        {
            logSummary();
        }
        catch (...)
        {}
    }

    void finishStream(
        UInt64 stream_late_mode_blocks,
        UInt64 stream_direct_mode_blocks,
        UInt64 stream_topn_heap_size,
        bool stream_topn_adaptive_disabled,
        UInt64 stream_topn_adaptive_warmup_rows,
        UInt64 stream_topn_adaptive_input_rows,
        UInt64 stream_topn_adaptive_candidate_rows)
    {
        finished_streams.fetch_add(1, std::memory_order_relaxed);
        late_mode_blocks.fetch_add(stream_late_mode_blocks, std::memory_order_relaxed);
        direct_mode_blocks.fetch_add(stream_direct_mode_blocks, std::memory_order_relaxed);
        topn_heap_size_sum.fetch_add(stream_topn_heap_size, std::memory_order_relaxed);
        topn_adaptive_warmup_rows.fetch_add(stream_topn_adaptive_warmup_rows, std::memory_order_relaxed);
        topn_adaptive_post_warmup_input_rows.fetch_add(stream_topn_adaptive_input_rows, std::memory_order_relaxed);
        topn_adaptive_post_warmup_candidate_rows.fetch_add(
            stream_topn_adaptive_candidate_rows,
            std::memory_order_relaxed);
        if (stream_topn_adaptive_disabled)
            topn_adaptive_disabled_streams.fetch_add(1, std::memory_order_relaxed);
    }

    void recordPushedFilter(UInt64 input_rows, UInt64 selected_rows)
    {
        pushed_filter_input_rows.fetch_add(input_rows, std::memory_order_relaxed);
        pushed_filter_selected_rows.fetch_add(selected_rows, std::memory_order_relaxed);
        pushed_filter_filtered_rows.fetch_add(input_rows - selected_rows, std::memory_order_relaxed);

        // Keep the old counter name for existing actRows/tests.
        stage0_output_rows.fetch_add(selected_rows, std::memory_order_relaxed);
    }

    void recordResidualFilter(UInt64 input_rows, UInt64 selected_rows)
    {
        residual_filter_input_rows.fetch_add(input_rows, std::memory_order_relaxed);
        residual_filter_selected_rows.fetch_add(selected_rows, std::memory_order_relaxed);
        residual_filter_filtered_rows.fetch_add(input_rows - selected_rows, std::memory_order_relaxed);

        // Keep the old counter name for existing actRows/tests.
        stage1_output_rows.fetch_add(selected_rows, std::memory_order_relaxed);
    }

    void recordFinalRestInputRows(UInt64 rows)
    {
        final_rest_input_rows.fetch_add(rows, std::memory_order_relaxed);

        // Keep the old counter name for existing actRows/tests.
        topn_candidate_rows.fetch_add(rows, std::memory_order_relaxed);
    }

    void recordRunningTopNEnabled() { topn_enabled.store(true, std::memory_order_relaxed); }

    void recordRunningTopN(UInt64 input_rows, UInt64 selected_rows)
    {
        recordRunningTopNEnabled();
        running_topn_input_rows.fetch_add(input_rows, std::memory_order_relaxed);
        running_topn_selected_rows.fetch_add(selected_rows, std::memory_order_relaxed);
        running_topn_filtered_rows.fetch_add(input_rows - selected_rows, std::memory_order_relaxed);
        recordFinalRestInputRows(selected_rows);
    }

    void recordRunningTopNBypass(UInt64 rows)
    {
        recordRunningTopNEnabled();
        running_topn_input_rows.fetch_add(rows, std::memory_order_relaxed);
        running_topn_bypass_rows.fetch_add(rows, std::memory_order_relaxed);
        recordFinalRestInputRows(rows);
    }

    void merge(const MultiStageLateMaterializationRuntimeStatsDelta & delta)
    {
        if (delta.empty())
            return;

        pushed_filter_input_rows.fetch_add(delta.pushed_filter_input_rows, std::memory_order_relaxed);
        pushed_filter_selected_rows.fetch_add(delta.pushed_filter_selected_rows, std::memory_order_relaxed);
        pushed_filter_filtered_rows.fetch_add(delta.pushed_filter_filtered_rows, std::memory_order_relaxed);
        residual_filter_input_rows.fetch_add(delta.residual_filter_input_rows, std::memory_order_relaxed);
        residual_filter_selected_rows.fetch_add(delta.residual_filter_selected_rows, std::memory_order_relaxed);
        residual_filter_filtered_rows.fetch_add(delta.residual_filter_filtered_rows, std::memory_order_relaxed);
        final_rest_input_rows.fetch_add(delta.final_rest_input_rows, std::memory_order_relaxed);
        running_topn_input_rows.fetch_add(delta.running_topn_input_rows, std::memory_order_relaxed);
        running_topn_selected_rows.fetch_add(delta.running_topn_selected_rows, std::memory_order_relaxed);
        running_topn_bypass_rows.fetch_add(delta.running_topn_bypass_rows, std::memory_order_relaxed);
        running_topn_filtered_rows.fetch_add(delta.running_topn_filtered_rows, std::memory_order_relaxed);

        stage0_output_rows.fetch_add(delta.stage0_output_rows, std::memory_order_relaxed);
        stage1_output_rows.fetch_add(delta.stage1_output_rows, std::memory_order_relaxed);
        topn_candidate_rows.fetch_add(delta.topn_candidate_rows, std::memory_order_relaxed);
    }

    void merge(const MultiStageLateMaterializationRuntimeStats & other)
    {
        pushed_filter_input_rows.fetch_add(other.pushed_filter_input_rows.load(std::memory_order_relaxed));
        pushed_filter_selected_rows.fetch_add(other.pushed_filter_selected_rows.load(std::memory_order_relaxed));
        pushed_filter_filtered_rows.fetch_add(other.pushed_filter_filtered_rows.load(std::memory_order_relaxed));
        residual_filter_input_rows.fetch_add(other.residual_filter_input_rows.load(std::memory_order_relaxed));
        residual_filter_selected_rows.fetch_add(other.residual_filter_selected_rows.load(std::memory_order_relaxed));
        residual_filter_filtered_rows.fetch_add(other.residual_filter_filtered_rows.load(std::memory_order_relaxed));
        final_rest_input_rows.fetch_add(other.final_rest_input_rows.load(std::memory_order_relaxed));
        running_topn_input_rows.fetch_add(other.running_topn_input_rows.load(std::memory_order_relaxed));
        running_topn_selected_rows.fetch_add(other.running_topn_selected_rows.load(std::memory_order_relaxed));
        running_topn_bypass_rows.fetch_add(other.running_topn_bypass_rows.load(std::memory_order_relaxed));
        running_topn_filtered_rows.fetch_add(other.running_topn_filtered_rows.load(std::memory_order_relaxed));

        stage0_output_rows.fetch_add(other.stage0_output_rows.load(std::memory_order_relaxed));
        stage1_output_rows.fetch_add(other.stage1_output_rows.load(std::memory_order_relaxed));
        topn_candidate_rows.fetch_add(other.topn_candidate_rows.load(std::memory_order_relaxed));
        finished_streams.fetch_add(other.finished_streams.load(std::memory_order_relaxed));
        late_mode_blocks.fetch_add(other.late_mode_blocks.load(std::memory_order_relaxed));
        direct_mode_blocks.fetch_add(other.direct_mode_blocks.load(std::memory_order_relaxed));
        topn_heap_size_sum.fetch_add(other.topn_heap_size_sum.load(std::memory_order_relaxed));
        topn_adaptive_warmup_rows.fetch_add(other.topn_adaptive_warmup_rows.load(std::memory_order_relaxed));
        topn_adaptive_post_warmup_input_rows.fetch_add(
            other.topn_adaptive_post_warmup_input_rows.load(std::memory_order_relaxed));
        topn_adaptive_post_warmup_candidate_rows.fetch_add(
            other.topn_adaptive_post_warmup_candidate_rows.load(std::memory_order_relaxed));
        topn_adaptive_disabled_streams.fetch_add(other.topn_adaptive_disabled_streams.load(std::memory_order_relaxed));
        topn_enabled.store(
            topn_enabled.load(std::memory_order_relaxed) || other.topn_enabled.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
    }

    void logSummary() const
    {
        if (log == nullptr)
            return;

        LOG_INFO(
            log,
            "Multi-stage late materialization finished, streams={} late_mode_blocks={} direct_mode_blocks={} "
            "pushed_filter_input_rows={} pushed_filter_selected_rows={} pushed_filter_filtered_rows={} "
            "residual_filter_input_rows={} residual_filter_selected_rows={} residual_filter_filtered_rows={} "
            "final_rest_input_rows={} topn_enabled={} running_topn_input_rows={} running_topn_selected_rows={} "
            "running_topn_bypass_rows={} running_topn_filtered_rows={} running_topn_heap_size_sum={} "
            "topn_adaptive_warmup_rows={} topn_adaptive_post_warmup_input_rows={} "
            "topn_adaptive_post_warmup_candidate_rows={} topn_adaptive_disabled_streams={}",
            finished_streams.load(std::memory_order_relaxed),
            late_mode_blocks.load(std::memory_order_relaxed),
            direct_mode_blocks.load(std::memory_order_relaxed),
            pushed_filter_input_rows.load(std::memory_order_relaxed),
            pushed_filter_selected_rows.load(std::memory_order_relaxed),
            pushed_filter_filtered_rows.load(std::memory_order_relaxed),
            residual_filter_input_rows.load(std::memory_order_relaxed),
            residual_filter_selected_rows.load(std::memory_order_relaxed),
            residual_filter_filtered_rows.load(std::memory_order_relaxed),
            final_rest_input_rows.load(std::memory_order_relaxed),
            topn_enabled.load(std::memory_order_relaxed),
            running_topn_input_rows.load(std::memory_order_relaxed),
            running_topn_selected_rows.load(std::memory_order_relaxed),
            running_topn_bypass_rows.load(std::memory_order_relaxed),
            running_topn_filtered_rows.load(std::memory_order_relaxed),
            topn_heap_size_sum.load(std::memory_order_relaxed),
            topn_adaptive_warmup_rows.load(std::memory_order_relaxed),
            topn_adaptive_post_warmup_input_rows.load(std::memory_order_relaxed),
            topn_adaptive_post_warmup_candidate_rows.load(std::memory_order_relaxed),
            topn_adaptive_disabled_streams.load(std::memory_order_relaxed));
    }

    std::atomic<UInt64> pushed_filter_input_rows{0};
    std::atomic<UInt64> pushed_filter_selected_rows{0};
    std::atomic<UInt64> pushed_filter_filtered_rows{0};
    std::atomic<UInt64> residual_filter_input_rows{0};
    std::atomic<UInt64> residual_filter_selected_rows{0};
    std::atomic<UInt64> residual_filter_filtered_rows{0};
    std::atomic<UInt64> final_rest_input_rows{0};
    std::atomic<UInt64> running_topn_input_rows{0};
    std::atomic<UInt64> running_topn_selected_rows{0};
    std::atomic<UInt64> running_topn_bypass_rows{0};
    std::atomic<UInt64> running_topn_filtered_rows{0};

    // Legacy names kept because actRows overrides and existing unit tests still refer to them.
    std::atomic<UInt64> stage0_output_rows{0};
    std::atomic<UInt64> stage1_output_rows{0};
    std::atomic<UInt64> topn_candidate_rows{0};
    std::atomic<UInt64> finished_streams{0};
    std::atomic<UInt64> late_mode_blocks{0};
    std::atomic<UInt64> direct_mode_blocks{0};
    std::atomic<UInt64> topn_heap_size_sum{0};
    std::atomic<UInt64> topn_adaptive_warmup_rows{0};
    std::atomic<UInt64> topn_adaptive_post_warmup_input_rows{0};
    std::atomic<UInt64> topn_adaptive_post_warmup_candidate_rows{0};
    std::atomic<UInt64> topn_adaptive_disabled_streams{0};
    std::atomic<bool> topn_enabled{false};
    LoggerPtr log;
};

using MultiStageLateMaterializationRuntimeStatsPtr = std::shared_ptr<MultiStageLateMaterializationRuntimeStats>;

} // namespace DB::DM
