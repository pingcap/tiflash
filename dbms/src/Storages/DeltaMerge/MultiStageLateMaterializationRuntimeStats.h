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

struct MultiStageLateMaterializationRuntimeStats
{
    MultiStageLateMaterializationRuntimeStats() = default;

    MultiStageLateMaterializationRuntimeStats(const String & log_id_, bool topn_enabled_)
        : topn_enabled(topn_enabled_)
        , log(Logger::get("MultiStageLateMaterialization", log_id_))
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
        bool stream_topn_adaptive_disabled)
    {
        finished_streams.fetch_add(1, std::memory_order_relaxed);
        late_mode_blocks.fetch_add(stream_late_mode_blocks, std::memory_order_relaxed);
        direct_mode_blocks.fetch_add(stream_direct_mode_blocks, std::memory_order_relaxed);
        topn_heap_size_sum.fetch_add(stream_topn_heap_size, std::memory_order_relaxed);
        if (stream_topn_adaptive_disabled)
            topn_adaptive_disabled_streams.fetch_add(1, std::memory_order_relaxed);
    }

    void logSummary() const
    {
        if (log == nullptr)
            return;

        const auto stage1_rows = stage1_output_rows.load(std::memory_order_relaxed);
        const auto topn_candidate_rows_for_log = topn_enabled ? topn_candidate_rows.load(std::memory_order_relaxed) : 0;
        const auto topn_filtered_rows = topn_enabled && stage1_rows >= topn_candidate_rows_for_log
            ? stage1_rows - topn_candidate_rows_for_log
            : 0;

        LOG_INFO(
            log,
            "Multi-stage late materialization finished, streams={} late_mode_blocks={} direct_mode_blocks={} "
            "stage0_output_rows={} stage1_output_rows={} topn_enabled={} topn_candidate_rows={} "
            "topn_filtered_rows={} topn_heap_size_sum={} topn_adaptive_disabled_streams={}",
            finished_streams.load(std::memory_order_relaxed),
            late_mode_blocks.load(std::memory_order_relaxed),
            direct_mode_blocks.load(std::memory_order_relaxed),
            stage0_output_rows.load(std::memory_order_relaxed),
            stage1_rows,
            topn_enabled,
            topn_candidate_rows_for_log,
            topn_filtered_rows,
            topn_heap_size_sum.load(std::memory_order_relaxed),
            topn_adaptive_disabled_streams.load(std::memory_order_relaxed));
    }

    std::atomic<UInt64> stage0_output_rows{0};
    std::atomic<UInt64> stage1_output_rows{0};
    std::atomic<UInt64> topn_candidate_rows{0};
    std::atomic<UInt64> finished_streams{0};
    std::atomic<UInt64> late_mode_blocks{0};
    std::atomic<UInt64> direct_mode_blocks{0};
    std::atomic<UInt64> topn_heap_size_sum{0};
    std::atomic<UInt64> topn_adaptive_disabled_streams{0};
    bool topn_enabled = false;
    LoggerPtr log;
};

using MultiStageLateMaterializationRuntimeStatsPtr = std::shared_ptr<MultiStageLateMaterializationRuntimeStats>;

} // namespace DB::DM
