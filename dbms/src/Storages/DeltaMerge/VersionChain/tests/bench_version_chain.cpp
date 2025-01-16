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


#include <Common/CurrentMetrics.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/VersionChain/tests/mvcc_test_utils.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <benchmark/benchmark.h>

using namespace DB;
using namespace DB::tests;
using namespace DB::DM;
using namespace DB::DM::tests;
using namespace DB::DM::tests::MVCC;

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace
{
template <typename... Args>
void MVCCFullPlace(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 delta_rows = state.range(0);
    auto [context, dm_context, cols, segment, segment_snapshot, random_sequences]
        = initialize(is_common_handle, delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    if (type == BenchType::DeltaIndex)
    {
        {
            RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
            auto delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment); // Warming up
            RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
        }
        for (auto _ : state)
        {
            RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
            auto delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
            RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
        }
    }
    else if (type == BenchType::VersionChain)
    {
        {
            VersionChain<Int64> version_chain;
            buildVersionChain(*dm_context, *segment_snapshot, version_chain); // Warming up
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
        for (auto _ : state)
        {
            VersionChain<Int64> version_chain;
            buildVersionChain(*dm_context, *segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
    }
}
CATCH

template <typename... Args>
void MVCCIncrementalPlace(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 incremental_delta_rows = state.range(0);
    constexpr UInt32 prepared_delta_rows = 10000;
    auto [context, dm_context, cols, segment, segment_snapshot, random_sequences]
        = initialize(is_common_handle, prepared_delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto base_delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
        RUNTIME_ASSERT(base_delta_index->getPlacedStatus().first == prepared_delta_rows);

        segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*base_delta_index);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);

        writeDelta(*dm_context, is_common_handle, *segment, incremental_delta_rows, random_sequences);
        segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);
        RUNTIME_ASSERT(segment_snapshot->delta->getRows() == prepared_delta_rows + incremental_delta_rows);

        for (auto _ : state)
        {
            RUNTIME_ASSERT(
                segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);
            auto delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
            RUNTIME_ASSERT(delta_index->getPlacedStatus().first == prepared_delta_rows + incremental_delta_rows);
        }
    }
    else if (type == BenchType::VersionChain)
    {
        VersionChain<Int64> base_version_chain;
        buildVersionChain(*dm_context, *segment_snapshot, base_version_chain);
        RUNTIME_ASSERT(base_version_chain.getReplayedRows() == prepared_delta_rows);
        writeDelta(*dm_context, is_common_handle, *segment, incremental_delta_rows, random_sequences);
        segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
        RUNTIME_ASSERT(segment_snapshot->delta->getRows() == prepared_delta_rows + incremental_delta_rows);
        for (auto _ : state)
        {
            auto version_chain = base_version_chain;
            RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows);
            buildVersionChain(*dm_context, *segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows + incremental_delta_rows);
        }
    }
}
CATCH

template <typename... Args>
void MVCCBuildBitmap(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 delta_rows = state.range(0);
    auto [context, dm_context, cols, segment, segment_snapshot, random_sequences]
        = initialize(is_common_handle, delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    auto rs_results = loadPackFilterResults(*dm_context, segment_snapshot, {segment->getRowKeyRange()});

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
        RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
        segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*delta_index);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == delta_rows);

        for (auto _ : state)
        {
            auto bitmap_filter = segment->buildBitmapFilter(
                *dm_context,
                segment_snapshot,
                {segment->getRowKeyRange()},
                rs_results,
                std::numeric_limits<UInt64>::max(),
                DEFAULT_BLOCK_SIZE,
                false);
            benchmark::DoNotOptimize(bitmap_filter);
        }
    }
    else if (type == BenchType::VersionChain)
    {
        VersionChain<Int64> version_chain;
        buildVersionChain(*dm_context, *segment_snapshot, version_chain);
        RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        for (auto _ : state)
        {
            auto bitmap_filter = buildBitmapFilter<Int64>(
                *dm_context,
                *segment_snapshot,
                {segment->getRowKeyRange()},
                rs_results,
                std::numeric_limits<UInt64>::max(),
                version_chain);
            benchmark::DoNotOptimize(bitmap_filter);
        }
    }
}
CATCH

// [ 1, 8, 64, 512, 4k, 8k, 64k ]
BENCHMARK_CAPTURE(MVCCFullPlace, Index, BenchType::DeltaIndex, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);
BENCHMARK_CAPTURE(MVCCFullPlace, Chain, BenchType::VersionChain, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);

BENCHMARK_CAPTURE(MVCCIncrementalPlace, Index, BenchType::DeltaIndex, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);
BENCHMARK_CAPTURE(MVCCIncrementalPlace, Chain, BenchType::VersionChain, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);

BENCHMARK_CAPTURE(MVCCBuildBitmap, Index, BenchType::DeltaIndex, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);
BENCHMARK_CAPTURE(MVCCBuildBitmap, Chain, BenchType::VersionChain, WriteLoad::RandomUpdate, IsNotCommonHandle)
    ->Range(1, 8 << 13);
} // namespace
