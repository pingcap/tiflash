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
void MVCCFullBuildIndex(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle, delta_rows] = std::make_tuple(std::move(args)...);
    auto [context, dm_context, cols, segment, segment_snapshot] = initialize(is_common_handle, delta_rows);
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
            buildVersionChain<Int64>(*dm_context, *segment_snapshot, version_chain); // Warming up
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
        for (auto _ : state)
        {
            VersionChain<Int64> version_chain;
            buildVersionChain<Int64>(*dm_context, *segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
    }
}
CATCH

/*

template <typename... Args>
void MVCCIncrementalBuildIndex(benchmark::State & state, Args &&... args)
try
{
    const auto [type, is_common_handle, incremental_delta_rows] = std::make_tuple(std::move(args)...);
    constexpr UInt32 prepared_delta_rows = 10000;
    initialize(type, is_common_handle, prepared_delta_rows);

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto base_delta_index = buildDeltaIndex(segment_snapshot, *segment);
        RUNTIME_ASSERT(base_delta_index->getPlacedStatus().first == prepared_delta_rows);

        segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*base_delta_index);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);

        writeDelta(*segment, incremental_delta_rows);
        segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);
        RUNTIME_ASSERT(segment_snapshot->delta->getRows() == prepared_delta_rows + incremental_delta_rows);

        for (auto _ : state)
        {
            RUNTIME_ASSERT(
                segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);
            auto delta_index = buildDeltaIndex(segment_snapshot, *segment);
            RUNTIME_ASSERT(delta_index->getPlacedStatus().first == prepared_delta_rows + incremental_delta_rows);
        }
    }
    else if (type == BenchType::VersionChain)
    {
        VersionChain<Int64> base_version_chain;
        buildVersionChain(*segment_snapshot, base_version_chain);
        RUNTIME_ASSERT(base_version_chain.getReplayedRows() == prepared_delta_rows);
        writeDelta(*segment, incremental_delta_rows);
        segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
        RUNTIME_ASSERT(segment_snapshot->delta->getRows() == prepared_delta_rows + incremental_delta_rows);
        for (auto _ : state)
        {
            auto version_chain = base_version_chain;
            RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows);
            buildVersionChain(*segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows + incremental_delta_rows);
        }
    }

    shutdown();
}
CATCH

template <typename... Args>
void MVCCBuildBitmap(benchmark::State & state, Args &&... args)
try
{
    const auto [type, is_common_handle, delta_rows] = std::make_tuple(std::move(args)...);
    initialize(type, is_common_handle, delta_rows);
    auto rs_results = loadPackFilterResults(segment_snapshot, {segment->getRowKeyRange()});

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto delta_index = buildDeltaIndex(segment_snapshot, *segment);
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
        buildVersionChain(*segment_snapshot, version_chain);
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
    shutdown();
}
CATCH
*/
// TODO: move verify to unit-tests.
template <typename... Args>
void MVCCBuildBitmapVerify(benchmark::State & state, Args &&... args)
try
{
    const auto [type, is_common_handle, delta_rows] = std::make_tuple(std::move(args)...);
    auto [context, dm_context, cols, segment, segment_snapshot] = initialize(is_common_handle, delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
    auto delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
    RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
    segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*delta_index);
    RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == delta_rows);

    VersionChain<Int64> version_chain;
    buildVersionChain<Int64>(*dm_context, *segment_snapshot, version_chain);
    RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);

    auto rs_results = loadPackFilterResults(*dm_context, segment_snapshot, {segment->getRowKeyRange()});
    auto bitmap_filter1 = segment->buildBitmapFilter(
        *dm_context,
        segment_snapshot,
        {segment->getRowKeyRange()},
        rs_results,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        false);
    auto bitmap_filter2 = buildBitmapFilter<Int64>(
        *dm_context,
        *segment_snapshot,
        {segment->getRowKeyRange()},
        rs_results,
        std::numeric_limits<UInt64>::max(),
        version_chain);

    const auto & filter1 = bitmap_filter1->getFilter();
    const auto & filter2 = bitmap_filter2->getFilter();
    RUNTIME_ASSERT(filter1.size() == filter2.size());
    for (UInt32 i = 0; i < filter1.size(); ++i)
    {
        if (filter1[i] != filter2[i])
        {
            fmt::println("i={}, filter1={}, filter2={}", i, filter1[i], filter2[i]);
            std::abort();
        }
    }

    for (auto _ : state) {}
}
CATCH

BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_1, BenchType::None, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_5, BenchType::None, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_10, BenchType::None, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_100, BenchType::None, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_500, BenchType::None, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_1k, BenchType::None, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_5k, BenchType::None, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_10k, BenchType::None, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_50k, BenchType::None, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_100k, BenchType::None, IsNotCommonHandle, 100000u);

/*
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_1,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    1u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_5,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    5u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_10,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    10u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_100,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    100u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_500,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    500u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_1k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    1000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_5k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    5000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_10k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    10000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_50k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    50000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_100k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    100000u);

BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_1,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    1u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_5,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    5u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_10,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    10u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_100,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    100u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_500,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    500u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_1k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    1000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_5k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    5000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_10k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    10000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_50k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    50000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_100k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsNotCommonHandle,
    100000u);
*/
/*
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_1,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    1u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_5,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    5u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_10,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    10u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_100,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    100u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_500,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    500u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_1k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    1000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_5k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    5000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_10k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    10000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_50k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    50000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    delta_idx_update_common_handle_100k,
    BenchType::DeltaIndex,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    100000u);

BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_1,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    1u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_5,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    5u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_10,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    10u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_100,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    100u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_500,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    500u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_1k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    1000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_5k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    5000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_10k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    10000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_50k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    50000u);
BENCHMARK_CAPTURE(
    MVCCFullBuildIndex,
    ver_chain_update_common_handle_100k,
    BenchType::VersionChain,
    WriteLoad::RandomUpdate,
    IsCommonHandle,
    100000u);*/
/*
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_1, BenchType::DeltaIndex, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_5, BenchType::DeltaIndex, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_10, BenchType::DeltaIndex, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_100, BenchType::DeltaIndex, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_500, BenchType::DeltaIndex, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, delta_index_1000, BenchType::DeltaIndex, IsNotCommonHandle, 1000u);

BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_1, BenchType::VersionChain, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_5, BenchType::VersionChain, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_10, BenchType::VersionChain, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_100, BenchType::VersionChain, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_500, BenchType::VersionChain, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCIncrementalBuildIndex, version_chain_1000, BenchType::VersionChain, IsNotCommonHandle, 1000u);

BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_1, BenchType::DeltaIndex, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_5, BenchType::DeltaIndex, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_10, BenchType::DeltaIndex, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_100, BenchType::DeltaIndex, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_500, BenchType::DeltaIndex, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_1000, BenchType::DeltaIndex, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_5000, BenchType::DeltaIndex, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_10000, BenchType::DeltaIndex, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_50000, BenchType::DeltaIndex, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, delta_index_100000, BenchType::DeltaIndex, IsNotCommonHandle, 100000u);

BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_1, BenchType::VersionChain, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_5, BenchType::VersionChain, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_10, BenchType::VersionChain, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_100, BenchType::VersionChain, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_500, BenchType::VersionChain, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_1000, BenchType::VersionChain, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_5000, BenchType::VersionChain, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_10000, BenchType::VersionChain, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_50000, BenchType::VersionChain, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCBuildBitmap, version_chain_100000, BenchType::VersionChain, IsNotCommonHandle, 100000u);

BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_1, BenchType::None, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_5, BenchType::None, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_10, BenchType::None, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_100, BenchType::None, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_500, BenchType::None, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_1000, BenchType::None, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_5000, BenchType::None, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_10000, BenchType::None, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_50000, BenchType::None, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCBuildBitmapVerify, verify_100000, BenchType::None, IsNotCommonHandle, 100000u);
*/
} // namespace
