// Copyright 2025 PingCAP, Inc.
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
#include <Debug/TiFlashTestEnv.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/VersionChain/MVCCBitmapFilter.h>
#include <Storages/DeltaMerge/VersionChain/tests/mvcc_test_utils.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
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
void MVCCFullBuild(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 delta_rows = state.range(0);
    auto [context, dm_context, cols, segment, segment_snapshot, write_seq]
        = initialize(write_load, is_common_handle, delta_rows);
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
        auto bench_impl = [&](auto handle_type) {
            {
                VersionChain<decltype(handle_type)> version_chain;
                buildVersionChain(*dm_context, *segment_snapshot, version_chain); // Warming up
                RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
            }
            for (auto _ : state)
            {
                VersionChain<decltype(handle_type)> version_chain;
                buildVersionChain(*dm_context, *segment_snapshot, version_chain);
                RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
            }
        };
        if (is_common_handle)
            bench_impl(String{});
        else
            bench_impl(Int64{});
    }
}
CATCH

template <typename... Args>
void MVCCIncrementalBuild(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 prepared_delta_rows = state.range(0);
    const UInt32 incremental_delta_rows = state.range(1);
    auto [context, dm_context, cols, segment, segment_snapshot, write_seq]
        = initialize(write_load, is_common_handle, prepared_delta_rows);
    SCOPE_EXIT({ context->shutdown(); });

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto base_delta_index = buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
        RUNTIME_ASSERT(base_delta_index->getPlacedStatus().first == prepared_delta_rows);

        segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*base_delta_index);
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == prepared_delta_rows);

        writeDelta(*dm_context, is_common_handle, *segment, incremental_delta_rows, *write_seq);
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
        auto bench_impl = [&](auto handle_type) {
            VersionChain<decltype(handle_type)> base_version_chain;
            buildVersionChain(*dm_context, *segment_snapshot, base_version_chain);
            RUNTIME_ASSERT(base_version_chain.getReplayedRows() == prepared_delta_rows);
            writeDelta(*dm_context, is_common_handle, *segment, incremental_delta_rows, *write_seq);
            segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
            RUNTIME_ASSERT(segment_snapshot->delta->getRows() == prepared_delta_rows + incremental_delta_rows);
            for (auto _ : state)
            {
                auto version_chain = base_version_chain.deepCopy();
                RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows);
                buildVersionChain(*dm_context, *segment_snapshot, version_chain);
                RUNTIME_ASSERT(version_chain.getReplayedRows() == prepared_delta_rows + incremental_delta_rows);
            }
        };
        if (is_common_handle)
            bench_impl(String{});
        else
            bench_impl(Int64{});
    }
}
CATCH

template <typename... Args>
void MVCCBuildBitmap(benchmark::State & state, Args &&... args)
try
{
    const auto [type, write_load, is_common_handle] = std::make_tuple(std::move(args)...);
    const UInt32 delta_rows = state.range(0);
    auto [context, dm_context, cols, segment, segment_snapshot, write_seq]
        = initialize(write_load, is_common_handle, delta_rows);
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
            auto bitmap_filter = segment->buildMVCCBitmapFilter(
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
        auto bench_impl = [&](auto handle_type) {
            VersionChain<decltype(handle_type)> version_chain;
            buildVersionChain(*dm_context, *segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
            for (auto _ : state)
            {
                auto bitmap_filter = buildMVCCBitmapFilter<decltype(handle_type)>(
                    *dm_context,
                    *segment_snapshot,
                    {segment->getRowKeyRange()},
                    rs_results,
                    std::numeric_limits<UInt64>::max(),
                    version_chain);
                benchmark::DoNotOptimize(bitmap_filter);
            }
        };
        if (is_common_handle)
            bench_impl(String{});
        else
            bench_impl(Int64{});
    }
}
CATCH

// [ 1, 8, 64, 512, 4k, 8k, 64k ]
#define MVCC_BENCHMARK(FUNC)                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Index / RandomUpdate, BenchType::DeltaIndex, WriteLoad::RandomUpdate, NotCommonHandle)   \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Chain / RandomUpdate, BenchType::VersionChain, WriteLoad::RandomUpdate, NotCommonHandle) \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Index / AppendOnly, BenchType::DeltaIndex, WriteLoad::AppendOnly, NotCommonHandle)       \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Chain / AppendOnly, BenchType::VersionChain, WriteLoad::AppendOnly, NotCommonHandle)     \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Index / RandomInsert, BenchType::DeltaIndex, WriteLoad::RandomInsert, NotCommonHandle)   \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(FUNC, Chain / RandomInsert, BenchType::VersionChain, WriteLoad::RandomInsert, NotCommonHandle) \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / RandomUpdate,                                                                         \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::RandomUpdate,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / RandomUpdate,                                                                         \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::RandomUpdate,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / AppendOnly,                                                                           \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::AppendOnly,                                                                                       \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / AppendOnly,                                                                           \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::AppendOnly,                                                                                       \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / RandomInsert,                                                                         \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::RandomInsert,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);                                                                                         \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / RandomInsert,                                                                         \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::RandomInsert,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->Range(1, 8 << 13);

// {10000, 20000, 30000, 40000, 50000} * {1, 8, 64, 512, 4k, 8k, 64k}
#define MVCC_BENCHMARK2(FUNC)                                                                                        \
    BENCHMARK_CAPTURE(FUNC, Index / RandomUpdate, BenchType::DeltaIndex, WriteLoad::RandomUpdate, NotCommonHandle)   \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(FUNC, Chain / RandomUpdate, BenchType::VersionChain, WriteLoad::RandomUpdate, NotCommonHandle) \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(FUNC, Index / AppendOnly, BenchType::DeltaIndex, WriteLoad::AppendOnly, NotCommonHandle)       \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(FUNC, Chain / AppendOnly, BenchType::VersionChain, WriteLoad::AppendOnly, NotCommonHandle)     \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(FUNC, Index / RandomInsert, BenchType::DeltaIndex, WriteLoad::RandomInsert, NotCommonHandle)   \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(FUNC, Chain / RandomInsert, BenchType::VersionChain, WriteLoad::RandomInsert, NotCommonHandle) \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / RandomUpdate,                                                                         \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::RandomUpdate,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / RandomUpdate,                                                                         \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::RandomUpdate,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / AppendOnly,                                                                           \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::AppendOnly,                                                                                       \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / AppendOnly,                                                                           \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::AppendOnly,                                                                                       \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Index / RandomInsert,                                                                         \
        BenchType::DeltaIndex,                                                                                       \
        WriteLoad::RandomInsert,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});                                                      \
    BENCHMARK_CAPTURE(                                                                                               \
        FUNC,                                                                                                        \
        CommonHandle / Chain / RandomInsert,                                                                         \
        BenchType::VersionChain,                                                                                     \
        WriteLoad::RandomInsert,                                                                                     \
        IsCommonHandle)                                                                                              \
        ->ArgsProduct(                                                                                               \
            {benchmark::CreateDenseRange(10000, 50000, /*step*/ 10000),                                              \
             benchmark::CreateRange(1, 8 << 13, /*multi=*/8)});

MVCC_BENCHMARK(MVCCFullBuild)
MVCC_BENCHMARK2(MVCCIncrementalBuild)
MVCC_BENCHMARK(MVCCBuildBitmap)
} // namespace
