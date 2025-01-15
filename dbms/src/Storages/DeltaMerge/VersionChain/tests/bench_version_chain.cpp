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
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <benchmark/benchmark.h>

#include <magic_enum.hpp>
#include <random>

using namespace DB;
using namespace DB::tests;
using namespace DB::DM;
using namespace DB::DM::tests;

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace
{
const String db_name = "test";

UInt64 version = 1;

enum class BenchType
{
    None = 0,
    DeltaIndex = 1,
    VersionChain = 2,
};

enum class WriteLoad
{
    RandomUpdate = 1,
    AppendOnly = 2,
    RandomInsert = 3,
};

// constexpr bool IsCommonHandle = true;
constexpr bool IsNotCommonHandle = false;


auto loadPackFilterResults(
    const DMContext & dm_context,
    const SegmentSnapshotPtr & snap,
    const RowKeyRanges & ranges)
{
    DMFilePackFilterResults results;
    results.reserve(snap->stable->getDMFiles().size());
    for (const auto & file : snap->stable->getDMFiles())
    {
        auto pack_filter = DMFilePackFilter::loadFrom(dm_context, file, true, ranges, EMPTY_RS_OPERATOR, {});
        results.push_back(pack_filter);
    }
    return results;
}

auto getContext()
{
    const auto table_name = std::to_string(clock_gettime_ns());
    const auto testdata_path = fmt::format("/tmp/{}", table_name);
    constexpr auto run_mode = DB::PageStorageRunMode::ONLY_V3;
    TiFlashTestEnv::initializeGlobalContext({testdata_path}, run_mode);
    return std::pair{TiFlashTestEnv::getContext(), std::move(table_name)};
}

auto getDMContext(Context & context, const String & table_name, bool is_common_handle)
{
    auto storage_path_pool
        = std::make_shared<StoragePathPool>(context.getPathPool().withTable(db_name, table_name, false));
    auto storage_pool = std::make_shared<StoragePool>(
        context,
        NullspaceID,
        /*NAMESPACE_ID*/ 100,
        *storage_path_pool,
        fmt::format("{}.{}", db_name, table_name));
    storage_pool->restore();

    auto dm_context = DMContext::createUnique(
        context,
        storage_path_pool,
        storage_pool,
        /*min_version_*/ 0,
        NullspaceID,
        /*physical_table_id*/ 100,
        /*pk_col_id*/ MutSup::extra_handle_id,
        is_common_handle,
        1, // rowkey_column_size
        context.getSettingsRef());

    auto cols = DMTestEnv::getDefaultColumns(
        is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

    return std::pair{std::move(dm_context), std::move(cols)};
}

SegmentPtr createSegment(DMContext & dm_context, const ColumnDefinesPtr & cols, bool is_common_handle)
{
    return Segment::newSegment(
        Logger::get(),
        dm_context,
        cols,
        RowKeyRange::newAll(is_common_handle, 1),
        DELTA_MERGE_FIRST_SEGMENT_ID,
        0);
}

constexpr Int64 MaxHandle = 1000000;

class RandomSequence
{
public:
    RandomSequence(UInt32 n)
        : v(randomInt64s(n))
        , pos(v.begin())
    {}

    std::vector<Int64> get(UInt32 n)
    {
        std::vector<Int64> res;
        while (res.size() < n)
        {
            auto copied = std::min(std::distance(pos, v.end()), static_cast<ssize_t>(n - res.size()));
            res.insert(res.end(), pos, pos + copied);
            std::advance(pos, copied);
            if (pos == v.end())
                reset();
        }
        return res;
    }

    void reset() { pos = v.begin(); }

private:
    std::vector<Int64> randomInt64s(UInt32 n)
    {
        static constexpr int rnd_seed = 573172;
        std::mt19937 g(rnd_seed);
        std::vector<Int64> v(n);
        for (UInt32 i = 0; i < n; ++i)
        {
            v[i] = g() % MaxHandle;
        }
        return v;
    }

    std::vector<Int64> v;
    std::vector<Int64>::iterator pos;
};

Strings toMockCommonHandles(const std::vector<Int64> & v)
{
    Strings handles;
    for (Int64 i : v)
        handles.push_back(genMockCommonHandle(i, 1));
    return handles;
}

std::vector<Int64> delta;
void writeDelta(DMContext & dm_context, bool is_common_handle, Segment & seg, UInt32 delta_rows, RandomSequence & random_sequences)
{
    for (UInt32 i = 0; i < delta_rows; i += 2048)
    {
        Block block;
        const auto n = std::min(delta_rows - i, 2048U);
        const auto v = random_sequences.get(n);
        delta = v;
        if (is_common_handle)
            block.insert(createColumn<String>(toMockCommonHandles(v), MutSup::extra_handle_column_name, MutSup::extra_handle_id));
        else
            block.insert(createColumn<Int64>(v, MutSup::extra_handle_column_name, MutSup::extra_handle_id));
        block.insert(createColumn<UInt64>(
            std::vector<UInt64>(n, version++),
            MutSup::version_column_name,
            MutSup::version_col_id));
        block.insert(createColumn<UInt8>(
            std::vector<UInt64>(n, /*deleted*/ 0),
            MutSup::delmark_column_name,
            MutSup::delmark_col_id));
        seg.write(dm_context, block, false);
    }
}

SegmentPtr createSegmentWithData(
    DMContext & dm_context,
    const ColumnDefinesPtr & cols,
    bool is_common_handle,
    UInt32 delta_rows,
    RandomSequence & random_sequences)
{
    auto seg = createSegment(dm_context, cols, is_common_handle);
    auto block = DMTestEnv::prepareSimpleWriteBlock(
        0,
        1000000,
        false,
        version++,
        MutSup::extra_handle_column_name,
        MutSup::extra_handle_id,
        is_common_handle ? MutSup::getExtraHandleColumnStringType() : MutSup::getExtraHandleColumnIntType(),
        is_common_handle);
    seg->write(dm_context, block, false);
    seg = seg->mergeDelta(dm_context, cols);
    writeDelta(dm_context, is_common_handle, *seg, delta_rows, random_sequences);
    return seg;
}

DeltaIndexPtr buildDeltaIndex(
    const DMContext & dm_context,
    const ColumnDefines & cols,
    const SegmentSnapshotPtr & snapshot,
    Segment & segment)
{
    auto pk_ver_col_defs = std::make_shared<ColumnDefines>(ColumnDefines{cols[0], cols[1]});

    auto delta_reader = std::make_shared<DeltaValueReader>(
        dm_context,
        snapshot->delta,
        pk_ver_col_defs,
        segment.getRowKeyRange(),
        ReadTag::MVCC);

    auto [delta_index, fully_indexed] = segment.ensurePlace(
        dm_context,
        snapshot,
        delta_reader,
        {segment.getRowKeyRange()},
        std::numeric_limits<UInt64>::max());

    if (delta_index == nullptr || !fully_indexed)
        std::abort();

    return delta_index;
}

template <typename T>
void buildVersionChain(const DMContext & dm_context, const SegmentSnapshot & snapshot, VersionChain<T> & version_chain)
{
    const auto base_ver_snap = version_chain.replaySnapshot(dm_context, snapshot);
    benchmark::DoNotOptimize(base_ver_snap);
}

auto initialize(bool is_common_handle, UInt32 delta_rows)
{
    auto [context, table_name] = getContext();
    auto [dm_context, cols] = getDMContext(*context, table_name, is_common_handle);
    RandomSequence random_sequences{10 * MaxHandle};
    auto segment = createSegmentWithData(*dm_context, cols, is_common_handle, delta_rows, random_sequences);
    auto segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
    return std::tuple{
        std::move(context),
        std::move(dm_context),
        std::move(cols),
        std::move(segment),
        std::move(segment_snapshot)};
}

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
    auto delta_index = 
    buildDeltaIndex(*dm_context, *cols, segment_snapshot, *segment);
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
