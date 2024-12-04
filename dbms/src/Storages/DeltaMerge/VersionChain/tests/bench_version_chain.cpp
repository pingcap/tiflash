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
ContextPtr context;
DMContextPtr dm_context;
ColumnDefinesPtr cols;

SegmentPtr segment;
SegmentSnapshotPtr segment_snapshot;

constexpr const char * log_level = "error";

const String db_name = "test";

UInt64 version = 1;

enum class BenchType
{
    None = 0,
    DeltaIndex = 1,
    VersionChain = 2,
};

void initContext(bool is_common_handle, BenchType type)
{
    if (context)
        return;

    bool enable_colors = isatty(STDERR_FILENO) && isatty(STDOUT_FILENO);
    DB::tests::TiFlashTestEnv::setupLogger(log_level, std::cerr, enable_colors);

    auto table_name = String(magic_enum::enum_name(type));
    UInt64 curr_ns = clock_gettime_ns();
    String testdata_path = fmt::format("/tmp/{}.{}", curr_ns, table_name);
    constexpr auto run_mode = DB::PageStorageRunMode::ONLY_V3;
    TiFlashTestEnv::initializeGlobalContext({testdata_path}, run_mode);
    context = TiFlashTestEnv::getContext();

    auto storage_path_pool
        = std::make_shared<StoragePathPool>(context->getPathPool().withTable(db_name, table_name, false));
    auto storage_pool = std::make_shared<StoragePool>(
        *context,
        NullspaceID,
        /*NAMESPACE_ID*/ 100,
        *storage_path_pool,
        fmt::format("{}.{}", db_name, table_name));
    storage_pool->restore();
    cols = DMTestEnv::getDefaultColumns(
        is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
    dm_context = DMContext::createUnique(
        *context,
        storage_path_pool,
        storage_pool,
        /*min_version_*/ 0,
        NullspaceID,
        /*physical_table_id*/ 100,
        /*pk_col_id*/ EXTRA_HANDLE_COLUMN_ID,
        is_common_handle,
        1, // rowkey_column_size
        context->getSettingsRef());
}

SegmentPtr createSegment(bool is_common_handle)
{
    return Segment::newSegment(
        Logger::get(),
        *dm_context,
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

RandomSequence random_sequences{10 * MaxHandle};

void writeDelta(Segment & seg, UInt32 delta_rows)
{
    for (UInt32 i = 0; i < delta_rows; i += 2048)
    {
        Block block;
        const auto n = std::min(delta_rows - i, 2048U);
        const auto v = random_sequences.get(n);
        block.insert(createColumn<Int64>(v, EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_ID));
        block.insert(createColumn<UInt64>(std::vector<UInt64>(n, version++), VERSION_COLUMN_NAME, VERSION_COLUMN_ID));
        block.insert(createColumn<UInt8>(std::vector<UInt64>(n, /*deleted*/ 0), TAG_COLUMN_NAME, TAG_COLUMN_ID));
        seg.write(*dm_context, block, false);
    }
}

SegmentPtr createSegment(bool is_common_handle, UInt32 delta_rows)
{
    auto seg = createSegment(is_common_handle);
    {
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, 1000000, false, version++);
        seg->write(*dm_context, block, false);
        seg = seg->mergeDelta(*dm_context, cols);
    }

    writeDelta(*seg, delta_rows);

    return seg;
}

DeltaIndexPtr buildDeltaIndex(const SegmentSnapshotPtr & snapshot, Segment & segment)
{
    auto pk_ver_col_defs = std::make_shared<ColumnDefines>(
        ColumnDefines{getExtraHandleColumnDefine(dm_context->is_common_handle), getVersionColumnDefine()});

    auto delta_reader = std::make_shared<DeltaValueReader>(
        *dm_context,
        snapshot->delta,
        pk_ver_col_defs,
        segment.getRowKeyRange(),
        ReadTag::MVCC);

    auto [delta_index, fully_indexed] = segment.ensurePlace(
        *dm_context,
        snapshot,
        delta_reader,
        {segment.getRowKeyRange()},
        std::numeric_limits<UInt64>::max());

    if (delta_index == nullptr || !fully_indexed)
        std::abort();

    return delta_index;
}

void buildVersionChain(const SegmentSnapshot & snapshot, VersionChain<Int64> & version_chain)
{
    const auto base_ver_snap = version_chain.replaySnapshot(*dm_context, snapshot);
    benchmark::DoNotOptimize(base_ver_snap);
}

void initialize(BenchType type, bool is_common_handle, UInt32 delta_rows)
{
    random_sequences.reset();
    initContext(is_common_handle, type);
    segment = createSegment(is_common_handle, delta_rows);
    segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
}

void shutdown()
{
    segment_snapshot = nullptr;
    segment = nullptr;
    cols = nullptr;
    dm_context = nullptr;
    context->shutdown();
    context = nullptr;
}

template <typename... Args>
void MVCCFullBuildIndex(benchmark::State & state, Args &&... args)
try
{
    const auto [type, is_common_handle, delta_rows] = std::make_tuple(std::move(args)...);
    initialize(type, is_common_handle, delta_rows);

    if (type == BenchType::DeltaIndex)
    {
        RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
        auto delta_index = buildDeltaIndex(segment_snapshot, *segment); // Warming up
        RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
        for (auto _ : state)
        {
            RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
            delta_index = buildDeltaIndex(segment_snapshot, *segment);
            RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
        }
    }
    else if (type == BenchType::VersionChain)
    {
        {
            VersionChain<Int64> version_chain;
            buildVersionChain(*segment_snapshot, version_chain); // Warming up
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
        for (auto _ : state)
        {
            VersionChain<Int64> version_chain;
            buildVersionChain(*segment_snapshot, version_chain);
            RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);
        }
    }
    shutdown();
}
CATCH

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
            auto version_chain = base_version_chain.deepCopy();
            RUNTIME_ASSERT(version_chain->getReplayedRows() == prepared_delta_rows);
            buildVersionChain(*segment_snapshot, *version_chain);
            RUNTIME_ASSERT(version_chain->getReplayedRows() == prepared_delta_rows + incremental_delta_rows);
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
                nullptr,
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
                nullptr,
                std::numeric_limits<UInt64>::max(),
                version_chain);
            benchmark::DoNotOptimize(bitmap_filter);
        }
    }
    shutdown();
}
CATCH

// TODO: move verify to unit-tests.
template <typename... Args>
void MVCCBuildBitmapVerify(benchmark::State & state, Args &&... args)
try
{
    const auto [type, is_common_handle, delta_rows] = std::make_tuple(std::move(args)...);
    initialize(type, is_common_handle, delta_rows);

    RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == 0);
    auto delta_index = buildDeltaIndex(segment_snapshot, *segment);
    RUNTIME_ASSERT(delta_index->getPlacedStatus().first == delta_rows);
    segment_snapshot->delta->getSharedDeltaIndex()->updateIfAdvanced(*delta_index);
    RUNTIME_ASSERT(segment_snapshot->delta->getSharedDeltaIndex()->getPlacedStatus().first == delta_rows);

    VersionChain<Int64> version_chain;
    buildVersionChain(*segment_snapshot, version_chain);
    RUNTIME_ASSERT(version_chain.getReplayedRows() == delta_rows);


    auto bitmap_filter1 = segment->buildBitmapFilter(
        *dm_context,
        segment_snapshot,
        {segment->getRowKeyRange()},
        nullptr,
        std::numeric_limits<UInt64>::max(),
        DEFAULT_BLOCK_SIZE,
        false);
    auto bitmap_filter2 = buildBitmapFilter<Int64>(
        *dm_context,
        *segment_snapshot,
        {segment->getRowKeyRange()},
        nullptr,
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

    shutdown();
}
CATCH


//constexpr bool IsCommonHandle = true;
constexpr bool IsNotCommonHandle = false;

BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_1, BenchType::DeltaIndex, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_5, BenchType::DeltaIndex, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_10, BenchType::DeltaIndex, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_100, BenchType::DeltaIndex, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_500, BenchType::DeltaIndex, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_1000, BenchType::DeltaIndex, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_5000, BenchType::DeltaIndex, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_10000, BenchType::DeltaIndex, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_50000, BenchType::DeltaIndex, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, delta_index_100000, BenchType::DeltaIndex, IsNotCommonHandle, 100000u);

BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_1, BenchType::VersionChain, IsNotCommonHandle, 1u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_5, BenchType::VersionChain, IsNotCommonHandle, 5u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_10, BenchType::VersionChain, IsNotCommonHandle, 10u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_100, BenchType::VersionChain, IsNotCommonHandle, 100u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_500, BenchType::VersionChain, IsNotCommonHandle, 500u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_1000, BenchType::VersionChain, IsNotCommonHandle, 1000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_5000, BenchType::VersionChain, IsNotCommonHandle, 5000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_10000, BenchType::VersionChain, IsNotCommonHandle, 10000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_50000, BenchType::VersionChain, IsNotCommonHandle, 50000u);
BENCHMARK_CAPTURE(MVCCFullBuildIndex, version_chain_100000, BenchType::VersionChain, IsNotCommonHandle, 100000u);

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
} // namespace
