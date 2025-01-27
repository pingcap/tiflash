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

#pragma once

#include <Common/CurrentMetrics.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <magic_enum.hpp>
#include <random>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
} // namespace CurrentMetrics

namespace DB::DM::tests::MVCC
{
inline const String db_name = "test";
inline UInt64 version = 1;

inline constexpr bool IsCommonHandle = true;
inline constexpr bool NotCommonHandle = false;

inline constexpr bool ForceReleaseCache = true;

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

inline auto loadPackFilterResults(
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

inline auto getContext()
{
    const auto table_name = std::to_string(clock_gettime_ns());
    const auto testdata_path = fmt::format("/tmp/{}", table_name);
    constexpr auto run_mode = DB::PageStorageRunMode::ONLY_V3;
    TiFlashTestEnv::initializeGlobalContext({testdata_path}, run_mode);
    return std::pair{TiFlashTestEnv::getContext(), std::move(table_name)};
}

inline auto getDMContext(Context & context, const String & table_name, bool is_common_handle)
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

inline SegmentPtr createSegment(DMContext & dm_context, const ColumnDefinesPtr & cols, bool is_common_handle)
{
    return Segment::newSegment(
        Logger::get(),
        dm_context,
        cols,
        RowKeyRange::newAll(is_common_handle, 1),
        DELTA_MERGE_FIRST_SEGMENT_ID,
        0);
}

class WriteSequence
{
public:
    virtual ~WriteSequence() = default;
    virtual std::vector<Int64> getStable() = 0;
    virtual std::vector<Int64> getDelta(UInt32 n) = 0;

protected:
    std::vector<Int64> getVec(Int64 stable_start, Int64 stable_end)
    {
        std::vector<Int64> v(stable_end - stable_start);
        std::iota(v.begin(), v.end(), stable_start);
        return v;
    }

    std::vector<Int64> getRandoms(Int64 rand_start, Int64 rand_end)
    {
        auto v = getVec(rand_start, rand_end);
        static constexpr int rnd_seed = 573172;
        std::mt19937 g(rnd_seed);
        std::shuffle(v.begin(), v.end(), g);
        return v;
    }
};

class RandomUpdateSequence : public WriteSequence
{
public:
    RandomUpdateSequence()
        : max_handle(1000000)
        , rnd_v(getRandoms(0, max_handle))
        , pos(rnd_v.begin())
    {}

    std::vector<Int64> getStable() override { return getVec(0, max_handle); }

    std::vector<Int64> getDelta(UInt32 n) override
    {
        std::vector<Int64> res;
        while (res.size() < n)
        {
            auto copied = std::min(std::distance(pos, rnd_v.end()), static_cast<ssize_t>(n - res.size()));
            res.insert(res.end(), pos, pos + copied);
            std::advance(pos, copied);
            if (pos == rnd_v.end())
                pos = rnd_v.begin();
        }
        return res;
    }

private:
    const Int64 max_handle;
    const std::vector<Int64> rnd_v;
    std::vector<Int64>::const_iterator pos;
};

class AppendOnlySequence : public WriteSequence
{
public:
    std::vector<Int64> getStable() override { return getVec(0, curr_handle); }

    std::vector<Int64> getDelta(UInt32 n) override
    {
        std::vector<Int64> v(n);
        std::iota(v.begin(), v.end(), curr_handle);
        curr_handle += n;
        return v;
    }

private:
    Int64 curr_handle = 1000000;
};

class RandomInsertSequence : public WriteSequence
{
public:
    RandomInsertSequence()
        : rnds(getRandoms(0, 10000000)) // 1kw
        , pos(rnds.begin())
    {}

    std::vector<Int64> getStable() override { return std::vector<Int64>(rnds.begin(), rnds.begin() + 1000000); }

    std::vector<Int64> getDelta(UInt32 n) override
    {
        RUNTIME_CHECK(std::distance(pos, rnds.end()) >= n, std::distance(pos, rnds.end()), n);
        std::vector<Int64> res;
        res.insert(res.end(), pos, pos + n);
        std::advance(pos, n);
        return res;
    }

private:
    const std::vector<Int64> rnds;
    std::vector<Int64>::const_iterator pos;
};

std::unique_ptr<WriteSequence> createWriteSequence(WriteLoad write_load)
{
    switch (write_load)
    {
    case WriteLoad::RandomUpdate:
        return std::make_unique<RandomUpdateSequence>();
    case WriteLoad::AppendOnly:
        return std::make_unique<AppendOnlySequence>();
    case WriteLoad::RandomInsert:
        return std::make_unique<RandomInsertSequence>();
    }
}

inline Strings toMockCommonHandles(const std::vector<Int64> & v)
{
    Strings handles;
    for (Int64 i : v)
        handles.push_back(genMockCommonHandle(i, 1));
    return handles;
}

template <ExtraHandleType HandleType>
Block createBlock(std::vector<HandleType> handles)
{
    Block block;
    block.insert(createColumn<HandleType>(handles, MutSup::extra_handle_column_name, MutSup::extra_handle_id));

    static UInt64 version = 1;
    block.insert(createColumn<UInt64>(
        std::vector<UInt64>(block.rows(), version++),
        MutSup::version_column_name,
        MutSup::version_col_id));

    block.insert(createColumn<UInt8>(
        std::vector<UInt64>(block.rows(), /*deleted*/ 0),
        MutSup::delmark_column_name,
        MutSup::delmark_col_id));
    return block;
}

inline void writeDelta(
    DMContext & dm_context,
    bool is_common_handle,
    Segment & seg,
    UInt32 delta_rows,
    WriteSequence & write_seq)
{
    for (UInt32 i = 0; i < delta_rows; i += 2048)
    {
        const auto n = std::min(delta_rows - i, 2048U);
        const auto v = write_seq.getDelta(n);
        auto block = is_common_handle ? createBlock(toMockCommonHandles(v)) : createBlock(std::move(v));
        seg.write(dm_context, block, false);
    }
}

inline SegmentPtr createSegmentWithData(
    DMContext & dm_context,
    const ColumnDefinesPtr & cols,
    bool is_common_handle,
    UInt32 delta_rows,
    WriteSequence & write_seq)
{
    auto seg = createSegment(dm_context, cols, is_common_handle);
    auto v = write_seq.getStable();
    auto block = is_common_handle ? createBlock(toMockCommonHandles(v)) : createBlock(std::move(v));
    seg->write(dm_context, block, false);
    seg = seg->mergeDelta(dm_context, cols);
    writeDelta(dm_context, is_common_handle, *seg, delta_rows, write_seq);
    return seg;
}

inline DeltaIndexPtr buildDeltaIndex(
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
auto buildVersionChain(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    VersionChain<T> & version_chain,
    const bool force_release_cache)
{
    return version_chain.replaySnapshot(dm_context, snapshot, force_release_cache);
}

inline auto initialize(WriteLoad write_load, bool is_common_handle, UInt32 delta_rows)
{
    auto [context, table_name] = getContext();
    auto [dm_context, cols] = getDMContext(*context, table_name, is_common_handle);
    auto write_seq = createWriteSequence(write_load);
    auto segment = createSegmentWithData(*dm_context, cols, is_common_handle, delta_rows, *write_seq);
    auto segment_snapshot = segment->createSnapshot(*dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
    return std::tuple{
        std::move(context),
        std::move(dm_context),
        std::move(cols),
        std::move(segment),
        std::move(segment_snapshot),
        std::move(write_seq)};
}

} // namespace DB::DM::tests::MVCC
