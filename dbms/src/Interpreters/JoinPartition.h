// Copyright 2023 PingCAP, Inc.
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

#include <Columns/ColumnNullable.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/JoinHashMap.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <absl/base/optimization.h>

namespace DB
{
class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
using Sizes = std::vector<size_t>;

struct BuildPartition
{
    BlocksList blocks;
    Blocks original_blocks;
    size_t rows{0};
    size_t bytes{0};
};
struct ProbePartition
{
    Blocks blocks;
    size_t rows{0};
    size_t bytes{0};
};

class Join;
struct alignas(ABSL_CACHELINE_SIZE) RowsNotInsertToMap
{
    explicit RowsNotInsertToMap(size_t max_block_size_)
        : max_block_size(max_block_size_)
    {
        RUNTIME_ASSERT(max_block_size > 0);
    }

    /// Row list
    RowRefList head;
    /// Materialized rows
    std::vector<MutableColumns> materialized_columns_vec;
    size_t max_block_size;

    size_t total_size = 0;

    /// Insert row to this structure.
    /// If need_materialize is true, row will be inserted into materialized_columns_vec.
    /// Else it will be inserted into row list.
    void insertRow(Block * stored_block, size_t index, bool need_materialize, Arena & pool);
};

class JoinPartition;
using JoinPartitions = std::vector<std::unique_ptr<JoinPartition>>;
class JoinPartition
{
public:
    JoinPartition(JoinMapMethod join_map_type_, ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_, size_t max_block_size, const LoggerPtr & log_, bool has_other_condition_)
        : kind(kind_)
        , strictness(strictness_)
        , join_map_method(join_map_type_)
        , pool(std::make_shared<Arena>())
        , spill(false)
        , has_other_condition(has_other_condition_)
        , log(log_)
    {
        /// Choose data structure to use for JOIN.
        initMap();
        if (needRecordNotInsertRows(kind))
            rows_not_inserted_to_map = std::make_unique<RowsNotInsertToMap>(max_block_size);
        memory_usage = getHashMapAndPoolByteCount();
    }
    void insertBlockForBuild(Block && block);
    void insertBlockForProbe(Block && block);
    size_t getRowCount();
    size_t getHashMapAndPoolByteCount();
    RowsNotInsertToMap * getRowsNotInsertedToMap()
    {
        if (needRecordNotInsertRows(kind))
        {
            assert(rows_not_inserted_to_map != nullptr);
            return rows_not_inserted_to_map.get();
        }
        return nullptr;
    };
    Blocks trySpillProbePartition(bool force, size_t max_cached_data_bytes)
    {
        std::unique_lock lock(partition_mutex);
        return trySpillProbePartition(force, max_cached_data_bytes, lock);
    }
    Blocks trySpillBuildPartition(bool force, size_t max_cached_data_bytes)
    {
        std::unique_lock lock(partition_mutex);
        return trySpillBuildPartition(force, max_cached_data_bytes, lock);
    }
    std::unique_lock<std::mutex> lockPartition();
    /// use lock as the argument to force the caller acquire the lock before call them
    void releaseBuildPartitionBlocks(std::unique_lock<std::mutex> &);
    void releaseProbePartitionBlocks(std::unique_lock<std::mutex> &);
    void releasePartitionPoolAndHashMap(std::unique_lock<std::mutex> &);
    Blocks trySpillBuildPartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock);
    Blocks trySpillProbePartition(bool force, size_t max_cached_data_bytes, std::unique_lock<std::mutex> & partition_lock);
    bool hasBuildData() const { return !build_partition.original_blocks.empty(); }
    void addMemoryUsage(size_t delta)
    {
        memory_usage += delta;
    }
    void subMemoryUsage(size_t delta)
    {
        if likely (memory_usage >= delta)
            memory_usage -= delta;
        else
            memory_usage = 0;
    }
    bool isSpill() const { return spill; }
    void markSpill() { spill = true; }
    JoinMapMethod getJoinMapMethod() const { return join_map_method; }
    ASTTableJoin::Kind getJoinKind() const { return kind; }
    Block * getLastBuildBlock() { return &build_partition.blocks.back(); }
    ArenaPtr & getPartitionPool()
    {
        assert(pool != nullptr);
        return pool;
    }
    size_t getMemoryUsage() const { return memory_usage; }
    template <typename Map>
    Map & getHashMap();

    /// insert block to hash maps in `JoinPartitions`
    static void insertBlockIntoMaps(
        JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const std::vector<size_t> & key_sizes,
        const TiDB::TiDBCollators & collators,
        Block * stored_block,
        ConstNullMapPtr & null_map,
        size_t stream_index,
        size_t insert_concurrency,
        bool enable_fine_grained_shuffle,
        bool enable_join_spill);

    /// probe the block using hash maps in `JoinPartitions`
    static void probeBlock(
        const JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const std::vector<size_t> & key_sizes,
        MutableColumns & added_columns,
        ConstNullMapPtr null_map,
        std::unique_ptr<IColumn::Filter> & filter,
        IColumn::Offset & current_offset,
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
        const std::vector<size_t> & right_indexes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        ProbeProcessInfo & probe_process_info,
        MutableColumnPtr & record_mapped_entry_column);
    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    static void probeBlockImpl(
        const JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const std::vector<size_t> & key_sizes,
        MutableColumns & added_columns,
        ConstNullMapPtr null_map,
        std::unique_ptr<IColumn::Filter> & filter,
        IColumn::Offset & current_offset,
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
        const std::vector<size_t> & right_indexes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        ProbeProcessInfo & probe_process_info,
        MutableColumnPtr & record_mapped_entry_column);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    static std::pair<PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> probeBlockNullAware(
        const JoinPartitions & join_partitions,
        Block & block,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const NALeftSideInfo & left_side_info,
        const NARightSideInfo & right_side_info);

    void releasePartition();

private:
    friend class ScanHashMapAfterProbeBlockInputStream;
    void initMap();
    /// mutex to protect concurrent modify partition
    /// note if you wants to acquire both build_probe_mutex and partition_mutex,
    /// please lock build_probe_mutex first
    std::mutex partition_mutex;
    BuildPartition build_partition;
    ProbePartition probe_partition;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;
    JoinMapMethod join_map_method;
    MapsAny maps_any; /// For ANY LEFT|INNER JOIN
    MapsAll maps_all; /// For ALL LEFT|INNER JOIN
    MapsAnyFull maps_any_full; /// For ANY RIGHT|FULL JOIN
    MapsAllFull maps_all_full; /// For ALL RIGHT|FULL JOIN
    MapsAllFullWithRowFlag maps_all_full_with_row_flag; /// For RIGHT_SEMI | RIGHT_ANTI_SEMI with other conditions
    /// For right/full/rightSemi/rightAnti join, including
    /// 1. Rows with NULL join keys
    /// 2. Rows that are filtered by right join conditions
    /// For null-aware semi join family, including rows with NULL join keys.
    std::unique_ptr<RowsNotInsertToMap> rows_not_inserted_to_map;
    ArenaPtr pool;
    bool spill;
    bool has_other_condition;
    /// only update this field when spill is enabled. todo support this field in non-spill mode
    std::atomic<size_t> memory_usage{0};
    const LoggerPtr log;
};
} // namespace DB
