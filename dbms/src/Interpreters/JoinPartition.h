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
#include <Interpreters/HashJoinSpillContext.h>
#include <Interpreters/JoinHashMap.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <absl/base/optimization.h>

namespace DB
{
class Arena;
struct ProbeProcessInfo;
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

struct JoinArenaPool
{
    Arena arena;
    std::vector<CachedColumnInfo *> cached_column_infos;
    size_t size() const { return cached_column_infos.capacity() * sizeof(CachedColumnInfo *) + arena.size(); }
    ~JoinArenaPool()
    {
        for (auto * cci : cached_column_infos)
            cci->~CachedColumnInfo();
        cached_column_infos.clear();
    }
};
using JoinArenaPoolPtr = std::shared_ptr<JoinArenaPool>;

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
    void insertRow(Block * stored_block, size_t index, bool need_materialize, JoinArenaPool & pool);
};

class JoinPartition;
using JoinPartitions = std::vector<std::unique_ptr<JoinPartition>>;
class JoinPartition
{
public:
    JoinPartition(
        JoinMapMethod join_map_type_,
        ASTTableJoin::Kind kind_,
        ASTTableJoin::Strictness strictness_,
        size_t partition_index_,
        size_t max_block_size,
        const HashJoinSpillContextPtr & hash_join_spill_context_,
        const LoggerPtr & log_,
        bool has_other_condition_)
        : partition_index(partition_index_)
        , pool(std::make_shared<JoinArenaPool>())
        , kind(kind_)
        , strictness(strictness_)
        , join_map_method(join_map_type_)
        , hash_join_spill_context(hash_join_spill_context_)
        , has_other_condition(has_other_condition_)
        , log(log_)
    {
        /// Choose data structure to use for JOIN.
        initMap();
        if (needRecordNotInsertRows(kind))
            rows_not_inserted_to_map = std::make_unique<RowsNotInsertToMap>(max_block_size);
        hash_table_pool_memory_usage = getHashMapAndPoolByteCount();
        block_data_memory_usage = 0;
    }
    void insertBlockForBuild(Block && block);
    void insertBlockForProbe(Block && block);
    size_t getRowCount();
    size_t getHashMapAndPoolByteCount();
    void setResizeCallbackIfNeeded();
    void updateHashMapAndPoolMemoryUsage();
    size_t getHashMapAndPoolMemoryUsage() const { return hash_table_pool_memory_usage; }
    RowsNotInsertToMap * getRowsNotInsertedToMap()
    {
        if (needRecordNotInsertRows(kind))
        {
            assert(rows_not_inserted_to_map != nullptr);
            return rows_not_inserted_to_map.get();
        }
        return nullptr;
    };
    Blocks trySpillProbePartition()
    {
        std::unique_lock lock(partition_mutex);
        return trySpillProbePartition(lock);
    }
    Blocks trySpillBuildPartition()
    {
        std::unique_lock lock(partition_mutex);
        return trySpillBuildPartition(lock);
    }
    std::unique_lock<std::mutex> lockPartition();
    std::unique_lock<std::mutex> tryLockPartition();
    /// use lock as the argument to force the caller acquire the lock before call them
    void releaseBuildPartitionBlocks(std::unique_lock<std::mutex> &);
    void releaseProbePartitionBlocks(std::unique_lock<std::mutex> &);
    void releasePartitionPoolAndHashMap(std::unique_lock<std::mutex> &);
    Blocks trySpillBuildPartition(std::unique_lock<std::mutex> & partition_lock);
    Blocks trySpillProbePartition(std::unique_lock<std::mutex> & partition_lock);
    bool hasBuildData() const { return !build_partition.original_blocks.empty(); }
    void addBlockDataMemoryUsage(size_t delta) { block_data_memory_usage += delta; }
    bool isSpill() const { return hash_join_spill_context->isPartitionSpilled(partition_index); }
    JoinMapMethod getJoinMapMethod() const { return join_map_method; }
    ASTTableJoin::Kind getJoinKind() const { return kind; }
    Block * getLastBuildBlock() { return &build_partition.blocks.back(); }
    JoinArenaPoolPtr & getPartitionPool()
    {
        assert(pool != nullptr);
        return pool;
    }
    size_t getMemoryUsage() const { return block_data_memory_usage + hash_table_pool_memory_usage; }
    size_t revocableBytes() const
    {
        if (build_partition.rows > 0 || probe_partition.rows > 0)
            return getMemoryUsage();
        else
            return 0;
    }
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
        bool enable_join_spill,
        size_t probe_cache_column_threshold);

    /// probe the block using hash maps in `JoinPartitions`
    static void probeBlock(
        const JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const std::vector<size_t> & key_sizes,
        MutableColumns & added_columns,
        ConstNullMapPtr null_map,
        IColumn::Offset & current_offset,
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
        const std::vector<size_t> & right_indexes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        ProbeProcessInfo & probe_process_info);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps, bool row_flagged_map>
    static void probeBlockImpl(
        const JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const std::vector<size_t> & key_sizes,
        MutableColumns & added_columns,
        ConstNullMapPtr null_map,
        IColumn::Offset & current_offset,
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
        const std::vector<size_t> & right_indexes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        ProbeProcessInfo & probe_process_info);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    static std::pair<PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> probeBlockNullAwareSemi(
        const JoinPartitions & join_partitions,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const NALeftSideInfo & left_side_info,
        const NARightSideInfo & right_side_info);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    static std::pair<PaddedPODArray<SemiJoinResult<KIND, STRICTNESS>>, std::list<SemiJoinResult<KIND, STRICTNESS> *>> probeBlockSemi(
        const JoinPartitions & join_partitions,
        size_t rows,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        const ProbeProcessInfo & probe_process_info);

    void releasePartition();

private:
    friend class ScanHashMapAfterProbeBlockInputStream;
    void initMap();
    size_t partition_index;
    /// mutex to protect concurrent modify partition
    /// note if you wants to acquire both build_probe_mutex and partition_mutex,
    /// please lock build_probe_mutex first
    std::mutex partition_mutex;
    BuildPartition build_partition;
    ProbePartition probe_partition;

    JoinArenaPoolPtr pool;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;
    JoinMapMethod join_map_method;
    MapsAny maps_any; /// For ANY LEFT|INNER JOIN
    MapsAll maps_all; /// For ALL LEFT|INNER JOIN
    MapsAllFull maps_all_full; /// For ALL RIGHT|FULL JOIN
    MapsAllFullWithRowFlag
        maps_all_full_with_row_flag; /// For RIGHT_SEMI | RIGHT_ANTI_SEMI | RIGHT_OUTER with other conditions
    /// For right outer/full/rightSemi/rightAnti join, including
    /// 1. Rows with NULL join keys
    /// 2. Rows that are filtered by right join conditions
    /// For null-aware semi join family, including rows with NULL join keys.
    std::unique_ptr<RowsNotInsertToMap> rows_not_inserted_to_map;
    HashJoinSpillContextPtr hash_join_spill_context;
    bool has_other_condition;
    /// only update this field when spill is enabled. todo support this field in non-spill mode
    /// all writes to it is protected by lock
    std::atomic<size_t> block_data_memory_usage{0};
    std::atomic<size_t> hash_table_pool_memory_usage{0};
    const LoggerPtr log;
};
} // namespace DB
