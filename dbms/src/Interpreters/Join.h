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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Core/Spiller.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/CancellationHook.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinHashMap.h>
#include <Interpreters/JoinPartition.h>
#include <Interpreters/SettingsCommon.h>

#include <shared_mutex>

namespace DB
{
class Join;
using JoinPtr = std::shared_ptr<Join>;
using Joins = std::vector<JoinPtr>;

struct RestoreInfo
{
    JoinPtr join;
    BlockInputStreamPtr scan_hash_map_stream;
    BlockInputStreamPtr build_stream;
    BlockInputStreamPtr probe_stream;

    RestoreInfo(JoinPtr & join_, BlockInputStreamPtr && scan_hash_map_stream_, BlockInputStreamPtr && build_stream_, BlockInputStreamPtr && probe_stream_)
        : join(join_)
        , scan_hash_map_stream(std::move(scan_hash_map_stream_))
        , build_stream(std::move(build_stream_))
        , probe_stream(std::move(probe_stream_))
    {}
};

struct PartitionBlock
{
    size_t partition_index;
    Block block;

    PartitionBlock()
        : partition_index(0)
        , block({})
    {}

    explicit PartitionBlock(Block && block_)
        : partition_index(0)
        , block(std::move(block_))
    {}

    PartitionBlock(size_t partition_index_, Block && block_)
        : partition_index(partition_index_)
        , block(std::move(block_))
    {}

    explicit operator bool() const { return static_cast<bool>(block); }
    bool operator!() const { return !block; }
};
using PartitionBlocks = std::list<PartitionBlock>;

/** Data structure for implementation of JOIN.
  * It is just a hash table: keys -> rows of joined ("right") table.
  * Additionally, CROSS JOIN is supported: instead of hash table, it use just set of blocks without keys.
  *
  * JOIN-s could be of nine types: ANY/ALL × LEFT/INNER/RIGHT/FULL, and also CROSS.
  *
  * If ANY is specified - then select only one row from the "right" table, (first encountered row), even if there was more matching rows.
  * If ALL is specified - usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY is more efficient.
  *
  * If INNER is specified - leave only rows that have matching rows from "right" table.
  * If LEFT is specified - in case when there is no matching row in "right" table, fill it with default values instead.
  * If RIGHT is specified - first process as INNER, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  * If FULL is specified - first process as LEFT, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  *
  * Thus, LEFT and RIGHT JOINs are not symmetric in terms of implementation.
  *
  * All JOINs (except CROSS) are done by equality condition on keys (equijoin).
  * Non-equality and other conditions are not supported.
  *
  * Implementation:
  *
  * 1. Build hash table in memory from "right" table.
  * This hash table is in form of keys -> row in case of ANY or keys -> [rows...] in case of ALL.
  * This is done in insertFromBlock method.
  *
  * 2. Process "left" table and join corresponding rows from "right" table by lookups in the map.
  * This is done in joinBlock methods.
  *
  * In case of ANY LEFT JOIN - form new columns with found values or default values.
  * This is the most simple. Number of rows in left table does not change.
  *
  * In case of ANY INNER JOIN - form new columns with found values,
  *  and also build a filter - in what rows nothing was found.
  * Then filter columns of "left" table.
  *
  * In case of ALL ... JOIN - form new columns with all found rows,
  *  and also fill 'offsets' array, describing how many times we need to replicate values of "left" table.
  * Then replicate columns of "left" table.
  *
  * How Nullable keys are processed:
  *
  * NULLs never join to anything, even to each other.
  * During building of map, we just skip keys with NULL value of any component.
  * During joining, we simply treat rows with any NULLs in key as non joined.
  *
  * Default values for outer joins (LEFT, RIGHT, FULL):
  *
  * Always generate Nullable column and substitute NULLs for non-joined rows,
  *  as in standard SQL.
  */

class Join
{
public:
    Join(const Names & key_names_left_,
         const Names & key_names_right_,
         ASTTableJoin::Kind kind_,
         ASTTableJoin::Strictness strictness_,
         const String & req_id,
         bool enable_fine_grained_shuffle_,
         size_t fine_grained_shuffle_count_,
         size_t max_bytes_before_external_join_,
         const SpillConfig & build_spill_config_,
         const SpillConfig & probe_spill_config_,
         Int64 join_restore_concurrency_,
         const TiDB::TiDBCollators & collators_ = TiDB::dummy_collators,
         const JoinNonEqualConditions & non_equal_conditions_ = {},
         size_t max_block_size = 0,
         const String & match_helper_name_ = "",
         const String & flag_mapped_entry_helper_name_ = "",
         size_t restore_round = 0,
         bool is_test = true);

    size_t restore_round;

    /** Call `setBuildConcurrencyAndInitJoinPartition` and `setSampleBlock`.
      * You must call this method before subsequent calls to insertFromBlock.
      */
    void initBuild(const Block & sample_block, size_t build_concurrency_ = 1);

    void initProbe(const Block & sample_block, size_t probe_concurrency_ = 1);

    void insertFromBlock(const Block & block, size_t stream_index);

    /** Join data from the map (that was previously built by calls to insertFromBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    Block joinBlock(ProbeProcessInfo & probe_process_info, bool dry_run = false) const;

    void checkTypes(const Block & block) const;

    /**
      * A stream that will scan and output rows from right table, might contain default values from left table
      * Use only after all calls to joinBlock was done.
      */
    BlockInputStreamPtr createScanHashMapAfterProbeStream(const Block & left_sample_block, size_t index, size_t step, size_t max_block_size) const;

    bool isEnableSpill() const;

    bool isRestoreJoin() const;

    bool getPartitionSpilled(size_t partition_index);

    bool hasPartitionSpilledWithLock();

    bool hasPartitionSpilled();

    bool isSpilled() const { return is_spilled; }

    std::optional<RestoreInfo> getOneRestoreStream(size_t max_block_size);

    void dispatchProbeBlock(Block & block, PartitionBlocks & partition_blocks_list);

    Blocks dispatchBlock(const Strings & key_columns_names, const Block & from_block);

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount();
    /// The peak build bytes usage, if spill is not enabled, the same as getTotalByteCount
    size_t getPeakBuildBytesUsage();

    size_t getTotalBuildInputRows() const { return total_input_build_rows; }

    ASTTableJoin::Kind getKind() const { return kind; }

    const Names & getLeftJoinKeys() const { return key_names_left; }

    void setInitActiveBuildThreads()
    {
        std::unique_lock lock(build_probe_mutex);
        active_build_threads = getBuildConcurrency();
    }

    size_t getProbeConcurrency() const
    {
        std::unique_lock lock(build_probe_mutex);
        return probe_concurrency;
    }
    void setProbeConcurrency(size_t concurrency)
    {
        std::unique_lock lock(build_probe_mutex);
        probe_concurrency = concurrency;
        active_probe_threads = probe_concurrency;
    }

    void wakeUpAllWaitingThreads()
    {
        std::unique_lock lk(build_probe_mutex);
        skip_wait = true;
        probe_cv.notify_all();
        build_cv.notify_all();
    }

    void finishOneBuild();
    void waitUntilAllBuildFinished() const;

    void finishOneProbe();
    void waitUntilAllProbeFinished() const;

    void finishOneNonJoin(size_t partition_index);

    size_t getBuildConcurrency() const
    {
        if (unlikely(build_concurrency == 0))
            throw Exception("Logical error: `setBuildConcurrencyAndInitPool` has not been called", ErrorCodes::LOGICAL_ERROR);
        return build_concurrency;
    }

    void meetError(const String & error_message);
    void meetErrorImpl(const String & error_message, std::unique_lock<std::mutex> & lock);
    void setCancellationHook(CancellationHook cancellation_hook) { is_cancelled = cancellation_hook; }

    static const String match_helper_prefix;
    static const DataTypePtr match_helper_type;
    static const String flag_mapped_entry_helper_prefix;
    static const DataTypePtr flag_mapped_entry_helper_type;

    // only use for left outer semi joins.
    const String match_helper_name;
    // only use for right semi, right anti joins with other conditions,
    // used to name the column that records matched map entry before other conditions filter
    const String flag_mapped_entry_helper_name;

    SpillerPtr build_spiller;
    SpillerPtr probe_spiller;

private:
    friend class ScanHashMapAfterProbeBlockInputStream;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;
    bool has_other_condition;
    ASTTableJoin::Strictness original_strictness;
    const bool may_probe_side_expanded_after_join;

    /// Names of key columns (columns for equi-JOIN) in "left" table (in the order they appear in USING clause).
    const Names key_names_left;
    /// Names of key columns (columns for equi-JOIN) in "right" table (in the order they appear in USING clause).
    const Names key_names_right;

    mutable std::mutex build_probe_mutex;

    mutable std::condition_variable build_cv;
    size_t build_concurrency;
    std::atomic<size_t> active_build_threads;

    mutable std::condition_variable probe_cv;
    size_t probe_concurrency;
    std::atomic<size_t> active_probe_threads;

    bool skip_wait = false;
    bool meet_error = false;
    String error_message;

    /// collators for the join key
    const TiDB::TiDBCollators collators;

    const JoinNonEqualConditions non_equal_conditions;

    size_t max_block_size;
    /** Blocks of "right" table.
      */
    BlocksList blocks;
    Blocks original_blocks;
    /// mutex to protect concurrent insert to blocks
    std::mutex blocks_lock;

    JoinPartitions partitions;

    std::list<size_t> spilled_partition_indexes;

    size_t max_bytes_before_external_join;
    SpillConfig build_spill_config;
    SpillConfig probe_spill_config;
    Int64 join_restore_concurrency;
    bool is_spilled = false;
    bool disable_spill = false;
    std::atomic<size_t> peak_build_bytes_usage{0};

    BlockInputStreams restore_build_streams;
    BlockInputStreams restore_probe_streams;
    BlockInputStreams restore_scan_hash_map_streams;
    Int64 restore_join_build_concurrency = -1;

    JoinPtr restore_join;

    /// Whether to directly check all blocks for row with null key.
    bool null_key_check_all_blocks_directly = false;

    /// For null-aware semi join with no other condition.
    /// Indicate if the right table is empty.
    std::atomic<bool> right_table_is_empty{true};
    /// Indicate if the right table has a all-key-null row.
    std::atomic<bool> right_has_all_key_null_row{false};

    bool has_build_data_in_memory = false;

private:
    JoinMapMethod join_map_method = JoinMapMethod::EMPTY;

    Sizes key_sizes;

    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table.
    Block sample_block_with_keys;

    bool is_test;

    Block build_sample_block;
    Block probe_sample_block;

    const LoggerPtr log;

    std::atomic<size_t> total_input_build_rows{0};

    /** Protect state for concurrent use in insertFromBlock and joinBlock.
      * Note that these methods could be called simultaneously only while use of StorageJoin,
      *  and StorageJoin only calls these two methods.
      * That's why another methods are not guarded.
      */
    mutable std::shared_mutex rwlock;

    bool initialized = false;
    bool enable_fine_grained_shuffle = false;
    size_t fine_grained_shuffle_count = 0;

    CancellationHook is_cancelled{[]() {
        return false;
    }};

    /** Set information about structure of right hand of JOIN (joined data).
      * You must call this method before subsequent calls to insertFromBlock.
      */
    void setSampleBlock(const Block & block);

    /** Set Join build concurrency and init hash map.
      * You must call this method before subsequent calls to insertFromBlock.
      */
    void setBuildConcurrencyAndInitJoinPartition(size_t build_concurrency_);

    /// Throw an exception if blocks have different types of key columns.
    void checkTypesOfKeys(const Block & block_left, const Block & block_right) const;

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    void insertFromBlockInternal(Block * stored_block, size_t stream_index);

    Block joinBlockHash(ProbeProcessInfo & probe_process_info) const;
    Block doJoinBlockHash(ProbeProcessInfo & probe_process_info) const;

    Block joinBlockNullAware(ProbeProcessInfo & probe_process_info) const;

    Block joinBlockCross(ProbeProcessInfo & probe_process_info) const;

    /** Handle non-equal join conditions
      *
      * @param block
      */
    void handleOtherConditions(Block & block, std::unique_ptr<IColumn::Filter> & filter, std::unique_ptr<IColumn::Offsets> & offsets_to_replicate, const std::vector<size_t> & right_table_column) const;

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
    void joinBlockCrossImpl(Block & block, ConstNullMapPtr null_map) const;

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockNullAwareImpl(
        Block & block,
        size_t left_columns,
        const ColumnRawPtrs & key_columns,
        const ConstNullMapPtr & null_map,
        const ConstNullMapPtr & filter_map,
        const ConstNullMapPtr & all_key_null_map) const;

    IColumn::Selector hashToSelector(const WeakHash32 & hash) const;
    IColumn::Selector selectDispatchBlock(const Strings & key_columns_names, const Block & from_block);

    void spillAllBuildPartitions();
    void spillAllProbePartitions();
    /// use lock as the argument to force the caller acquire the lock before call them
    void releaseAllPartitions();


    void spillMostMemoryUsedPartitionIfNeed();
    std::shared_ptr<Join> createRestoreJoin(size_t max_bytes_before_external_join_);

    void workAfterBuildFinish();
    void workAfterProbeFinish();
};

} // namespace DB
