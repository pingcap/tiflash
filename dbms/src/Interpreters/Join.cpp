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

#include <Columns/ColumnUtils.h>
#include <Columns/ColumnsCommon.h>
#include <Common/ColumnsHashing.h>
#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Core/AutoSpillTrigger.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Pipeline/Schedule/Tasks/OneTimeNotifyFuture.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/CrossJoinProbeHelper.h>
#include <Interpreters/Join.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/NullableUtils.h>
#include <common/logger_useful.h>

#include <exception>
#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_join_prob_failpoint[];
extern const char exception_mpp_hash_build[];
extern const char exception_mpp_hash_probe[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TYPE_MISMATCH;
} // namespace ErrorCodes

namespace
{
ColumnRawPtrs getKeyColumns(const Names & key_names, const Block & block)
{
    size_t keys_size = key_names.size();
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i] = block.getByName(key_names[i]).column.get();

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i]->isColumnNullable())
            key_columns[i] = &static_cast<const ColumnNullable &>(*key_columns[i]).getNestedColumn();
    }

    return key_columns;
}
size_t getRestoreJoinBuildConcurrency(
    size_t total_partitions,
    size_t spilled_partitions,
    Int64 join_restore_concurrency,
    size_t total_concurrency)
{
    if (join_restore_concurrency < 0)
    {
        /// restore serially, so one restore join will take up all the concurrency
        return total_concurrency;
    }
    else if (join_restore_concurrency > 0)
    {
        /// try to restore `join_restore_concurrency` partition at a time, but restore_join_build_concurrency should be at least 2
        return std::max(2, total_concurrency / join_restore_concurrency);
    }
    else
    {
        assert(total_partitions >= spilled_partitions);
        size_t unspilled_partitions = total_partitions - spilled_partitions;
        /// try to restore at most (unspilled_partitions - 1) partitions at a time
        size_t max_concurrent_restore_partition = unspilled_partitions <= 1 ? 1 : unspilled_partitions - 1;
        size_t restore_times
            = (spilled_partitions + max_concurrent_restore_partition - 1) / max_concurrent_restore_partition;
        size_t restore_build_concurrency = (restore_times * total_concurrency) / spilled_partitions;
        return std::max(2, restore_build_concurrency);
    }
}

} // namespace

using PointerHelper = PointerTypeColumnHelper<sizeof(void *)>;
const std::string Join::match_helper_prefix = "__left-semi-join-match-helper";
const DataTypePtr Join::match_helper_type = makeNullable(std::make_shared<DataTypeInt8>());
const String Join::flag_mapped_entry_helper_prefix = "__flag-mapped-entry-match-helper";
const DataTypePtr Join::flag_mapped_entry_helper_type = std::make_shared<PointerHelper::DataType>();
#ifdef DBMS_PUBLIC_GTEST
const size_t MAX_RESTORE_ROUND_IN_GTEST = 2;
#endif


Join::Join(
    const Names & key_names_left_,
    const Names & key_names_right_,
    ASTTableJoin::Kind kind_,
    const String & req_id,
    size_t fine_grained_shuffle_count_,
    size_t max_bytes_before_external_join,
    const SpillConfig & build_spill_config,
    const SpillConfig & probe_spill_config,
    const RestoreConfig & restore_config_,
    const NamesAndTypes & output_columns_,
    const RegisterOperatorSpillContext & register_operator_spill_context_,
    AutoSpillTrigger * auto_spill_trigger_,
    const TiDB::TiDBCollators & collators_,
    const JoinNonEqualConditions & non_equal_conditions_,
    size_t max_block_size_,
    size_t shallow_copy_cross_probe_threshold_,
    const String & match_helper_name_,
    const String & flag_mapped_entry_helper_name_,
    size_t probe_cache_column_threshold_,
    bool is_test_,
    const std::vector<RuntimeFilterPtr> & runtime_filter_list_)
    : restore_config(restore_config_)
    , match_helper_name(match_helper_name_)
    , flag_mapped_entry_helper_name(flag_mapped_entry_helper_name_)
    , wait_build_finished_future(std::make_shared<OneTimeNotifyFuture>())
    , wait_probe_finished_future(std::make_shared<OneTimeNotifyFuture>())
    , kind(kind_)
    , join_req_id(req_id)
    , may_probe_side_expanded_after_join(mayProbeSideExpandedAfterJoin(kind))
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , build_concurrency(0)
    , active_build_threads(0)
    , probe_concurrency(0)
    , active_probe_threads(0)
    , collators(collators_)
    , non_equal_conditions(non_equal_conditions_)
    , max_block_size(max_block_size_)
    , runtime_filter_list(runtime_filter_list_)
    , register_operator_spill_context(register_operator_spill_context_)
    , auto_spill_trigger(auto_spill_trigger_)
    , shallow_copy_cross_probe_threshold(
          shallow_copy_cross_probe_threshold_ > 0 ? shallow_copy_cross_probe_threshold_
                                                  : std::max(1, max_block_size / 10))
    , probe_cache_column_threshold(probe_cache_column_threshold_)
    , output_columns(output_columns_)
    , is_test(is_test_)
    , log(Logger::get(
          restore_config.restore_round == 0 ? join_req_id
                                            : fmt::format(
                                                "{}_round_{}_part_{}",
                                                join_req_id,
                                                restore_config.restore_round,
                                                restore_config.restore_partition_id)))
    , enable_fine_grained_shuffle(fine_grained_shuffle_count_ > 0)
    , fine_grained_shuffle_count(fine_grained_shuffle_count_)
{
    has_other_condition = non_equal_conditions.other_cond_expr != nullptr;
    bool is_semi = isSemiFamily(kind) || isLeftOuterSemiFamily(kind) || isNullAwareSemiFamily(kind);
    if (is_semi && !has_other_condition)
        strictness = ASTTableJoin::Strictness::Any;
    else
        strictness = ASTTableJoin::Strictness::All;

    if (is_semi)
        probe_cache_column_threshold = 0;

    if unlikely (kind == ASTTableJoin::Kind::Cross_RightOuter)
        throw Exception("Cross right outer join should be converted to cross Left outer join during compile");
    RUNTIME_CHECK(!(isNecessaryKindToUseRowFlaggedHashMap(kind) && strictness == ASTTableJoin::Strictness::Any));
    const char * err = non_equal_conditions.validate(kind);
    if unlikely (err != nullptr)
        throw Exception("Validate join conditions error: {}" + String(err));

    hash_join_spill_context = std::make_shared<HashJoinSpillContext>(
        build_spill_config,
        probe_spill_config,
        max_bytes_before_external_join,
        log);
    size_t max_restore_round = 4;
#ifdef DBMS_PUBLIC_GTEST
    max_restore_round = MAX_RESTORE_ROUND_IN_GTEST;
#endif

    if (hash_join_spill_context->supportSpill() && restore_config.restore_round >= max_restore_round)
    {
        LOG_WARNING(log, fmt::format("restore round reach to {}, spilling will be disabled.", max_restore_round));
        hash_join_spill_context->disableSpill();
    }
    output_block = Block(output_columns);

    LOG_DEBUG(
        log,
        "FineGrainedShuffle flag {}, stream count {}",
        enable_fine_grained_shuffle,
        fine_grained_shuffle_count);
}

void Join::meetError(const String & error_message_)
{
    std::unique_lock lock(build_probe_mutex);
    meetErrorImpl(error_message_, lock);
}

void Join::meetErrorImpl(const String & error_message_, std::unique_lock<std::mutex> &)
{
    if (meet_error)
        return;
    meet_error = true;
    error_message = error_message_.empty() ? "Join meet error" : error_message_;
    build_cv.notify_all();
    probe_cv.notify_all();
    // wait_build/probe_finished_future does not need to call finish here
    // because it is called in PipelineExecutorContext.
}

size_t Join::getTotalRowCount() const
{
    std::shared_lock rw_lock(rwlock);

    size_t res = 0;

    if (join_map_method == JoinMapMethod::CROSS)
    {
        res = total_input_build_rows;
    }
    else
    {
        for (const auto & partition : partitions)
        {
            auto partition_lock = partition->lockPartition();
            res += partition->getRowCount();
        }
    }

    return res;
}

size_t Join::getTotalHashTableAndPoolByteCount()
{
    if (join_map_method == JoinMapMethod::CROSS)
        return 0;
    size_t res = 0;
    for (const auto & partition : partitions)
        res += partition->getHashMapAndPoolMemoryUsage();
    return res;
}

size_t Join::getTotalByteCount()
{
    size_t res = 0;
    if (isEnableSpill())
    {
        for (const auto & join_partition : partitions)
            res += join_partition->getMemoryUsage();
    }
    else
    {
        if (join_map_method == JoinMapMethod::CROSS)
        {
            for (const auto & block : blocks)
                res += block.bytes();
        }
        else
        {
            for (const auto & block : original_blocks)
                res += block.bytes();

            for (const auto & partition : partitions)
            {
                /// note the return value might not be accurate since it does not use lock, but should be enough for current usage
                res += partition->getHashMapAndPoolByteCount();
            }
        }
    }
    if (peak_build_bytes_usage)
        peak_build_bytes_usage = res;

    return res;
}

size_t Join::getPeakBuildBytesUsage()
{
    /// call `getTotalByteCount` first to make sure peak_build_bytes_usage has a meaningful value
    getTotalByteCount();
    return peak_build_bytes_usage;
}

void Join::setBuildConcurrencyAndInitJoinPartition(size_t build_concurrency_)
{
    if (unlikely(build_concurrency > 0))
        throw Exception(
            "Logical error: `setBuildConcurrencyAndInitJoinPartition` shouldn't be called more than once",
            ErrorCodes::LOGICAL_ERROR);
    /// do not set active_build_threads because in compile stage, `joinBlock` will be called to get generate header, if active_build_threads
    /// is set here, `joinBlock` will hang when used to get header
    build_concurrency = std::max(1, build_concurrency_);

    partitions.reserve(build_concurrency);
    for (size_t i = 0; i < getBuildConcurrency(); ++i)
    {
        partitions.push_back(std::make_unique<JoinPartition>(
            join_map_method,
            kind,
            strictness,
            i,
            max_block_size,
            hash_join_spill_context,
            log,
            has_other_condition));
    }
}

void Join::setRightSampleBlock(const Block & block)
{
    right_sample_block = materializeBlock(block);

    /// Copy from `right_sample_block` key columns to `right_sample_block_only_keys`, keeping the order.
    for (size_t pos = 0; pos < right_sample_block.columns(); ++pos)
    {
        const auto & name = right_sample_block.getByPosition(pos).name;
        if (key_names_right.end() != std::find(key_names_right.begin(), key_names_right.end(), name))
            right_sample_block_only_keys.insert(right_sample_block.getByPosition(pos));
    }

    size_t num_columns_to_add = right_sample_block.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = right_sample_block.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(right_sample_block.getByPosition(i));

    if (isLeftOuterSemiFamily(kind))
        right_sample_block.insert(ColumnWithTypeAndName(Join::match_helper_type, match_helper_name));
}

std::shared_ptr<Join> Join::createRestoreJoin(size_t max_bytes_before_external_join_, size_t restore_partition_id)
{
    auto ret = std::make_shared<Join>(
        key_names_left,
        key_names_right,
        kind,
        join_req_id,
        /// restore join never enable fine grained shuffle
        0,
        max_bytes_before_external_join_,
        hash_join_spill_context->createBuildSpillConfig(
            fmt::format("{}_{}_build", join_req_id, restore_config.restore_round + 1)),
        hash_join_spill_context->createProbeSpillConfig(
            fmt::format("{}_{}_probe", join_req_id, restore_config.restore_round + 1)),
        RestoreConfig{restore_config.join_restore_concurrency, restore_config.restore_round + 1, restore_partition_id},
        output_columns,
        register_operator_spill_context,
        auto_spill_trigger,
        collators,
        non_equal_conditions,
        max_block_size,
        shallow_copy_cross_probe_threshold,
        match_helper_name,
        flag_mapped_entry_helper_name,
        probe_cache_column_threshold,
        is_test);
    /// init output names after finalize, the restored join don't need to finalize
    ret->output_columns_after_finalize = output_columns_after_finalize;
    ret->output_column_names_set_after_finalize = output_column_names_set_after_finalize;
    ret->output_columns_names_set_for_other_condition_after_finalize
        = output_columns_names_set_for_other_condition_after_finalize;
    ret->required_columns = required_columns;
    ret->output_block_after_finalize = output_block_after_finalize;
    ret->finalized = true;
    return ret;
}

void Join::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    std::unique_lock lock(rwlock);
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;
    join_map_method = chooseJoinMapMethod(getKeyColumns(key_names_right, sample_block), key_sizes, collators);
    build_sample_block = sample_block;
    setBuildConcurrencyAndInitJoinPartition(build_concurrency_);
    hash_join_spill_context->init(build_concurrency);
    if (hash_join_spill_context->supportSpill())
    {
        if (join_map_method == JoinMapMethod::CROSS)
        {
            /// todo support spill for cross join
            hash_join_spill_context->disableSpill();
            LOG_WARNING(log, "Join does not support spill, reason: cross join spill is not supported");
        }
        if (isNullAwareSemiFamily(kind))
        {
            hash_join_spill_context->disableSpill();
            LOG_WARNING(log, "Join does not support spill, reason: null aware join spill is not supported");
        }
    }
    if (register_operator_spill_context != nullptr)
        register_operator_spill_context(hash_join_spill_context);
    if (hash_join_spill_context->isSpillEnabled())
    {
        hash_join_spill_context->buildBuildSpiller(build_sample_block);
    }
    for (auto & partition : partitions)
        partition->setResizeCallbackIfNeeded();
    setRightSampleBlock(sample_block);
    build_side_marked_spilled_data.resize(build_concurrency);
}

void Join::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    std::unique_lock lock(rwlock);
    setProbeConcurrency(probe_concurrency_);
    probe_sample_block = sample_block;
    if (hash_join_spill_context->isSpillEnabled())
        hash_join_spill_context->buildProbeSpiller(probe_sample_block);
    probe_side_marked_spilled_data.resize(probe_concurrency);
}

Join::MarkedSpillData & Join::getBuildSideMarkedSpillData(size_t stream_index)
{
    assert(stream_index < build_side_marked_spilled_data.size());
    return build_side_marked_spilled_data[stream_index];
}

const Join::MarkedSpillData & Join::getBuildSideMarkedSpillData(size_t stream_index) const
{
    assert(stream_index < build_side_marked_spilled_data.size());
    return build_side_marked_spilled_data[stream_index];
}

bool Join::hasBuildSideMarkedSpillData(size_t stream_index) const
{
    std::shared_lock lock(rwlock);
    return !getBuildSideMarkedSpillData(stream_index).empty();
}

void Join::flushBuildSideMarkedSpillData(size_t stream_index)
{
    std::shared_lock lock(rwlock);
    auto & data = getBuildSideMarkedSpillData(stream_index);
    assert(!data.empty());
    for (auto & [part_id, blocks_to_spill] : data)
        spillBuildSideBlocks(part_id, std::move(blocks_to_spill));
    data.clear();
}

Join::MarkedSpillData & Join::getProbeSideMarkedSpillData(size_t stream_index)
{
    assert(stream_index < probe_side_marked_spilled_data.size());
    return probe_side_marked_spilled_data[stream_index];
}

const Join::MarkedSpillData & Join::getProbeSideMarkedSpillData(size_t stream_index) const
{
    assert(stream_index < probe_side_marked_spilled_data.size());
    return probe_side_marked_spilled_data[stream_index];
}

bool Join::hasProbeSideMarkedSpillData(size_t stream_index) const
{
    std::shared_lock lock(rwlock);
    return !getProbeSideMarkedSpillData(stream_index).empty();
}

void Join::flushProbeSideMarkedSpillData(size_t stream_index)
{
    std::shared_lock lock(rwlock);
    auto & data = getProbeSideMarkedSpillData(stream_index);
    assert(!data.empty());
    for (auto & [part_id, blocks_to_spill] : data)
        spillProbeSideBlocks(part_id, std::move(blocks_to_spill));
    data.clear();
}

void Join::checkAndMarkPartitionSpilledIfNeeded(size_t stream_index)
{
    /// todo need to check more partitions if partition_size is not equal to total stream size
    size_t partition_index = stream_index;
    const auto & join_partition = partitions[partition_index];
    auto partition_lock = join_partition->tryLockPartition();
    if (partition_lock)
        checkAndMarkPartitionSpilledIfNeededInternal(*join_partition, partition_lock, partition_index, stream_index);
    /// if someone already hold the lock, it will check the spill
}

void Join::checkAndMarkPartitionSpilledIfNeededInternal(
    JoinPartition & join_partition,
    std::unique_lock<std::mutex> & partition_lock,
    size_t partition_index,
    size_t stream_index)
{
    auto ret
        = hash_join_spill_context->updatePartitionRevocableMemory(partition_index, join_partition.revocableBytes());
    if (ret)
    {
        if (!hash_join_spill_context->isPartitionSpilled(partition_index))
        {
            /// first spill
            hash_join_spill_context->markPartitionSpilled(partition_index);
            join_partition.releasePartitionPoolAndHashMap(partition_lock);
        }
        auto blocks_to_spill = join_partition.trySpillBuildPartition(partition_lock);
        markBuildSideSpillData(partition_index, std::move(blocks_to_spill), stream_index);
    }
}

/// the block should be valid.
void Join::insertFromBlock(const Block & block, size_t stream_index)
{
    if unlikely (block.rows() == 0)
        return;
    std::shared_lock lock(rwlock);
    assert(stream_index < getBuildConcurrency());
    total_input_build_rows += block.rows();

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (!isEnableSpill())
    {
        Block * stored_block = nullptr;
        {
            std::lock_guard lk(blocks_lock);
            blocks.push_back(block);
            stored_block = &blocks.back();
            original_blocks.push_back(block);
        }
        insertFromBlockInternal(stored_block, stream_index);
    }
    else
    {
        Blocks dispatch_blocks;
        size_t block_size_to_be_inserted = build_concurrency;
        if (enable_fine_grained_shuffle)
        {
            /// fine grain shuffle does not need dispatch
            dispatch_blocks.resize(build_concurrency, {});
            dispatch_blocks[stream_index] = block;
            block_size_to_be_inserted = 1;
        }
        else
        {
            dispatch_blocks = dispatchBlock(key_names_right, block);
        }
        assert(dispatch_blocks.size() == build_concurrency);

        for (size_t j = stream_index; j < block_size_to_be_inserted + stream_index; ++j)
        {
            size_t i = j % build_concurrency;
            if (!dispatch_blocks[i].rows())
            {
                continue;
            }
            {
                const auto & join_partition = partitions[i];
                auto partition_lock = join_partition->lockPartition();
                join_partition->insertBlockForBuild(std::move(dispatch_blocks[i]));
                /// to release memory before insert if already marked spill
                checkAndMarkPartitionSpilledIfNeededInternal(*join_partition, partition_lock, i, stream_index);
                if (!hash_join_spill_context->isPartitionSpilled(i))
                {
                    bool meet_resize_exception = false;
                    try
                    {
                        insertFromBlockInternal(join_partition->getLastBuildBlock(), i);
                        join_partition->updateHashMapAndPoolMemoryUsage();
                    }
                    catch (ResizeException &)
                    {
                        meet_resize_exception = true;
                        LOG_DEBUG(log, "Meet resize exception when insert into partition {}", i);
                    }
                    /// double check here to release memory
                    checkAndMarkPartitionSpilledIfNeededInternal(*join_partition, partition_lock, i, stream_index);
                    if (meet_resize_exception)
                        RUNTIME_CHECK_MSG(
                            hash_join_spill_context->isPartitionSpilled(i),
                            "resize exception must trigger partition to spill");
                }
            }
            if (auto_spill_trigger != nullptr)
                auto_spill_trigger->triggerAutoSpill();
        }
        if (!hash_join_spill_context->isInAutoSpillMode())
            spillMostMemoryUsedPartitionIfNeed(stream_index);
        if (log->is(Poco::Message::PRIO_DEBUG))
        {
            auto total_bytes = getTotalByteCount();
            if (total_bytes > 100 * 1024 * 1024)
                LOG_DEBUG(
                    log,
                    fmt::format(
                        "all bytes used after one insert: {}, hash table and pool size: {}",
                        getTotalByteCount(),
                        getTotalHashTableAndPoolByteCount()));
        }
    }
}

bool Join::isEnableSpill() const
{
    return hash_join_spill_context->isSpillEnabled();
}

bool Join::isRestoreJoin() const
{
    return restore_config.restore_round > 0;
}

void Join::insertFromBlockInternal(Block * stored_block, size_t stream_index)
{
    Block key_block;
    if (!runtime_filter_list.empty())
    {
        /// save the key column for runtime filter
        for (const auto & name : key_names_right)
            key_block.insert(stored_block->getByName(name));
    }

    const Block & block = *stored_block;

    size_t rows = block.rows();

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_right);

    if (isNullAwareSemiFamily(kind))
    {
        if (rows > 0 && right_table_is_empty.load(std::memory_order_acquire))
            right_table_is_empty.store(false, std::memory_order_release);

        if (strictness == ASTTableJoin::Strictness::Any)
        {
            if (!right_has_all_key_null_row.load(std::memory_order_acquire))
            {
                /// Note that `extractAllKeyNullMap` must be done before `extractNestedColumnsAndNullMap`
                /// because `extractNestedColumnsAndNullMap` will change the nullable column to its nested column.
                ColumnPtr all_key_null_map_holder;
                ConstNullMapPtr all_key_null_map{};
                extractAllKeyNullMap(key_columns, all_key_null_map_holder, all_key_null_map);

                if (all_key_null_map)
                {
                    for (UInt8 is_null : *all_key_null_map)
                    {
                        if (is_null)
                        {
                            right_has_all_key_null_row.store(true, std::memory_order_release);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// Reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, non_equal_conditions.right_filter_column, null_map_holder, null_map);

    size_t size = stored_block->columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < size; ++i)
    {
        ColumnPtr col = stored_block->safeGetByPosition(i).column;
        if (ColumnPtr converted = col->convertToFullColumnIfConst())
            stored_block->safeGetByPosition(i).column = converted;
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
    {
        for (size_t i = 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    bool enable_join_spill = hash_join_spill_context->isSpillEnabled();

    if (!isCrossJoin(kind))
    {
        if (enable_join_spill)
            assert(partitions[stream_index]->getPartitionPool() != nullptr);
        /// Fill the hash table.
        JoinPartition::insertBlockIntoMaps(
            partitions,
            rows,
            key_columns,
            key_sizes,
            collators,
            stored_block,
            null_map,
            stream_index,
            getBuildConcurrency(),
            enable_fine_grained_shuffle,
            enable_join_spill,
            probe_cache_column_threshold);
    }

    // generator in runtime filter
    if (!runtime_filter_list.empty())
        generateRuntimeFilterValues(key_block);
}

void Join::generateRuntimeFilterValues(const Block & block)
{
    LOG_TRACE(log, "begin to generate rf values for one block in join id, block rows:{}", block.rows());
    for (const auto & rf : runtime_filter_list)
    {
        auto column_with_type_and_name = block.getByName(rf->getSourceColumnName());
        LOG_TRACE(log, "update rf values in join, values size:{}", column_with_type_and_name.column->size());
        rf->updateValues(column_with_type_and_name, log);
    }
}

void Join::finalizeRuntimeFilter()
{
    for (const auto & rf : runtime_filter_list)
    {
        rf->finalize(log);
    }
}

void Join::cancelRuntimeFilter(const String & reason)
{
    for (const auto & rf : runtime_filter_list)
    {
        rf->cancel(log, reason);
    }
}

namespace
{
void mergeNullAndFilterResult(
    Block & block,
    ColumnVector<UInt8>::Container & filter_column,
    const String & filter_column_name,
    bool null_as_true)
{
    if (filter_column_name.empty())
        return;
    ColumnPtr current_filter_column = block.getByName(filter_column_name).column;
    auto [filter_vec, nullmap_vec] = getDataAndNullMapVectorFromFilterColumn(current_filter_column);
    if (nullmap_vec != nullptr)
    {
        if (filter_column.empty())
        {
            filter_column.insert(nullmap_vec->begin(), nullmap_vec->end());
            if (null_as_true)
            {
                for (size_t i = 0; i < nullmap_vec->size(); ++i)
                    filter_column[i] = filter_column[i] || (*filter_vec)[i];
            }
            else
            {
                for (size_t i = 0; i < nullmap_vec->size(); ++i)
                    filter_column[i] = !filter_column[i] && (*filter_vec)[i];
            }
        }
        else
        {
            if (null_as_true)
            {
                for (size_t i = 0; i < nullmap_vec->size(); ++i)
                    filter_column[i] = filter_column[i] && ((*nullmap_vec)[i] || (*filter_vec)[i]);
            }
            else
            {
                for (size_t i = 0; i < nullmap_vec->size(); ++i)
                    filter_column[i] = filter_column[i] && !(*nullmap_vec)[i] && (*filter_vec)[i];
            }
        }
    }
    else
    {
        if (filter_column.empty())
        {
            filter_column.insert(filter_vec->begin(), filter_vec->end());
        }
        else
        {
            for (size_t i = 0; i < filter_vec->size(); ++i)
                filter_column[i] = filter_column[i] && (*filter_vec)[i];
        }
    }
}
void applyNullToNotMatchedRows(Block & block, const Block & right_columns, const ColumnUInt8 & filter_column)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        if (right_columns.has(column.name))
        {
            auto full_column
                = column.column->isColumnConst() ? column.column->convertToFullColumnIfConst() : column.column;
            RUNTIME_CHECK_MSG(full_column->isColumnNullable(), "the right table column for left join must be nullable");
            auto current_column = full_column;
            auto result_column = (*std::move(current_column)).mutate();
            static_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(filter_column);
            column.column = std::move(result_column);
        }
    }
}
} // namespace

/**
 * handle other join conditions
 * Join Kind/Strictness               ALL                 ANY
 *     INNER                    TiDB inner join      TiDB semi join
 *     LEFT                     TiDB left join       should not happen
 *     RIGHT                    TiDB right join      should not happen
 *     RIGHT_SEMI/ANTI          TiDB semi/anti join  should not happen
 *     ANTI                     should not happen    TiDB anti semi join
 * @param block
 * @param offsets_to_replicate
 * @param left_table_columns
 * @param right_table_columns
 */
void Join::handleOtherConditions(Block & block, IColumn::Filter * anti_filter, IColumn::Offsets * offsets_to_replicate)
    const
{
    /// save block_rows because block.rows() returns the first column's size, after other_cond_expr->execute(block),
    /// some column maybe removed, and the first column maybe the match_helper_column which does not have the same size
    /// as the other columns
    auto block_rows = block.rows();
    non_equal_conditions.other_cond_expr->execute(block);

    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);

    ColumnUInt8::Container row_filter(block_rows, 0);

    auto erase_useless_column = [&](Block & input_block) {
        for (size_t i = 0; i < input_block.columns();)
        {
            auto & col_name = input_block.getByPosition(i).name;
            if ((!flag_mapped_entry_helper_name.empty() && col_name == flag_mapped_entry_helper_name)
                || output_column_names_set_after_finalize.contains(col_name))
                ++i;
            else
                input_block.erase(i);
        }
    };

    if (isLeftOuterSemiFamily(kind))
    {
        if (filter.size() != block_rows)
        {
            assert(filter.empty());
            filter.assign(block_rows, static_cast<UInt8>(1));
        }
        const auto * old_match_nullable
            = checkAndGetColumn<ColumnNullable>(block.getByName(match_helper_name).column.get());
        const auto & old_match_vec
            = static_cast<const ColumnVector<Int8> *>(old_match_nullable->getNestedColumnPtr().get())->getData();

        {
            /// we assume there is no null value in the `match-helper` column after adder<>().
            if (!mem_utils::memoryIsZero(
                    old_match_nullable->getNullMapData().data(),
                    old_match_nullable->getNullMapData().size()))
                throw Exception("T here shouldn't be null before merging other conditions.", ErrorCodes::LOGICAL_ERROR);
        }

        const auto rows = offsets_to_replicate->size();
        if (old_match_vec.size() != rows)
            throw Exception(
                "Size of column match-helper must be equal to column size of left block.",
                ErrorCodes::LOGICAL_ERROR);

        auto match_col = ColumnInt8::create(rows, 0);
        auto & match_vec = match_col->getData();
        auto match_nullmap = ColumnUInt8::create(rows, 0);
        auto & match_nullmap_vec = match_nullmap->getData();

        /// nullmap and data of `other_eq_filter_from_in_column`.
        const ColumnUInt8::Container *eq_in_vec = nullptr, *eq_in_nullmap = nullptr;
        ColumnPtr eq_in_column = nullptr;
        if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
        {
            eq_in_column = block.getByName(non_equal_conditions.other_eq_cond_from_in_name).column;
            auto data_and_null_map_vec = getDataAndNullMapVectorFromFilterColumn(eq_in_column);
            eq_in_vec = data_and_null_map_vec.first;
            eq_in_nullmap = data_and_null_map_vec.second;
        }

        /// for (anti)leftOuterSemi join, we should keep only one row for each original row of left table.
        /// and because it is semi join, we needn't save columns of right table, so we just keep the first replica.
        for (size_t i = 0; i < offsets_to_replicate->size(); ++i)
        {
            size_t prev_offset = i > 0 ? (*offsets_to_replicate)[i - 1] : 0;
            size_t current_offset = (*offsets_to_replicate)[i];

            row_filter[prev_offset] = 1;
            if (old_match_vec[i] == 0)
                continue;

            /// fill match_vec and match_nullmap_vec
            /// if there is `1` in filter, match_vec is 1.
            /// if there is `null` in eq_in_nullmap, match_nullmap_vec is 1.
            /// else, match_vec is 0.

            bool has_row_matched = false, has_row_null = false;
            for (size_t index = prev_offset; index < current_offset; index++)
            {
                if (!filter[index])
                    continue;
                if (eq_in_nullmap && (*eq_in_nullmap)[index])
                    has_row_null = true;
                else if (!eq_in_vec || (*eq_in_vec)[index])
                {
                    has_row_matched = true;
                    break;
                }
            }

            if (has_row_matched)
                match_vec[i] = 1;
            else if (has_row_null)
                match_nullmap_vec[i] = 1;
        }

        erase_useless_column(block);
        auto helper_pos = block.getPositionByName(match_helper_name);
        for (size_t i = 0; i < block.columns(); ++i)
            if (i != helper_pos)
                block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        block.safeGetByPosition(helper_pos).column
            = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
        return;
    }

    /// other_eq_filter_from_in_column is used in anti semi join:
    /// if there is a row that return null or false for other_condition, then for anti semi join, this row should be returned.
    /// otherwise, it will check other_eq_filter_from_in_column, if other_eq_filter_from_in_column return false, this row should
    /// be returned, if other_eq_filter_from_in_column return true or null this row should not be returned.
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));
    assert(block_rows == filter.size());

    if (isInnerJoin(kind) || isNecessaryKindToUseRowFlaggedHashMap(kind))
    {
        erase_useless_column(block);
        /// inner | rightSemi | rightAnti | rightOuter join,  just use other_filter_column to filter result
        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);
        return;
    }

    bool is_semi_family = isSemiFamily(kind) || isLeftOuterSemiFamily(kind);
    for (size_t i = 0, prev_offset = 0; i < offsets_to_replicate->size(); ++i)
    {
        size_t current_offset = (*offsets_to_replicate)[i];
        bool has_row_kept = false;
        for (size_t index = prev_offset; index < current_offset; index++)
        {
            if (is_semi_family)
            {
                /// for semi/anti join, at most one row is kept
                row_filter[index] = !has_row_kept && filter[index];
            }
            else
            {
                row_filter[index] = filter[index];
            }
            if (row_filter[index])
                has_row_kept = true;
        }
        if (prev_offset < current_offset)
        {
            /// for outer join, at least one row must be kept
            if (isLeftOuterJoin(kind) && !has_row_kept)
                row_filter[prev_offset] = 1;
            if (isAntiJoin(kind))
            {
                if (has_row_kept && !(*anti_filter)[i])
                    /// anti_filter=false means equal condition is matched,
                    /// has_row_kept=true means other condition is matched,
                    /// for anti join, we should not return any rows when the both conditions are matched.
                    for (size_t index = prev_offset; index < current_offset; index++)
                        row_filter[index] = 0;
                else
                    /// when there is a condition not matched, we should return this row.
                    row_filter[prev_offset] = 1;
            }
        }
        prev_offset = current_offset;
    }
    erase_useless_column(block);
    if (isLeftOuterJoin(kind))
    {
        /// for left join, convert right column to null if not joined
        applyNullToNotMatchedRows(block, right_sample_block, *filter_column);
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    if (is_semi_family)
    {
        /// for semi/anti join, filter out not matched rows
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}

// Now this function only support cross join, todo support hash join
void Join::handleOtherConditionsForOneProbeRow(Block & block, ProbeProcessInfo & probe_process_info) const
{
    assert(kind != ASTTableJoin::Kind::Cross_RightOuter);
    /// save block_rows because block.rows() returns the first column's size, after other_cond_expr->execute(block),
    /// some column maybe removed, and the first column maybe the match_helper_column which does not have the same size
    /// as the other columns
    auto block_rows = block.rows();
    /// inside this function, we can ensure that
    /// 1. probe_process_info.offsets_to_replicate.size() == 1
    /// 2. probe_process_info.offsets_to_replicate[0] == block_rows
    /// 3. for anti semi join: probe_process_info.filter[0] == 1
    /// 4. for left outer semi join: match_helper_column[0] == 1
    assert(probe_process_info.offsets_to_replicate->size() == 1);
    assert((*probe_process_info.offsets_to_replicate)[0] == block_rows);

    auto erase_useless_column = [&](Block & input_block) {
        for (size_t i = 0; i < input_block.columns();)
        {
            auto & col_name = input_block.getByPosition(i).name;
            if ((!flag_mapped_entry_helper_name.empty() && col_name == flag_mapped_entry_helper_name)
                || output_column_names_set_after_finalize.contains(col_name))
                ++i;
            else
                input_block.erase(i);
        }
    };

    non_equal_conditions.other_cond_expr->execute(block);
    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);
    UInt64 matched_row_count_in_current_block = 0;
    if (isLeftOuterSemiFamily(kind) && !non_equal_conditions.other_eq_cond_from_in_name.empty())
    {
        if (filter.size() != block_rows)
        {
            assert(filter.empty());
            filter.assign(block_rows, static_cast<UInt8>(1));
        }
        assert(probe_process_info.cross_join_data->has_row_matched == false);
        ColumnPtr eq_in_column = block.getByName(non_equal_conditions.other_eq_cond_from_in_name).column;
        auto [eq_in_vec, eq_in_nullmap] = getDataAndNullMapVectorFromFilterColumn(eq_in_column);
        for (size_t i = 0; i < block_rows; ++i)
        {
            if (!filter[i])
                continue;
            if (eq_in_nullmap && (*eq_in_nullmap)[i])
                probe_process_info.cross_join_data->has_row_null = true;
            else if ((*eq_in_vec)[i])
            {
                probe_process_info.cross_join_data->has_row_matched = true;
                break;
            }
        }
    }
    else
    {
        mergeNullAndFilterResult(block, filter, non_equal_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));
        assert(filter.size() == block_rows);
        matched_row_count_in_current_block = countBytesInFilter(filter);
        probe_process_info.cross_join_data->has_row_matched |= matched_row_count_in_current_block != 0;
    }
    erase_useless_column(block);
    /// case 1, inner join
    if (kind == ASTTableJoin::Kind::Cross)
    {
        if (matched_row_count_in_current_block > 0)
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column
                    = block.safeGetByPosition(i).column->filter(filter, matched_row_count_in_current_block);
        }
        else
        {
            block = block.cloneEmpty();
        }
        return;
    }
    /// case 2, left outer join
    if (kind == ASTTableJoin::Kind::Cross_LeftOuter)
    {
        if (matched_row_count_in_current_block > 0)
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column
                    = block.safeGetByPosition(i).column->filter(filter, matched_row_count_in_current_block);
        }
        else if (probe_process_info.isCurrentProbeRowFinished() && !probe_process_info.cross_join_data->has_row_matched)
        {
            /// no matched rows for current row, return the un-matched result
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
            filter.resize(1);
            applyNullToNotMatchedRows(block, right_sample_block, *filter_column);
        }
        else
        {
            block = block.cloneEmpty();
        }
        return;
    }
    /// case 3, semi join
    if (kind == ASTTableJoin::Kind::Cross_Semi)
    {
        if (probe_process_info.cross_join_data->has_row_matched)
        {
            /// has matched rows, return the first row, and set the current row probe done
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
            probe_process_info.finishCurrentProbeRow();
        }
        else
        {
            /// no matched rows, just return an empty block
            block = block.cloneEmpty();
        }
        return;
    }
    /// case 4, anti join
    if (kind == ASTTableJoin::Kind::Cross_Anti)
    {
        if (probe_process_info.cross_join_data->has_row_matched)
        {
            block = block.cloneEmpty();
            probe_process_info.finishCurrentProbeRow();
        }
        else if (probe_process_info.isCurrentProbeRowFinished())
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
        }
        else
        {
            block = block.cloneEmpty();
        }
        return;
    }
    /// case 5, left outer semi join
    if (isLeftOuterSemiFamily(kind))
    {
        if (probe_process_info.cross_join_data->has_row_matched || probe_process_info.isCurrentProbeRowFinished())
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
            auto match_col = ColumnInt8::create(1, 0);
            auto & match_vec = match_col->getData();
            auto match_nullmap = ColumnUInt8::create(1, 0);
            auto & match_nullmap_vec = match_nullmap->getData();
            if (probe_process_info.cross_join_data->has_row_matched)
                match_vec[0] = 1;
            else if (probe_process_info.cross_join_data->has_row_null)
                match_nullmap_vec[0] = 1;
            block.getByName(match_helper_name).column
                = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
            probe_process_info.finishCurrentProbeRow();
        }
        else
        {
            block = block.cloneEmpty();
        }
        return;
    }
    throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}

Block Join::doJoinBlockHash(ProbeProcessInfo & probe_process_info, const JoinBuildInfo & join_build_info) const
{
    assert(probe_process_info.prepare_for_probe_done);
    probe_process_info.updateStartRow<false>();
    /// this makes a copy of `probe_process_info.block`
    Block block = probe_process_info.block;
    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;
    for (size_t pos = 0; pos < block.columns();)
    {
        if (probe_output_name_set.find(block.getByPosition(pos).name) == probe_output_name_set.end())
            block.erase(pos);
        else
            ++pos;
    }

    size_t existing_columns = block.columns();

    /// Add new columns to the block.
    std::vector<size_t> num_columns_to_add;
    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        if (probe_output_name_set.find(right_sample_block.getByPosition(i).name) != probe_output_name_set.end())
            num_columns_to_add.push_back(i);
    }

    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add.size());

    std::vector<size_t> right_indexes;
    right_indexes.reserve(num_columns_to_add.size());

    size_t rows = probe_process_info.block.rows();
    for (const auto & index : num_columns_to_add)
    {
        const ColumnWithTypeAndName & src_column = right_sample_block.getByPosition(index);
        RUNTIME_CHECK_MSG(
            !block.has(src_column.name),
            "block from probe side has a column with the same name: {} as a column in right_sample_block",
            src_column.name);

        added_columns.push_back(src_column.column->cloneEmpty());
        if (src_column.type && src_column.type->haveMaximumSizeOfValue())
        {
            // todo figure out more accurate `rows`
            added_columns.back()->reserve(rows);
        }
        right_indexes.push_back(index);
    }

    bool use_row_flagged_hash_map = useRowFlaggedHashMap(kind, has_other_condition);
    if (use_row_flagged_hash_map)
    {
        /// For RightSemi/RightAnti join with other conditions, using this column to record hash entries that matches keys
        /// Note: this column will record map entry addresses, so should use it carefully and better limit its usage in this function only.
        // todo figure out more accurate `rows`
        added_columns.push_back(flag_mapped_entry_helper_type->createColumn());
        added_columns.back()->reserve(rows);
    }

    IColumn::Offset current_offset = 0;
    auto & offsets_to_replicate = probe_process_info.offsets_to_replicate;

    JoinPartition::probeBlock(
        partitions,
        rows,
        probe_process_info.hash_join_data->key_columns,
        key_sizes,
        added_columns,
        probe_process_info.null_map,
        current_offset,
        offsets_to_replicate,
        right_indexes,
        collators,
        join_build_info,
        probe_process_info);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);
    /// For RIGHT_SEMI/RIGHT_ANTI join without other conditions, hash table has been marked already, just return empty build table header
    if (isRightSemiFamily(kind) && !use_row_flagged_hash_map)
    {
        return right_sample_block;
    }

    for (size_t index = 0; index < num_columns_to_add.size(); ++index)
    {
        const ColumnWithTypeAndName & sample_col = right_sample_block.getByPosition(num_columns_to_add[index]);
        block.insert(ColumnWithTypeAndName(std::move(added_columns[index]), sample_col.type, sample_col.name));
    }
    if (use_row_flagged_hash_map)
        block.insert(ColumnWithTypeAndName(
            std::move(added_columns[num_columns_to_add.size()]),
            flag_mapped_entry_helper_type,
            flag_mapped_entry_helper_name));

    size_t process_rows = probe_process_info.end_row - probe_process_info.start_row;

    // if rows equal 0, we could ignore filter and offsets_to_replicate, and do not need to update start row.
    if (likely(rows != 0))
    {
        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        if (offsets_to_replicate)
        {
            for (size_t i = 0; i < existing_columns; ++i)
            {
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicateRange(
                    probe_process_info.start_row,
                    probe_process_info.end_row,
                    *offsets_to_replicate);
            }

            if (rows != process_rows)
            {
                offsets_to_replicate->assignFromSelf(probe_process_info.start_row, probe_process_info.end_row);
            }
        }
    }

    /// handle other conditions
    if (has_other_condition)
    {
        assert(offsets_to_replicate != nullptr);
        handleOtherConditions(block, nullptr, offsets_to_replicate.get());

        if (useRowFlaggedHashMap(kind, has_other_condition))
        {
            // set hash table used flag using SemiMapped column
            auto & mapped_column = block.getByName(flag_mapped_entry_helper_name).column;
            const auto & ptr_col = static_cast<const PointerHelper::ColumnType &>(*mapped_column);
            const auto & container = static_cast<const PointerHelper::ArrayType &>(ptr_col.getData());
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto ptr_value = container[i];
                auto * current = reinterpret_cast<RowRefListWithUsedFlag *>(ptr_value);
                current->setUsed();
            }

            if (isRightSemiFamily(kind))
            {
                // Return build table header for right semi/anti join
                block = right_sample_block;
            }
            else if (kind == ASTTableJoin::Kind::RightOuter)
            {
                block.erase(flag_mapped_entry_helper_name);
            }
        }
    }

    return block;
}

Block Join::removeUselessColumn(Block & block) const
{
    // cancelled
    if (!block)
        return block;

    // remove useless columns and adjust the order of columns
    Block projected_block;
    for (const auto & name_and_type : output_columns_after_finalize)
    {
        auto & column = block.getByName(name_and_type.name);
        projected_block.insert(std::move(column));
    }
    return projected_block;
}

Block Join::joinBlockHash(ProbeProcessInfo & probe_process_info) const
{
    std::vector<Block> result_blocks;
    size_t result_rows = 0;
    JoinBuildInfo join_build_info{
        enable_fine_grained_shuffle,
        fine_grained_shuffle_count,
        isEnableSpill(),
        hash_join_spill_context->isSpilled(),
        build_concurrency,
        restore_config.restore_round};
    probe_process_info.prepareForHashProbe(
        key_names_left,
        non_equal_conditions.left_filter_column,
        kind,
        strictness,
        join_build_info.needVirtualDispatchForProbeBlock(),
        collators,
        restore_config.restore_round);
    while (true)
    {
        if (is_cancelled())
            return {};
        auto block = doJoinBlockHash(probe_process_info, join_build_info);
        assert(block);
        block = removeUselessColumn(block);
        result_rows += block.rows();
        result_blocks.push_back(std::move(block));
        /// exit the while loop if
        /// 1. probe_process_info.all_rows_joined_finish is true, which means all the rows in current block is processed
        /// 2. the block may be expanded after join and result_rows exceeds the min_result_block_size
        if (probe_process_info.all_rows_joined_finish
            || (may_probe_side_expanded_after_join && result_rows >= probe_process_info.min_result_block_size))
            break;
    }
    assert(!result_blocks.empty());
    return vstackBlocks(std::move(result_blocks));
}

Block Join::doJoinBlockCross(ProbeProcessInfo & probe_process_info) const
{
    assert(probe_process_info.prepare_for_probe_done);
    if (cross_probe_mode == CrossProbeMode::DEEP_COPY_RIGHT_BLOCK)
    {
        probe_process_info.updateStartRow<false>();
        auto block = crossProbeBlockDeepCopyRightBlock(kind, strictness, probe_process_info, original_blocks);
        if (non_equal_conditions.other_cond_expr != nullptr)
        {
            assert(probe_process_info.offsets_to_replicate != nullptr);
            if (probe_process_info.end_row - probe_process_info.start_row != probe_process_info.block.rows())
            {
                probe_process_info.cutFilterAndOffsetVector(probe_process_info.start_row, probe_process_info.end_row);
            }
            handleOtherConditions(
                block,
                probe_process_info.filter.get(),
                probe_process_info.offsets_to_replicate.get());
        }
        return block;
    }
    else if (cross_probe_mode == CrossProbeMode::SHALLOW_COPY_RIGHT_BLOCK)
    {
        probe_process_info.updateStartRow<true>();
        auto [block, is_matched_rows]
            = crossProbeBlockShallowCopyRightBlock(kind, strictness, probe_process_info, original_blocks);
        if (is_matched_rows)
        {
            if (non_equal_conditions.other_cond_expr != nullptr)
            {
                probe_process_info.cutFilterAndOffsetVector(0, 1);
                /// for matched rows, each call to `doJoinBlockCross` only handle part of the probed data for one left row, the internal
                /// state is saved in `probe_process_info`
                handleOtherConditionsForOneProbeRow(block, probe_process_info);
            }
            for (size_t i = 0; i < probe_process_info.cross_join_data->left_column_index_in_left_block.size(); ++i)
            {
                auto & name = probe_process_info.block
                                  .getByPosition(probe_process_info.cross_join_data->left_column_index_in_left_block[i])
                                  .name;
                if (block.has(name))
                {
                    auto & column_and_name = block.getByName(name);
                    if (column_and_name.column->isColumnConst())
                        column_and_name.column = column_and_name.column->convertToFullColumnIfConst();
                }
            }
            if (isLeftOuterSemiFamily(kind))
            {
                auto & help_column = block.getByName(match_helper_name);
                if (help_column.column->isColumnConst())
                    help_column.column = help_column.column->convertToFullColumnIfConst();
            }
        }
        else if (non_equal_conditions.other_cond_expr != nullptr)
        {
            probe_process_info.cutFilterAndOffsetVector(0, block.rows());
            handleOtherConditions(
                block,
                probe_process_info.filter.get(),
                probe_process_info.offsets_to_replicate.get());
        }
        return block;
    }
    else
    {
        throw Exception(fmt::format("Unsupported cross probe mode: {}", magic_enum::enum_name(cross_probe_mode)));
    }
}

Block Join::joinBlockCross(ProbeProcessInfo & probe_process_info) const
{
    probe_process_info.prepareForCrossProbe(
        non_equal_conditions.left_filter_column,
        kind,
        strictness,
        right_sample_block,
        has_other_condition ? output_columns_names_set_for_other_condition_after_finalize
                            : output_column_names_set_after_finalize,
        right_rows_to_be_added_when_matched_for_cross_join,
        cross_probe_mode,
        blocks.size());

    std::vector<Block> result_blocks;
    size_t result_rows = 0;

    while (true)
    {
        if (is_cancelled())
            return {};
        Block block = doJoinBlockCross(probe_process_info);
        assert(block);
        block = removeUselessColumn(block);
        result_rows += block.rows();
        result_blocks.push_back(std::move(block));
        if (probe_process_info.all_rows_joined_finish
            || (may_probe_side_expanded_after_join && result_rows >= probe_process_info.min_result_block_size))
            break;
    }

    assert(!result_blocks.empty());
    return vstackBlocks(std::move(result_blocks));
}

void Join::checkTypes(const Block & block) const
{
    checkTypesOfKeys(block, right_sample_block_only_keys);
}

Block Join::joinBlockNullAwareSemi(ProbeProcessInfo & probe_process_info) const
{
    probe_process_info.prepareForNullAware(key_names_left, non_equal_conditions.left_filter_column);

    Block block{};
#define CALL(KIND, STRICTNESS, MAP) block = joinBlockNullAwareSemiImpl<KIND, STRICTNESS, MAP>(probe_process_info);


    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
    if (kind == NullAware_Anti && strictness == All)
        CALL(NullAware_Anti, All, MapsAll)
    else if (kind == NullAware_Anti && strictness == Any)
        CALL(NullAware_Anti, Any, MapsAny)
    else if (kind == NullAware_LeftOuterSemi && strictness == All)
        CALL(NullAware_LeftOuterSemi, All, MapsAll)
    else if (kind == NullAware_LeftOuterSemi && strictness == Any)
        CALL(NullAware_LeftOuterSemi, Any, MapsAny)
    else if (kind == NullAware_LeftOuterAnti && strictness == All)
        CALL(NullAware_LeftOuterAnti, All, MapsAll)
    else if (kind == NullAware_LeftOuterAnti && strictness == Any)
        CALL(NullAware_LeftOuterAnti, Any, MapsAny)
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
#undef CALL

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);

    /// Null-aware semi join never expand the left block, just handle the whole block at one time is enough
    probe_process_info.all_rows_joined_finish = true;

    return removeUselessColumn(block);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
Block Join::joinBlockNullAwareSemiImpl(const ProbeProcessInfo & probe_process_info) const
{
    size_t rows = probe_process_info.block.rows();
    std::vector<RowsNotInsertToMap *> null_rows(partitions.size(), nullptr);
    for (size_t i = 0; i < partitions.size(); ++i)
        null_rows[i] = partitions[i]->getRowsNotInsertedToMap();

    NALeftSideInfo left_side_info(
        probe_process_info.null_map,
        probe_process_info.null_aware_join_data->filter_map,
        probe_process_info.null_aware_join_data->all_key_null_map);
    NARightSideInfo right_side_info(
        right_has_all_key_null_row.load(std::memory_order_relaxed),
        right_table_is_empty.load(std::memory_order_relaxed),
        null_key_check_all_blocks_directly,
        null_rows);
    auto [res, res_list] = JoinPartition::probeBlockNullAwareSemi<KIND, STRICTNESS, Maps>(
        partitions,
        rows,
        probe_process_info.null_aware_join_data->key_columns,
        key_sizes,
        collators,
        left_side_info,
        right_side_info);

    RUNTIME_ASSERT(res.size() == rows, "SemiJoinResult size {} must be equal to block size {}", res.size(), rows);

    if (is_cancelled())
        return {};

    Block block{};
    for (size_t i = 0; i < probe_process_info.block.columns(); ++i)
    {
        const auto & column = probe_process_info.block.getByPosition(i);
        if (output_columns_names_set_for_other_condition_after_finalize.contains(column.name))
            block.insert(column);
    }

    size_t left_columns = block.columns();

    /// Add new columns to the block.
    std::vector<size_t> right_column_indices_to_add;

    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        const auto & column = right_sample_block.getByPosition(i);
        if (output_columns_names_set_for_other_condition_after_finalize.contains(column.name))
        {
            RUNTIME_CHECK_MSG(
                !block.has(column.name),
                "block from probe side has a column with the same name: {} as a column in right_sample_block",
                column.name);
            block.insert(column);
            right_column_indices_to_add.push_back(i);
        }
    }

    if (!res_list.empty())
    {
        NASemiJoinHelper<KIND, STRICTNESS, typename Maps::MappedType> helper(
            block,
            left_columns,
            right_column_indices_to_add,
            blocks,
            null_rows,
            max_block_size,
            non_equal_conditions,
            is_cancelled);

        helper.joinResult(res_list);

        RUNTIME_CHECK_MSG(res_list.empty(), "NASemiJoinResult list must be empty after calculating join result");
    }

    if (is_cancelled())
        return {};

    /// Now all results are known.

    std::unique_ptr<IColumn::Filter> filter;
    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
        filter = std::make_unique<IColumn::Filter>(rows);

    MutableColumnPtr left_semi_column_ptr = nullptr;
    ColumnInt8::Container * left_semi_column_data = nullptr;
    ColumnUInt8::Container * left_semi_null_map = nullptr;

    if constexpr (
        KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi || KIND == ASTTableJoin::Kind::NullAware_LeftOuterAnti)
    {
        left_semi_column_ptr = block.getByPosition(block.columns() - 1).column->cloneEmpty();
        auto * left_semi_column = typeid_cast<ColumnNullable *>(left_semi_column_ptr.get());
        left_semi_column_data = &typeid_cast<ColumnVector<Int8> &>(left_semi_column->getNestedColumn()).getData();
        left_semi_null_map = &left_semi_column->getNullMapColumn().getData();
        left_semi_column_data->reserve(rows);
        left_semi_null_map->reserve(rows);
    }

    size_t rows_for_anti = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        auto result = res[i].getResult();
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
        {
            if (result == SemiJoinResultType::TRUE_VALUE)
            {
                // If the result is true, this row should be kept.
                (*filter)[i] = 1;
                ++rows_for_anti;
            }
            else
            {
                // If the result is null or false, this row should be filtered.
                (*filter)[i] = 0;
            }
        }
        else
        {
            switch (result)
            {
            case SemiJoinResultType::FALSE_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(0);
                break;
            case SemiJoinResultType::TRUE_VALUE:
                left_semi_column_data->push_back(1);
                left_semi_null_map->push_back(0);
                break;
            case SemiJoinResultType::NULL_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(1);
                break;
            }
        }
    }

    if constexpr (
        KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi || KIND == ASTTableJoin::Kind::NullAware_LeftOuterAnti)
    {
        block.getByPosition(block.columns() - 1).column = std::move(left_semi_column_ptr);
    }

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            auto & column = block.getByPosition(i);
            if (output_column_names_set_after_finalize.contains(column.name))
                column.column = column.column->filter(*filter, rows_for_anti);
        }
    }
    return block;
}

Block Join::joinBlockSemi(ProbeProcessInfo & probe_process_info) const
{
    JoinBuildInfo join_build_info{
        enable_fine_grained_shuffle,
        fine_grained_shuffle_count,
        isEnableSpill(),
        hash_join_spill_context->isSpilled(),
        build_concurrency,
        restore_config.restore_round};

    probe_process_info.prepareForHashProbe(
        key_names_left,
        non_equal_conditions.left_filter_column,
        kind,
        strictness,
        join_build_info.needVirtualDispatchForProbeBlock(),
        collators,
        restore_config.restore_round);

    Block block{};
#define CALL(KIND, STRICTNESS, MAP) \
    block = joinBlockSemiImpl<KIND, STRICTNESS, MAP>(join_build_info, probe_process_info);

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
    if (kind == Semi && strictness == All)
        CALL(Semi, All, MapsAll)
    else if (kind == Semi && strictness == Any)
        CALL(Semi, Any, MapsAny)
    else if (kind == Anti && strictness == All)
        CALL(Anti, All, MapsAll)
    else if (kind == Anti && strictness == Any)
        CALL(Anti, Any, MapsAny)
    else if (kind == LeftOuterSemi && strictness == All)
        CALL(LeftOuterSemi, All, MapsAll)
    else if (kind == LeftOuterSemi && strictness == Any)
        CALL(LeftOuterSemi, Any, MapsAny)
    else if (kind == LeftOuterAnti && strictness == All)
        CALL(LeftOuterAnti, All, MapsAll)
    else if (kind == LeftOuterAnti && strictness == Any)
        CALL(LeftOuterAnti, Any, MapsAny)
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
#undef CALL

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);

    /// (left outer) (anti) semi join never expand the left block, just handle the whole block at one time is enough
    probe_process_info.all_rows_joined_finish = true;

    return removeUselessColumn(block);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
Block Join::joinBlockSemiImpl(const JoinBuildInfo & join_build_info, const ProbeProcessInfo & probe_process_info) const
{
    size_t rows = probe_process_info.block.rows();

    auto [res, res_list] = JoinPartition::probeBlockSemi<KIND, STRICTNESS, Maps>(
        partitions,
        rows,
        key_sizes,
        collators,
        join_build_info,
        probe_process_info);

    RUNTIME_ASSERT(res.size() == rows, "SemiJoinResult size {} must be equal to block size {}", res.size(), rows);
    if (is_cancelled())
        return {};

    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;
    Block block{};
    for (size_t i = 0; i < probe_process_info.block.columns(); ++i)
    {
        const auto & column = probe_process_info.block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
            block.insert(column);
    }

    size_t left_columns = block.columns();
    /// Add new columns to the block.
    std::vector<size_t> right_column_indices_to_add;

    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        const auto & column = right_sample_block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
        {
            RUNTIME_CHECK_MSG(
                !block.has(column.name),
                "block from probe side has a column with the same name: {} as a column in right_sample_block",
                column.name);
            block.insert(column);
            right_column_indices_to_add.push_back(i);
        }
    }

    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        if (!res_list.empty())
        {
            SemiJoinHelper<KIND, typename Maps::MappedType> helper(
                block,
                left_columns,
                right_column_indices_to_add,
                max_block_size,
                non_equal_conditions,
                is_cancelled);

            helper.joinResult(res_list);

            RUNTIME_CHECK_MSG(res_list.empty(), "SemiJoinResult list must be empty after calculating join result");
        }
    }

    if (is_cancelled())
        return {};

    /// Now all results are known.

    std::unique_ptr<IColumn::Filter> filter;
    if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
        filter = std::make_unique<IColumn::Filter>(rows);

    MutableColumnPtr left_semi_column_ptr = nullptr;
    ColumnInt8::Container * left_semi_column_data = nullptr;
    ColumnUInt8::Container * left_semi_null_map = nullptr;

    if constexpr (KIND == ASTTableJoin::Kind::LeftOuterSemi || KIND == ASTTableJoin::Kind::LeftOuterAnti)
    {
        left_semi_column_ptr = block.getByPosition(block.columns() - 1).column->cloneEmpty();
        auto * left_semi_column = typeid_cast<ColumnNullable *>(left_semi_column_ptr.get());
        left_semi_column_data = &typeid_cast<ColumnVector<Int8> &>(left_semi_column->getNestedColumn()).getData();
        left_semi_column_data->reserve(rows);
        left_semi_null_map = &left_semi_column->getNullMapColumn().getData();
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
        {
            left_semi_null_map->resize_fill(rows, 0);
        }
        else
        {
            left_semi_null_map->reserve(rows);
        }
    }

    size_t rows_for_semi_anti = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        auto result = res[i].getResult();
        if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
        {
            if (isTrueSemiJoinResult(result))
            {
                // If the result is true, this row should be kept.
                (*filter)[i] = 1;
                ++rows_for_semi_anti;
            }
            else
            {
                // If the result is null or false, this row should be filtered.
                (*filter)[i] = 0;
            }
        }
        else
        {
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                left_semi_column_data->push_back(result);
            }
            else
            {
                switch (result)
                {
                case SemiJoinResultType::FALSE_VALUE:
                    left_semi_column_data->push_back(0);
                    left_semi_null_map->push_back(0);
                    break;
                case SemiJoinResultType::TRUE_VALUE:
                    left_semi_column_data->push_back(1);
                    left_semi_null_map->push_back(0);
                    break;
                case SemiJoinResultType::NULL_VALUE:
                    left_semi_column_data->push_back(0);
                    left_semi_null_map->push_back(1);
                    break;
                }
            }
        }
    }

    if constexpr (KIND == ASTTableJoin::Kind::LeftOuterSemi || KIND == ASTTableJoin::Kind::LeftOuterAnti)
    {
        block.getByPosition(block.columns() - 1).column = std::move(left_semi_column_ptr);
    }

    if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            auto & column = block.getByPosition(i);
            if (output_column_names_set_after_finalize.contains(column.name))
                column.column = column.column->filter(*filter, rows_for_semi_anti);
        }
    }
    return block;
}

void Join::checkTypesOfKeys(const Block & block_left, const Block & block_right) const
{
    size_t keys_size = key_names_left.size();
    for (size_t i = 0; i < keys_size; ++i)
    {
        /// Compare up to Nullability.
        DataTypePtr left_type = removeNullable(block_left.getByName(key_names_left[i]).type);
        DataTypePtr right_type = removeNullable(block_right.getByName(key_names_right[i]).type);
        if unlikely (!left_type->equals(*right_type))
            throw Exception(
                fmt::format(
                    "Type mismatch of columns to JOIN by: {} {} at left, {} {} at right",
                    key_names_left[i],
                    left_type->getName(),
                    key_names_right[i],
                    right_type->getName()),
                ErrorCodes::TYPE_MISMATCH);
    }
}

bool Join::finishOneBuild(size_t stream_index)
{
    std::unique_lock lock(build_probe_mutex);
    if (active_build_threads == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_build);
    }
    --active_build_threads;
    if (active_build_threads == 0)
    {
        workAfterBuildFinish(stream_index);
        return true;
    }
    return false;
}

void Join::finalizeBuild()
{
    {
        std::unique_lock lock(build_probe_mutex);
        if (hash_join_spill_context->getBuildSpiller())
            hash_join_spill_context->getBuildSpiller()->finishSpill();
        assert(active_build_threads == 0);
        build_finished = true;
    }
    build_cv.notify_all();
    wait_build_finished_future->finish();
    LOG_INFO(log, "build finalize with {} entries from {} rows.", getTotalRowCount(), getTotalBuildInputRows());
}

void Join::workAfterBuildFinish(size_t stream_index)
{
    if (isNullAwareSemiFamily(kind))
        finalizeNullAwareSemiFamilyBuild();

    if (isCrossJoin(kind))
        finalizeCrossJoinBuild();

    if (isEnableSpill())
    {
        hash_join_spill_context->finishBuild();
        if (isSpilled())
        {
            // TODO support runtime filter with spill.
            cancelRuntimeFilter(
                "Currently runtime filter is not compatible with join spill, so cancel runtime filter here.");
        }
        else
        {
            // set rf is ready
            finalizeRuntimeFilter();
        }

        for (size_t i = 0; i < partitions.size(); ++i)
        {
            if (hash_join_spill_context->isPartitionMarkedForAutoSpill(i)
                || hash_join_spill_context->isPartitionSpilled(i))
            {
                if (!hash_join_spill_context->isPartitionSpilled(i))
                {
                    /// corner case, spill is requested in the last minute
                    const auto & join_partition = partitions[i];
                    auto partition_lock = join_partition->lockPartition();
                    hash_join_spill_context->markPartitionSpilled(i);
                    join_partition->releasePartitionPoolAndHashMap(partition_lock);
                }
                markBuildSideSpillData(i, partitions[i]->trySpillBuildPartition(), stream_index);
            }
        }

        for (size_t i = 0; i < partitions.size(); ++i)
        {
            if (hash_join_spill_context->isPartitionSpilled(i))
                remaining_partition_indexes_to_restore.push_back(i);
        }
        LOG_DEBUG(log, "memory usage after build finish: {}", getTotalByteCount());

        has_build_data_in_memory = std::any_of(partitions.cbegin(), partitions.cend(), [](const auto & p) {
            return !p->isSpill() && p->hasBuildData();
        });
    }
    else
    {
        // set rf is ready
        finalizeRuntimeFilter();

        has_build_data_in_memory = !original_blocks.empty();
    }
}

void Join::finalizeNullAwareSemiFamilyBuild()
{
    size_t null_rows_size = 0;
    for (const auto & partition : partitions)
    {
        null_rows_size += partition->getRowsNotInsertedToMap()->total_size;
    }
    /// Considering the rows with null key in left table, in the worse case, it may need to check all rows in right table.
    /// Null rows are used for speeding up the check process. If the result of null-aware equal expression is NULL, the
    /// check process can be finished.
    /// However, if checking null rows does not get a NULL result, these null rows will be checked again in the process of
    /// checking all blocks.
    /// So there is a tradeoff between returning quickly and avoiding waste.
    ///
    /// If all rows have null key, test results at the time of writing show that the time consumed by checking null rows is
    /// several times than that of checking all blocks, and it increases as the number of rows in the right table increases.
    /// For example, the number of rows in right table is 2k and 20000k in left table, null rows take about 1 time as long
    /// as all blocks. When the number of rows in right table is 5k, 1.4 times. 10k => 1.7 times. 20k => 1.9 times.
    ///
    /// Given that many null rows should be a rare case, let's use 2 times to simplify thinking.
    /// So if null rows occupy 1/3 of all rows, the time consumed by null rows and all blocks are basically the same.
    /// I choose 1/3 as the cutoff point. If null rows occupy more than 1/3, we should check all blocks directly.
    if (unlikely(is_test))
        null_key_check_all_blocks_directly = false;
    else
        null_key_check_all_blocks_directly
            = static_cast<double>(null_rows_size) > static_cast<double>(total_input_build_rows) / 3.0;
}

void Join::finalizeCrossJoinBuild()
{
    original_blocks.clear();
    for (const auto & block : blocks)
        original_blocks.push_back(block);
    right_rows_to_be_added_when_matched_for_cross_join = 0;
    for (const auto & block : original_blocks)
        right_rows_to_be_added_when_matched_for_cross_join += block.rows();
    if (strictness == ASTTableJoin::Strictness::Any)
    {
        /// for cross any join, at most 1 row is added
        right_rows_to_be_added_when_matched_for_cross_join
            = std::min(right_rows_to_be_added_when_matched_for_cross_join, 1);
    }
    else if (blocks.size() > 1 && right_rows_to_be_added_when_matched_for_cross_join <= max_block_size)
    {
        /// for cross all join, if total right rows is less than max_block_size, then merge all
        /// the right blocks into one block
        blocks.clear();
        auto merged_block = vstackBlocks(std::move(original_blocks));
        original_blocks.clear();
        blocks.push_back(merged_block);
        original_blocks.push_back(merged_block);
    }
    /// since shallow_copy_probe_threshold is at least 1, if strictness is any, it will never use SHALLOW_COPY_RIGHT_BLOCK
    cross_probe_mode = right_rows_to_be_added_when_matched_for_cross_join > shallow_copy_cross_probe_threshold
        ? CrossProbeMode::SHALLOW_COPY_RIGHT_BLOCK
        : CrossProbeMode::DEEP_COPY_RIGHT_BLOCK;
    LOG_DEBUG(log, "Cross join will use {} probe mode", magic_enum::enum_name(cross_probe_mode));
}

void Join::finalizeProfileInfo()
{
    profile_info->is_spill_enabled = isEnableSpill();
    profile_info->is_spilled = isSpilled();
    profile_info->peak_build_bytes_usage = getPeakBuildBytesUsage();
}

void Join::workAfterProbeFinish(size_t stream_index)
{
    finalizeProfileInfo();

    if (isEnableSpill())
    {
        // flush cached blocks for spilled partition.
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            if (hash_join_spill_context->isPartitionSpilled(i))
                markProbeSideSpillData(i, partitions[i]->trySpillProbePartition(), stream_index);
        }
        hash_join_spill_context->finishSpillableStage();
    }

    // If it is no longer to scan non-matched-data from the hash table, the hash table can be released.
    if (!needScanHashMapAfterProbe(kind))
        releaseAllPartitions();
}

void Join::waitUntilAllBuildFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    build_cv.wait(lock, [&]() { return build_finished || meet_error || skip_wait; });
    if (meet_error)
        throw Exception(error_message);
}

bool Join::finishOneProbe(size_t stream_index)
{
    std::unique_lock lock(build_probe_mutex);
    if (active_probe_threads == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_probe);
    }
    --active_probe_threads;
    if (active_probe_threads == 0)
    {
        workAfterProbeFinish(stream_index);
        return true;
    }
    return false;
}

void Join::finalizeProbe()
{
    {
        std::unique_lock lock(build_probe_mutex);
        if (hash_join_spill_context->getProbeSpiller())
            hash_join_spill_context->getProbeSpiller()->finishSpill();
        assert(active_probe_threads == 0);
        probe_finished = true;
    }
    probe_cv.notify_all();
    wait_probe_finished_future->finish();
}

void Join::waitUntilAllProbeFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    probe_cv.wait(lock, [&]() { return probe_finished || meet_error || skip_wait; });
    if (meet_error)
        throw Exception(error_message);
}

bool Join::isProbeFinishedForPipeline() const
{
    if (!probe_finished)
    {
        setNotifyFuture(wait_probe_finished_future.get());
        return false;
    }
    return true;
}

bool Join::isBuildFinishedForPipeline() const
{
    if (!build_finished)
    {
        setNotifyFuture(wait_build_finished_future.get());
        return false;
    }
    return true;
}

void Join::finishOneNonJoin(size_t partition_index)
{
    // When spill is enabled, the build data blocks are seperated for each partition, so when non-joined-scan ends, the corresponding join partition can be released.
    // When spill is not enabled, all build data blocks are stored in the same partition, so the join partition cannot be released.
    if (isEnableSpill())
    {
        if likely (build_finished && probe_finished)
        {
            /// only clear hash table if not active build/probe threads
            while (partition_index < build_concurrency)
            {
                partitions[partition_index]->releasePartition();
                partition_index += build_concurrency;
            }
        }
    }
}

Block Join::joinBlock(ProbeProcessInfo & probe_process_info) const
{
    assert(!probe_process_info.all_rows_joined_finish);
    assert(finalized);
    if unlikely (!build_finished)
    {
        /// build is not finished yet, the query must be cancelled, so just return {}
        LOG_WARNING(log, "JoinBlock without non zero active_build_threads, return empty block");
        return {};
    }
    std::shared_lock lock(rwlock);

    Block block{};

    using enum ASTTableJoin::Kind;
    if (isCrossJoin(kind))
        block = joinBlockCross(probe_process_info);
    else if (isNullAwareSemiFamily(kind))
        block = joinBlockNullAwareSemi(probe_process_info);
    else if (isSemiFamily(kind) || isLeftOuterSemiFamily(kind))
        block = joinBlockSemi(probe_process_info);
    else
        block = joinBlockHash(probe_process_info);

    // if cancelled, just return empty block
    if (!block)
        return block;
    /// for (cartesian)antiLeftSemi join, the meaning of "match-helper" is `non-matched` instead of `matched`.
    if (kind == Cross_LeftOuterAnti)
    {
        const auto * nullable_column
            = checkAndGetColumn<ColumnNullable>(block.getByName(match_helper_name).column.get());
        const auto & vec_matched
            = static_cast<const ColumnVector<Int8> *>(nullable_column->getNestedColumnPtr().get())->getData();

        auto col_non_matched = ColumnInt8::create(vec_matched.size());
        auto & vec_non_matched = col_non_matched->getData();

        for (size_t i = 0; i < vec_matched.size(); ++i)
            vec_non_matched[i] = !vec_matched[i];

        block.getByName(match_helper_name).column
            = ColumnNullable::create(std::move(col_non_matched), std::move(nullable_column->getNullMapColumnPtr()));
    }

    return block;
}

BlockInputStreamPtr Join::createScanHashMapAfterProbeStream(
    const Block & left_sample_block,
    size_t index,
    size_t step,
    size_t max_block_size_) const
{
    RUNTIME_CHECK_MSG(finalized, "Create ScanHashMapAfterProbeStream before join be finalized");
    return std::make_shared<ScanHashMapAfterProbeBlockInputStream>(
        *this,
        left_sample_block,
        index,
        step,
        max_block_size_);
}

Blocks Join::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_shards = build_concurrency;
    size_t num_cols = from_block.columns();
    Blocks result(num_shards);
    if (num_shards == 1)
    {
        result[0] = from_block;
        return result;
    }

    IColumn::Selector selector = selectDispatchBlock(key_columns_names, from_block);


    for (size_t i = 0; i < num_shards; ++i)
        result[i] = from_block.cloneEmpty();

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

IColumn::Selector Join::hashToSelector(const WeakHash32 & hash) const
{
    size_t num_shards = build_concurrency;
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);

    if unlikely (enable_fine_grained_shuffle && fine_grained_shuffle_count != build_concurrency)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            selector[i] = data[i] % fine_grained_shuffle_count;
            selector[i] = selector[i] % num_shards;
        }
    }
    else
    {
        if (num_shards & (num_shards - 1))
        {
            for (size_t i = 0; i < num_rows; ++i)
            {
                selector[i] = data[i] % num_shards;
            }
        }
        else
        {
            for (size_t i = 0; i < num_rows; ++i)
            {
                selector[i] = data[i] & (num_shards - 1);
            }
        }
    }

    return selector;
}

IColumn::Selector Join::selectDispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(from_block, materialized_columns, key_columns_names);

    size_t num_rows = from_block.rows();
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    WeakHash32 hash(0);
    computeDispatchHash(num_rows, key_columns, collators, sort_key_containers, restore_config.restore_round, hash);
    return hashToSelector(hash);
}

void Join::spillBuildSideBlocks(UInt64 part_id, Blocks && spill_blocks) const
{
    hash_join_spill_context->getBuildSpiller()->spillBlocks(std::move(spill_blocks), part_id);
    hash_join_spill_context->finishOneSpill(part_id);
}

void Join::spillProbeSideBlocks(UInt64 part_id, Blocks && spill_blocks) const
{
    hash_join_spill_context->getProbeSpiller()->spillBlocks(std::move(spill_blocks), part_id);
    hash_join_spill_context->finishOneSpill(part_id);
}

void Join::markBuildSideSpillData(UInt64 part_id, Blocks && blocks_to_spill, size_t stream_index)
{
    if (!blocks_to_spill.empty())
    {
        auto & data = getBuildSideMarkedSpillData(stream_index);
        assert(data.find(part_id) == data.end());
        data[part_id] = std::move(blocks_to_spill);
    }
}

void Join::markProbeSideSpillData(UInt64 part_id, Blocks && blocks_to_spill, size_t stream_index)
{
    if (!blocks_to_spill.empty())
    {
        auto & data = getProbeSideMarkedSpillData(stream_index);
        assert(data.find(part_id) == data.end());
        data[part_id] = std::move(blocks_to_spill);
    }
}

void Join::spillMostMemoryUsedPartitionIfNeed(size_t stream_index)
{
    if (!hash_join_spill_context->isSpillEnabled())
        return;
    {
        std::unique_lock lk(build_probe_mutex);

#ifdef DBMS_PUBLIC_GTEST
        // for join spill to disk gtest
        if (restore_config.restore_round == std::max(2, MAX_RESTORE_ROUND_IN_GTEST) - 1
            && hash_join_spill_context->spilledPartitionCount() >= partitions.size() / 2)
            return;
#endif

        for (const auto & partition_to_be_spilled : hash_join_spill_context->getPartitionsToSpill())
        {
            RUNTIME_CHECK_MSG(
                build_concurrency > 1,
                "spilling is not is not supported when stream size = 1, please increase max_threads or set "
                "max_bytes_before_external_join = 0.");
            LOG_INFO(
                log,
                fmt::format(
                    "Join with restore round: {}, used {} bytes, will spill partition: {}.",
                    restore_config.restore_round,
                    getTotalByteCount(),
                    partition_to_be_spilled));

            std::unique_lock partition_lock = partitions[partition_to_be_spilled]->lockPartition();
            hash_join_spill_context->markPartitionSpilled(partition_to_be_spilled);
            partitions[partition_to_be_spilled]->releasePartitionPoolAndHashMap(partition_lock);
            auto blocks_to_spill = partitions[partition_to_be_spilled]->trySpillBuildPartition(partition_lock);
            markBuildSideSpillData(partition_to_be_spilled, std::move(blocks_to_spill), stream_index);
        }
    }
}

bool Join::getPartitionSpilled(size_t partition_index) const
{
    return hash_join_spill_context->isPartitionSpilled(partition_index);
}


bool Join::hasPartitionToRestore()
{
    std::unique_lock lk(build_probe_mutex);
    return !remaining_partition_indexes_to_restore.empty();
}

std::optional<RestoreInfo> Join::getOneRestoreStream(size_t max_block_size_)
{
    std::unique_lock lock(build_probe_mutex);
    if (meet_error)
        throw Exception(error_message);
    try
    {
        while (true)
        {
            LOG_TRACE(log, "restore_infos size =  {}", restore_infos.size());
            if (!restore_infos.empty())
            {
                auto restore_info = std::move(restore_infos.back());
                restore_infos.pop_back();
                if (restore_infos.empty())
                {
                    remaining_partition_indexes_to_restore.pop_front();
                }
                return restore_info;
            }
            if (remaining_partition_indexes_to_restore.empty())
            {
                return {};
            }

            // build new restore infos.
            auto spilled_partition_index = remaining_partition_indexes_to_restore.front();
            RUNTIME_CHECK_MSG(
                hash_join_spill_context->isPartitionSpilled(spilled_partition_index),
                "should not restore unspilled partition.");

            if (restore_join_build_concurrency <= 0)
                restore_join_build_concurrency = getRestoreJoinBuildConcurrency(
                    partitions.size(),
                    remaining_partition_indexes_to_restore.size(),
                    restore_config.join_restore_concurrency,
                    probe_concurrency);
            /// for restore join we make sure that the restore_join_build_concurrency is at least 2, so it can be spill again.
            /// And restore_join_build_concurrency should not be greater than probe_concurrency, Otherwise some restore_stream will never be executed.
            RUNTIME_CHECK_MSG(
                2 <= restore_join_build_concurrency
                    && restore_join_build_concurrency <= static_cast<Int64>(probe_concurrency),
                "restore_join_build_concurrency must in [2, {}], but the current value is {}",
                probe_concurrency,
                restore_join_build_concurrency);
            LOG_INFO(
                log,
                "Begin restore data from disk for hash join, partition {}, restore round {}, build concurrency {}.",
                spilled_partition_index,
                restore_config.restore_round,
                restore_join_build_concurrency);

            auto restore_build_streams = hash_join_spill_context->getBuildSpiller()->restoreBlocks(
                spilled_partition_index,
                restore_join_build_concurrency,
                true);
            RUNTIME_CHECK_MSG(
                restore_build_streams.size() == static_cast<size_t>(restore_join_build_concurrency),
                "restore streams size must equal to restore_join_build_concurrency");
            auto restore_probe_streams = hash_join_spill_context->getProbeSpiller()->restoreBlocks(
                spilled_partition_index,
                restore_join_build_concurrency,
                true);
            auto new_max_bytes_before_external_join = hash_join_spill_context->getOperatorSpillThreshold();
            /// have operator level threshold
            if (new_max_bytes_before_external_join > 0)
                new_max_bytes_before_external_join = std::max(
                    1,
                    static_cast<UInt64>(
                        new_max_bytes_before_external_join
                        * (static_cast<double>(restore_join_build_concurrency) / build_concurrency)));
            restore_join = createRestoreJoin(new_max_bytes_before_external_join, spilled_partition_index);
            restore_join->initBuild(build_sample_block, restore_join_build_concurrency);
            restore_join->setInitActiveBuildThreads();
            restore_join->initProbe(probe_sample_block, restore_join_build_concurrency);
            restore_join->setCancellationHook(is_cancelled);
            BlockInputStreams restore_scan_hash_map_streams;
            restore_scan_hash_map_streams.resize(restore_join_build_concurrency, nullptr);
            if (needScanHashMapAfterProbe(kind))
            {
                auto header = restore_probe_streams.back()->getHeader();
                for (Int64 i = 0; i < restore_join_build_concurrency; ++i)
                    restore_scan_hash_map_streams[i] = restore_join->createScanHashMapAfterProbeStream(
                        header,
                        i,
                        restore_join_build_concurrency,
                        max_block_size_);
            }
            for (Int64 i = 0; i < restore_join_build_concurrency; ++i)
            {
                restore_infos.emplace_back(
                    restore_join,
                    i,
                    std::move(restore_scan_hash_map_streams[i]),
                    std::move(restore_build_streams[i]),
                    std::move(restore_probe_streams[i]));
            }
        }
    }
    catch (...)
    {
        restore_infos.clear();
        meetErrorImpl(getCurrentExceptionMessage(false, true), lock);
        std::rethrow_exception(std::current_exception());
    }
}

void Join::dispatchProbeBlock(Block & block, PartitionBlocks & partition_blocks_list, size_t stream_index)
{
    Blocks partition_blocks = dispatchBlock(key_names_left, block);
    for (size_t i = 0; i < partition_blocks.size(); ++i)
    {
        if (partition_blocks[i].rows() == 0)
            continue;
        Blocks blocks_to_spill;
        bool need_spill = false;
        {
            std::unique_lock partition_lock = partitions[i]->lockPartition();
            if (getPartitionSpilled(i))
            {
                partitions[i]->insertBlockForProbe(std::move(partition_blocks[i]));
                if (hash_join_spill_context->updatePartitionRevocableMemory(i, partitions[i]->revocableBytes()))
                    blocks_to_spill = partitions[i]->trySpillProbePartition(partition_lock);
                need_spill = true;
            }
        }
        if (need_spill)
        {
            markProbeSideSpillData(i, std::move(blocks_to_spill), stream_index);
        }
        else
        {
            partition_blocks_list.emplace_back(i, std::move(partition_blocks[i]));
        }
    }
}

void Join::releaseAllPartitions()
{
    for (auto & partition : partitions)
    {
        partition->releasePartition();
    }
}

void Join::wakeUpAllWaitingThreads()
{
    std::unique_lock lk(build_probe_mutex);
    skip_wait = true;
    probe_cv.notify_all();
    build_cv.notify_all();
    cancelRuntimeFilter("Join has been cancelled.");
    // wait_build/probe_finished_future does not need to call finish here
    // because it is called in PipelineExecutorContext.
}

void Join::finalize(const Names & parent_require)
{
    if unlikely (finalized)
        return;
    /// finalize will do 3 things
    /// 1. update expected_output_schema
    /// 2. set expected_output_schema_for_other_condition
    /// 3. generated needed input columns
    NameSet required_names_set;
    for (const auto & name : parent_require)
        required_names_set.insert(name);
    if unlikely (!match_helper_name.empty() && !required_names_set.contains(match_helper_name))
    {
        /// should only happens in some tests
        required_names_set.insert(match_helper_name);
    }
    for (const auto & name_and_type : output_columns)
    {
        if (required_names_set.contains(name_and_type.name))
        {
            output_columns_after_finalize.push_back(name_and_type);
            output_column_names_set_after_finalize.insert(name_and_type.name);
        }
    }
    RUNTIME_CHECK_MSG(
        output_column_names_set_after_finalize.size() == output_columns_after_finalize.size(),
        "Logical error, the output of join contains duplicated columns");

    output_block_after_finalize = Block(output_columns_after_finalize);
    Names updated_require;
    if (match_helper_name.empty())
        updated_require = parent_require;
    else
    {
        required_names_set.erase(match_helper_name);
        for (const auto & name : required_names_set)
            updated_require.push_back(name);
    }
    if (!non_equal_conditions.null_aware_eq_cond_name.empty())
    {
        updated_require.push_back(non_equal_conditions.null_aware_eq_cond_name);
    }
    if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
        updated_require.push_back(non_equal_conditions.other_eq_cond_from_in_name);
    if (!non_equal_conditions.other_cond_name.empty())
        updated_require.push_back(non_equal_conditions.other_cond_name);
    auto keep_used_input_columns
        = !isCrossJoin(kind) && (isNullAwareSemiFamily(kind) || isSemiFamily(kind) || isLeftOuterSemiFamily(kind));
    /// nullaware/semi join will reuse the input columns so need to let finalize keep the input columns
    if (non_equal_conditions.null_aware_eq_cond_expr != nullptr)
    {
        non_equal_conditions.null_aware_eq_cond_expr->finalize(updated_require, keep_used_input_columns);
        updated_require = non_equal_conditions.null_aware_eq_cond_expr->getRequiredColumns();
    }
    if (non_equal_conditions.other_cond_expr != nullptr)
    {
        non_equal_conditions.other_cond_expr->finalize(updated_require, keep_used_input_columns);
        updated_require = non_equal_conditions.other_cond_expr->getRequiredColumns();
    }

    if (non_equal_conditions.other_cond_expr != nullptr || non_equal_conditions.null_aware_eq_cond_expr != nullptr)
    {
        output_columns_names_set_for_other_condition_after_finalize = output_column_names_set_after_finalize;
        for (const auto & name : updated_require)
            output_columns_names_set_for_other_condition_after_finalize.insert(name);
        if (!match_helper_name.empty())
            output_columns_names_set_for_other_condition_after_finalize.insert(match_helper_name);
    }

    /// remove duplicated column
    required_names_set.clear();
    for (const auto & name : updated_require)
        required_names_set.insert(name);
    /// add some internal used columns
    if (!non_equal_conditions.left_filter_column.empty())
        required_names_set.insert(non_equal_conditions.left_filter_column);
    if (!non_equal_conditions.right_filter_column.empty())
        required_names_set.insert(non_equal_conditions.right_filter_column);
    /// add join key to required_columns
    for (const auto & name : key_names_right)
        required_names_set.insert(name);
    for (const auto & name : key_names_left)
        required_names_set.insert(name);

    for (const auto & name : required_names_set)
        required_columns.push_back(name);
    finalized = true;
}

} // namespace DB
