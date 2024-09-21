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
#include <Common/ColumnsHashing.h>
#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Join.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/NullableUtils.h>
#include <common/logger_useful.h>

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
SpillConfig createSpillConfigWithNewSpillId(const SpillConfig & config, const String & new_spill_id)
{
    return SpillConfig(config.spill_dir, new_spill_id, config.max_cached_data_bytes_in_spiller, config.max_spilled_rows_per_file, config.max_spilled_bytes_per_file, config.file_provider);
}
size_t getRestoreJoinBuildConcurrency(size_t total_partitions, size_t spilled_partitions, Int64 join_restore_concurrency, size_t total_concurrency)
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
        size_t restore_times = (spilled_partitions + max_concurrent_restore_partition - 1) / max_concurrent_restore_partition;
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

Join::Join(
    const Names & key_names_left_,
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
    const TiDB::TiDBCollators & collators_,
    const JoinNonEqualConditions & non_equal_conditions_,
    size_t max_block_size_,
    const String & match_helper_name_,
    const String & flag_mapped_entry_helper_name_,
    size_t restore_round_,
    bool is_test_)
    : restore_round(restore_round_)
    , match_helper_name(match_helper_name_)
    , flag_mapped_entry_helper_name(flag_mapped_entry_helper_name_)
    , kind(kind_)
    , strictness(strictness_)
    , original_strictness(strictness)
    , may_probe_side_expanded_after_join(mayProbeSideExpandedAfterJoin(kind, strictness))
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , build_concurrency(0)
    , active_build_threads(0)
    , probe_concurrency(0)
    , active_probe_threads(0)
    , collators(collators_)
    , non_equal_conditions(non_equal_conditions_)
    , max_block_size(max_block_size_)
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , build_spill_config(build_spill_config_)
    , probe_spill_config(probe_spill_config_)
    , join_restore_concurrency(join_restore_concurrency_)
    , is_test(is_test_)
    , log(Logger::get(req_id))
    , enable_fine_grained_shuffle(enable_fine_grained_shuffle_)
    , fine_grained_shuffle_count(fine_grained_shuffle_count_)
{
    if (non_equal_conditions.other_cond_expr != nullptr)
    {
        /// if there is other_condition, then should keep all the valid rows during probe stage
        if (strictness == ASTTableJoin::Strictness::Any)
        {
            strictness = ASTTableJoin::Strictness::All;
        }
        has_other_condition = true;
    }
    else
    {
        has_other_condition = false;
    }

    String err = non_equal_conditions.validate(kind);
    if (unlikely(!err.empty()))
        throw Exception("Validate join conditions error: {}" + err);

    LOG_DEBUG(log, "FineGrainedShuffle flag {}, stream count {}", enable_fine_grained_shuffle, fine_grained_shuffle_count);
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
}

size_t Join::getTotalRowCount() const
{
    size_t res = 0;

    if (join_map_method == JoinMapMethod::CROSS)
    {
        res = total_input_build_rows;
    }
    else
    {
        for (const auto & partition : partitions)
            res += partition->getRowCount();
    }

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
        throw Exception("Logical error: `setBuildConcurrencyAndInitJoinPartition` shouldn't be called more than once", ErrorCodes::LOGICAL_ERROR);
    /// do not set active_build_threads because in compile stage, `joinBlock` will be called to get generate header, if active_build_threads
    /// is set here, `joinBlock` will hang when used to get header
    build_concurrency = std::max(1, build_concurrency_);

    partitions.reserve(build_concurrency);
    for (size_t i = 0; i < getBuildConcurrency(); ++i)
    {
        partitions.push_back(std::make_unique<JoinPartition>(join_map_method, kind, strictness, max_block_size, log, has_other_condition));
    }
}

void Join::setSampleBlock(const Block & block)
{
    sample_block_with_columns_to_add = materializeBlock(block);

    /// Move from `sample_block_with_columns_to_add` key columns to `sample_block_with_keys`, keeping the order.
    size_t pos = 0;
    while (pos < sample_block_with_columns_to_add.columns())
    {
        const auto & name = sample_block_with_columns_to_add.getByPosition(pos).name;
        if (key_names_right.end() != std::find(key_names_right.begin(), key_names_right.end(), name))
        {
            sample_block_with_keys.insert(sample_block_with_columns_to_add.getByPosition(pos));
            sample_block_with_columns_to_add.erase(pos);
        }
        else
            ++pos;
    }

    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = sample_block_with_columns_to_add.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(sample_block_with_columns_to_add.getByPosition(i));

    if (isLeftOuterSemiFamily(kind))
        sample_block_with_columns_to_add.insert(ColumnWithTypeAndName(Join::match_helper_type, match_helper_name));
}

std::shared_ptr<Join> Join::createRestoreJoin(size_t max_bytes_before_external_join_)
{
    return std::make_shared<Join>(
        key_names_left,
        key_names_right,
        kind,
        original_strictness,
        log->identifier(),
        false,
        0,
        max_bytes_before_external_join_,
        createSpillConfigWithNewSpillId(build_spill_config, fmt::format("{}_hash_join_{}_build", log->identifier(), restore_round + 1)),
        createSpillConfigWithNewSpillId(probe_spill_config, fmt::format("{}_hash_join_{}_probe", log->identifier(), restore_round + 1)),
        join_restore_concurrency,
        collators,
        non_equal_conditions,
        max_block_size,
        match_helper_name,
        flag_mapped_entry_helper_name,
        restore_round + 1,
        is_test);
}

void Join::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    std::unique_lock lock(rwlock);
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;
    join_map_method = chooseJoinMapMethod(getKeyColumns(key_names_right, sample_block), key_sizes, collators);
    setBuildConcurrencyAndInitJoinPartition(build_concurrency_);
    build_sample_block = sample_block;
    build_spiller = std::make_unique<Spiller>(build_spill_config, false, build_concurrency_, build_sample_block, log);
    if (max_bytes_before_external_join > 0)
    {
        if (join_map_method == JoinMapMethod::CROSS)
        {
            /// todo support spill for cross join
            max_bytes_before_external_join = 0;
            LOG_WARNING(log, "Cross join does not support spilling, so set max_bytes_before_external_join = 0");
        }
        if (isNullAwareSemiFamily(kind))
        {
            max_bytes_before_external_join = 0;
            LOG_WARNING(log, "null aware join does not support spilling, so set max_bytes_before_external_join = 0");
        }
    }
    setSampleBlock(sample_block);
}

void Join::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    std::unique_lock lock(rwlock);
    setProbeConcurrency(probe_concurrency_);
    probe_sample_block = sample_block;
    probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, build_concurrency, probe_sample_block, log);
}

/// the block should be valid.
void Join::insertFromBlock(const Block & block, size_t stream_index)
{
    std::shared_lock lock(rwlock);
    assert(stream_index < getBuildConcurrency());
    total_input_build_rows += block.rows();

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);
    Block * stored_block = nullptr;

    if (!isEnableSpill())
    {
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
        if (enable_fine_grained_shuffle)
        {
            dispatch_blocks.resize(build_concurrency, {});
            dispatch_blocks[stream_index] = block;
        }
        else
        {
            dispatch_blocks = dispatchBlock(key_names_right, block);
        }
        assert(dispatch_blocks.size() == build_concurrency);

        size_t bytes_to_be_added = 0;
        for (const auto & partition_block : dispatch_blocks)
        {
            if (partition_block)
            {
                bytes_to_be_added += partition_block.bytes();
            }
        }
        bool force_spill_partition_blocks = false;
        {
            std::unique_lock lk(build_probe_mutex);
            if (max_bytes_before_external_join && bytes_to_be_added + getTotalByteCount() >= max_bytes_before_external_join)
            {
                force_spill_partition_blocks = true;
            }
        }

        for (size_t j = stream_index; j < build_concurrency + stream_index; ++j)
        {
            stored_block = nullptr;
            size_t i = j % build_concurrency;
            if (!dispatch_blocks[i].rows())
            {
                continue;
            }
            Blocks blocks_to_spill;
            {
                const auto & join_partition = partitions[i];
                auto partition_lock = join_partition->lockPartition();
                partitions[i]->insertBlockForBuild(std::move(dispatch_blocks[i]));
                if (join_partition->isSpill())
                    blocks_to_spill = join_partition->trySpillBuildPartition(force_spill_partition_blocks, build_spill_config.max_cached_data_bytes_in_spiller, partition_lock);
                else
                    stored_block = join_partition->getLastBuildBlock();
                if (stored_block != nullptr)
                {
                    size_t byte_before_insert = join_partition->getHashMapAndPoolByteCount();
                    insertFromBlockInternal(stored_block, i);
                    size_t byte_after_insert = join_partition->getHashMapAndPoolByteCount();
                    if likely (byte_after_insert > byte_before_insert)
                    {
                        join_partition->addMemoryUsage(byte_after_insert - byte_before_insert);
                    }
                    continue;
                }
            }
            build_spiller->spillBlocks(std::move(blocks_to_spill), i);
        }
#ifdef DBMS_PUBLIC_GTEST
        // for join spill to disk gtest
        if (restore_round == 2)
            return;
#endif
        spillMostMemoryUsedPartitionIfNeed();
    }
}

bool Join::isEnableSpill() const
{
    return max_bytes_before_external_join > 0;
}

bool Join::isRestoreJoin() const
{
    return restore_round > 0;
}

void Join::insertFromBlockInternal(Block * stored_block, size_t stream_index)
{
    size_t keys_size = key_names_right.size();

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

    if (needScanHashMapAfterProbe(kind))
    {
        /** Move the key columns to the beginning of the block.
          * This is where ScanHashMapAfterProbBlockInputStream will expect.
          */
        size_t key_num = 0;
        for (const auto & name : key_names_right)
        {
            size_t pos = stored_block->getPositionByName(name);
            ColumnWithTypeAndName col = stored_block->safeGetByPosition(pos);
            stored_block->erase(pos);
            stored_block->insert(key_num, std::move(col));
            ++key_num;
        }
    }
    else
    {
        /// Remove the key columns from stored_block, as they are not needed.
        for (const auto & name : key_names_right)
            stored_block->erase(stored_block->getPositionByName(name));
    }

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
        for (size_t i = getFullness(kind) ? keys_size : 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    bool enable_join_spill = max_bytes_before_external_join;

    if (!isCrossJoin(kind))
    {
        if (enable_join_spill)
            assert(partitions[stream_index]->getPartitionPool() != nullptr);
        /// Fill the hash table.
        JoinPartition::insertBlockIntoMaps(partitions, rows, key_columns, key_sizes, collators, stored_block, null_map, stream_index, getBuildConcurrency(), enable_fine_grained_shuffle, enable_join_spill);
    }
}

void mergeNullAndFilterResult(Block & block, ColumnVector<UInt8>::Container & filter_column, const String & filter_column_name, bool null_as_true)
{
    auto orig_filter_column = block.getByName(filter_column_name).column;
    if (orig_filter_column->isColumnConst())
        orig_filter_column = orig_filter_column->convertToFullColumnIfConst();
    if (orig_filter_column->isColumnNullable())
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(orig_filter_column.get());
        const auto & nested_column_data = static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
        for (size_t i = 0; i < nullable_column->size(); ++i)
        {
            if (filter_column[i] == 0)
                continue;
            if (nullable_column->isNullAt(i))
                filter_column[i] = null_as_true;
            else
                filter_column[i] = filter_column[i] && nested_column_data[i];
        }
    }
    else
    {
        const auto * other_filter_column = checkAndGetColumn<ColumnVector<UInt8>>(orig_filter_column.get());
        const auto & other_filter_column_data = static_cast<const ColumnVector<UInt8> *>(other_filter_column)->getData();
        for (size_t i = 0; i < other_filter_column->size(); ++i)
            filter_column[i] = filter_column[i] && other_filter_column_data[i];
    }
}

/**
 * handle other join conditions
 * Join Kind/Strictness               ALL               ANY
 *     INNER                    TiDB inner join    TiDB semi join
 *     LEFT                     TiDB left join     should not happen
 *     RIGHT                    should not happen  should not happen
 *     ANTI                     should not happen  TiDB anti semi join
 * @param block
 * @param offsets_to_replicate
 * @param left_table_columns
 * @param right_table_columns
 */
void Join::handleOtherConditions(Block & block, std::unique_ptr<IColumn::Filter> & anti_filter, std::unique_ptr<IColumn::Offsets> & offsets_to_replicate, const std::vector<size_t> & right_table_columns) const
{
    non_equal_conditions.other_cond_expr->execute(block);

    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    filter.assign(block.rows(), static_cast<UInt8>(1));
    if (!non_equal_conditions.other_cond_name.empty())
    {
        mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);
    }

    ColumnUInt8::Container row_filter(filter.size(), 0);

    if (isLeftOuterSemiFamily(kind))
    {
        const auto helper_pos = block.getPositionByName(match_helper_name);

        const auto * old_match_nullable = checkAndGetColumn<ColumnNullable>(block.safeGetByPosition(helper_pos).column.get());
        const auto & old_match_vec = static_cast<const ColumnVector<Int8> *>(old_match_nullable->getNestedColumnPtr().get())->getData();

        {
            /// we assume there is no null value in the `match-helper` column after adder<>().
            if (!mem_utils::memoryIsZero(old_match_nullable->getNullMapData().data(), old_match_nullable->getNullMapData().size()))
                throw Exception("T here shouldn't be null before merging other conditions.", ErrorCodes::LOGICAL_ERROR);
        }

        const auto rows = offsets_to_replicate->size();
        if (old_match_vec.size() != rows)
            throw Exception("Size of column match-helper must be equal to column size of left block.", ErrorCodes::LOGICAL_ERROR);

        auto match_col = ColumnInt8::create(rows, 0);
        auto & match_vec = match_col->getData();
        auto match_nullmap = ColumnUInt8::create(rows, 0);
        auto & match_nullmap_vec = match_nullmap->getData();

        /// nullmap and data of `other_eq_filter_from_in_column`.
        const ColumnUInt8::Container *eq_in_vec = nullptr, *eq_in_nullmap = nullptr;
        if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
        {
            auto orig_filter_column = block.getByName(non_equal_conditions.other_eq_cond_from_in_name).column;
            if (orig_filter_column->isColumnConst())
                orig_filter_column = orig_filter_column->convertToFullColumnIfConst();
            if (orig_filter_column->isColumnNullable())
            {
                const auto * nullable_column = checkAndGetColumn<ColumnNullable>(orig_filter_column.get());
                eq_in_vec = &static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
                eq_in_nullmap = &nullable_column->getNullMapData();
            }
            else
                eq_in_vec = &checkAndGetColumn<ColumnUInt8>(orig_filter_column.get())->getData();
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

        for (size_t i = 0; i < block.columns(); ++i)
            if (i != helper_pos)
                block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        block.safeGetByPosition(helper_pos).column = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
        return;
    }

    if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
    {
        /// other_eq_filter_from_in_column is used in anti semi join:
        /// if there is a row that return null or false for other_condition, then for anti semi join, this row should be returned.
        /// otherwise, it will check other_eq_filter_from_in_column, if other_eq_filter_from_in_column return false, this row should
        /// be returned, if other_eq_filter_from_in_column return true or null this row should not be returned.
        mergeNullAndFilterResult(block, filter, non_equal_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));
    }

    if ((isInnerJoin(kind) && original_strictness == ASTTableJoin::Strictness::All) || isRightSemiFamily(kind))
    {
        /// inner | rightSemi | rightAnti join,  just use other_filter_column to filter result
        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);
        return;
    }

    for (size_t i = 0, prev_offset = 0; i < offsets_to_replicate->size(); ++i)
    {
        size_t current_offset = (*offsets_to_replicate)[i];
        bool has_row_kept = false;
        for (size_t index = prev_offset; index < current_offset; index++)
        {
            if (original_strictness == ASTTableJoin::Strictness::Any)
            {
                /// for semi/anti join, at most one row is kept
                row_filter[index] = !has_row_kept && filter[index];
            }
            else
            {
                /// original strictness = ALL && kind = Anti should not happen
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
    if (isLeftOuterJoin(kind))
    {
        /// for left join, convert right column to null if not joined
        for (size_t right_table_column : right_table_columns)
        {
            auto & column = block.getByPosition(right_table_column);
            auto full_column = column.column->isColumnConst() ? column.column->convertToFullColumnIfConst() : column.column;
            if (!full_column->isColumnNullable())
            {
                throw Exception("Should not reach here, the right table column for left join must be nullable");
            }
            auto current_column = full_column;
            auto result_column = (*std::move(current_column)).mutate();
            static_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(*filter_column);
            column.column = std::move(result_column);
        }
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    if (isInnerJoin(kind) || isAntiJoin(kind))
    {
        /// for semi/anti join, filter out not matched rows
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}

Block Join::doJoinBlockHash(ProbeProcessInfo & probe_process_info) const
{
    assert(probe_process_info.prepare_for_probe_done);
    probe_process_info.updateStartRow();
    /// this makes a copy of `probe_process_info.block`
    Block block = probe_process_info.block;
    size_t keys_size = key_names_left.size();
    size_t existing_columns = block.columns();

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT/RIGHT_SEMI/RIGHT_ANTI_SEMI JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `ScanHashMapAfterProbe`), and they need to be skipped.
      */
    size_t num_columns_to_skip = 0;
    if (needScanHashMapAfterProbe(kind))
        num_columns_to_skip = keys_size;

    /// Add new columns to the block.
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    std::vector<size_t> right_table_column_indexes;
    right_table_column_indexes.reserve(num_columns_to_add);

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        right_table_column_indexes.push_back(i + existing_columns);
    }

    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    std::vector<size_t> right_indexes;
    right_indexes.reserve(num_columns_to_add);

    size_t rows = block.rows();
    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);

        added_columns.push_back(src_column.column->cloneEmpty());
        added_columns.back()->reserve(rows);
        right_indexes.push_back(num_columns_to_skip + i);
    }

    /// For RightSemi/RightAnti join with other conditions, using this column to record hash entries that matches keys
    /// Note: this column will record map entry addresses, so should use it carefully and better limit its usage in this function only.
    MutableColumnPtr flag_mapped_entry_helper_column = nullptr;
    if (isRightSemiFamily(kind) && non_equal_conditions.other_cond_expr != nullptr)
    {
        flag_mapped_entry_helper_column = flag_mapped_entry_helper_type->createColumn();
        flag_mapped_entry_helper_column->reserve(rows);
    }

    IColumn::Offset current_offset = 0;
    auto & filter = probe_process_info.filter;
    auto & offsets_to_replicate = probe_process_info.offsets_to_replicate;

    bool enable_spill_join = isEnableSpill();
    JoinBuildInfo join_build_info{enable_fine_grained_shuffle, fine_grained_shuffle_count, enable_spill_join, is_spilled, build_concurrency, restore_round};
    JoinPartition::probeBlock(partitions, rows, probe_process_info.key_columns, key_sizes, added_columns, probe_process_info.null_map, filter, current_offset, offsets_to_replicate, right_indexes, collators, join_build_info, probe_process_info, flag_mapped_entry_helper_column);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);
    /// For RIGHT_SEMI/RIGHT_ANTI join without other conditions, hash table has been marked already, just return empty build table header
    if (isRightSemiFamily(kind) && !flag_mapped_entry_helper_column)
    {
        return sample_block_with_columns_to_add;
    }

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & sample_col = sample_block_with_columns_to_add.getByPosition(i);
        block.insert(ColumnWithTypeAndName(std::move(added_columns[i]), sample_col.type, sample_col.name));
    }
    if (flag_mapped_entry_helper_column)
        block.insert(ColumnWithTypeAndName(std::move(flag_mapped_entry_helper_column), flag_mapped_entry_helper_type, flag_mapped_entry_helper_name));

    size_t process_rows = probe_process_info.end_row - probe_process_info.start_row;

    // if rows equal 0, we could ignore filter and offsets_to_replicate, and do not need to update start row.
    if (likely(rows != 0))
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        if (filter && !(kind == ASTTableJoin::Kind::Anti && strictness == ASTTableJoin::Strictness::All))
        {
            // If ANY INNER | RIGHT JOIN, the result will not be spilt, so the block rows must equal process_rows.
            RUNTIME_CHECK(rows == process_rows);
            for (size_t i = 0; i < existing_columns; ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(*filter, -1);
        }

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        if (offsets_to_replicate)
        {
            for (size_t i = 0; i < existing_columns; ++i)
            {
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicateRange(probe_process_info.start_row, probe_process_info.end_row, *offsets_to_replicate);
            }

            if (rows != process_rows)
            {
                if (isLeftOuterSemiFamily(kind))
                {
                    auto helper_col = block.getByName(match_helper_name).column;
                    helper_col = helper_col->cut(probe_process_info.start_row, probe_process_info.end_row);
                }
                offsets_to_replicate->assignFromSelf(probe_process_info.start_row, probe_process_info.end_row);
                if (isAntiJoin(kind) && filter != nullptr)
                    filter->assignFromSelf(probe_process_info.start_row, probe_process_info.end_row);
            }
        }
    }

    /// handle other conditions
    if (!non_equal_conditions.other_cond_name.empty() || !non_equal_conditions.other_eq_cond_from_in_name.empty())
    {
        if (!offsets_to_replicate)
            throw Exception("Should not reach here, the strictness of join with other condition must be ALL");
        handleOtherConditions(block, filter, offsets_to_replicate, right_table_column_indexes);

        if (isRightSemiFamily(kind))
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
            // Return build table header for right semi/anti join
            block = sample_block_with_columns_to_add;
        }
    }

    return block;
}

Block Join::joinBlockHash(ProbeProcessInfo & probe_process_info) const
{
    std::vector<Block> result_blocks;
    size_t result_rows = 0;
    probe_process_info.prepareForProbe(key_names_left, non_equal_conditions.left_filter_column, kind, strictness);
    while (true)
    {
<<<<<<< HEAD
        auto block = doJoinBlockHash(probe_process_info);
=======
        if (is_cancelled())
            return {};
        auto block = doJoinBlockHash(probe_process_info, join_build_info);
>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))
        assert(block);
        result_rows += block.rows();
        result_blocks.push_back(std::move(block));
        /// exit the while loop if
        /// 1. probe_process_info.all_rows_joined_finish is true, which means all the rows in current block is processed
        /// 2. the block may be expanded after join and result_rows exceeds the min_result_block_size
        if (probe_process_info.all_rows_joined_finish || (may_probe_side_expanded_after_join && result_rows >= probe_process_info.min_result_block_size))
            break;
    }
    assert(!result_blocks.empty());
    return vstackBlocks(std::move(result_blocks));
}

namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder;

template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>
{
    static size_t calTotalRightRows(const BlocksList & blocks)
    {
        size_t total_rows = 0;
        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            total_rows += rows_right;
        }
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            total_rows = std::min(total_rows, 1);
        return total_rows;
    }
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        size_t expanded_row_size = 0;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], i, total_right_rows);

        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                rows_right = std::min(rows_right, 1);
            }

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right = block_right.getByPosition(col_num).column.get();
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(*column_right, 0, rows_right);
            }
            expanded_row_size += rows_right;
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                if (expanded_row_size >= 1)
                    break;
            }
        }
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset + expanded_row_size;
        current_offset += expanded_row_size;
    }
    static void addNotFound(MutableColumns & /* dst_columns */, size_t /* num_existing_columns */, ColumnRawPtrs & /* src_left_columns */, size_t /* num_columns_to_add */, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        /// for inner all/any join, just skip this row
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset;
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, STRICTNESS>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        /// for left all/any join, mark this row as matched
        (*is_row_matched)[i - start_offset] = 1;
        (*expanded_row_size_after_join)[i - start_offset] = 1 + current_offset;
        current_offset += 1;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertFrom(*src_left_columns[col_num], i);
        for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            dst_columns[num_existing_columns + col_num]->insertDefault();
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::Any>
{
    static void addFound(MutableColumns & /* dst_columns */, size_t /* num_existing_columns */, ColumnRawPtrs & /* src_left_columns */, size_t /* num_columns_to_add */, size_t start_offset, size_t i, const BlocksList & /* blocks */, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t /* total_right_rows */)
    {
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset;
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, ASTTableJoin::Strictness::Any>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static bool allRightRowsMaybeAdded()
    {
        return false;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::All>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, ASTTableJoin::Strictness::Any>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static bool allRightRowsMaybeAdded()
    {
        return true;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuterSemi, STRICTNESS>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add - 1, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
        dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_1);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftOuter, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add - 1, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
        dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_0);
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
} // namespace

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
void Join::joinBlockCrossImpl(Block & block, ConstNullMapPtr null_map [[maybe_unused]]) const
{
    /// Add new columns to the block.
    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
    size_t rows_left = block.rows();

    ColumnRawPtrs src_left_columns(num_existing_columns);

    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = block.getByPosition(i).column.get();
    }

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);
        block.insert(src_column);
    }

    /// NOTE It would be better to use `reserve`, as well as `replicate` methods to duplicate the values of the left block.
    size_t right_table_rows = 0;
    for (const Block & block_right : blocks)
        right_table_rows += block_right.rows();

    size_t left_rows_per_iter = std::max(rows_left, 1);
    if (max_block_size > 0 && right_table_rows > 0 && non_equal_conditions.other_cond_expr != nullptr
        && CrossJoinAdder<KIND, STRICTNESS>::allRightRowsMaybeAdded())
    {
        /// if other_condition is not null, and all right columns maybe added during join, try to use multiple iter
        /// to make memory usage under control, for anti semi cross join that is converted by not in subquery,
        /// it is likely that other condition may filter out most of the rows
        left_rows_per_iter = std::max(max_block_size / right_table_rows, 1);
    }

    std::vector<size_t> right_column_index;
    for (size_t i = 0; i < num_columns_to_add; ++i)
        right_column_index.push_back(num_existing_columns + i);

    std::vector<Block> result_blocks;
    auto total_right_rows = CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::calTotalRightRows(blocks);
    for (size_t start = 0; start <= rows_left; start += left_rows_per_iter)
    {
        size_t end = std::min(start + left_rows_per_iter, rows_left);
        MutableColumns dst_columns(block.columns());
        for (size_t i = 0; i < block.columns(); ++i)
        {
            dst_columns[i] = block.getByPosition(i).column->cloneEmpty();
            size_t reserved_rows = total_right_rows * (end - start);
            if likely (reserved_rows > 0)
                dst_columns[i]->reserve(reserved_rows);
        }
        IColumn::Offset current_offset = 0;
        std::unique_ptr<IColumn::Filter> is_row_matched = std::make_unique<IColumn::Filter>(end - start);
        std::unique_ptr<IColumn::Offsets> expanded_row_size_after_join = std::make_unique<IColumn::Offsets>(end - start);
        for (size_t i = start; i < end; ++i)
        {
            if constexpr (has_null_map)
            {
                if ((*null_map)[i])
                {
                    /// filter out by left_conditions, so just treated as not joined column
                    CrossJoinAdder<KIND, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
                    continue;
                }
            }
            if (right_table_rows > 0)
            {
                CrossJoinAdder<KIND, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, blocks, is_row_matched.get(), current_offset, expanded_row_size_after_join.get(), total_right_rows);
            }
            else
            {
                CrossJoinAdder<KIND, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
            }
        }
        auto block_per_iter = block.cloneWithColumns(std::move(dst_columns));
        if (non_equal_conditions.other_cond_expr != nullptr)
            handleOtherConditions(block_per_iter, is_row_matched, expanded_row_size_after_join, right_column_index);
        if (start == 0 || block_per_iter.rows() > 0)
            /// always need to generate at least one block
            result_blocks.push_back(block_per_iter);
    }

    if (result_blocks.size() == 1)
    {
        block = result_blocks[0];
    }
    else
    {
        block = vstackBlocks(std::move(result_blocks));
    }
}

Block Join::joinBlockCross(ProbeProcessInfo & probe_process_info) const
{
    Block block = probe_process_info.block;
    size_t rows_left = block.rows();
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    recordFilteredRows(block, non_equal_conditions.left_filter_column, null_map_holder, null_map);

    std::unique_ptr<IColumn::Filter> filter = std::make_unique<IColumn::Filter>(rows_left);
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows_left);

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;

#define DISPATCH(HAS_NULL_MAP)                                                       \
    if (kind == Cross && strictness == All)                                          \
        joinBlockCrossImpl<Cross, All, HAS_NULL_MAP>(block, null_map);               \
    else if (kind == Cross && strictness == Any)                                     \
        joinBlockCrossImpl<Cross, Any, HAS_NULL_MAP>(block, null_map);               \
    else if (kind == Cross_LeftOuter && strictness == All)                           \
        joinBlockCrossImpl<Cross_LeftOuter, All, HAS_NULL_MAP>(block, null_map);     \
    else if (kind == Cross_LeftOuter && strictness == Any)                           \
        joinBlockCrossImpl<Cross_LeftOuter, Any, HAS_NULL_MAP>(block, null_map);     \
    else if (kind == Cross_Anti && strictness == All)                                \
        joinBlockCrossImpl<Cross_Anti, All, HAS_NULL_MAP>(block, null_map);          \
    else if (kind == Cross_Anti && strictness == Any)                                \
        joinBlockCrossImpl<Cross_Anti, Any, HAS_NULL_MAP>(block, null_map);          \
    else if (kind == Cross_LeftOuterSemi && strictness == All)                       \
        joinBlockCrossImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(block, null_map); \
    else if (kind == Cross_LeftOuterSemi && strictness == Any)                       \
        joinBlockCrossImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(block, null_map); \
    else if (kind == Cross_LeftOuterAnti && strictness == All)                       \
        joinBlockCrossImpl<Cross_LeftOuterSemi, All, HAS_NULL_MAP>(block, null_map); \
    else if (kind == Cross_LeftOuterAnti && strictness == Any)                       \
        joinBlockCrossImpl<Cross_LeftOuterSemi, Any, HAS_NULL_MAP>(block, null_map); \
    else                                                                             \
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    if (null_map)
    {
<<<<<<< HEAD
        DISPATCH(true)
=======
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
>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))
    }
    else
    {
        DISPATCH(false)
    }
#undef DISPATCH
    /// todo control the returned block size for cross join
    probe_process_info.all_rows_joined_finish = true;
    return block;
}

void Join::checkTypes(const Block & block) const
{
    checkTypesOfKeys(block, sample_block_with_keys);
}

Block Join::joinBlockNullAware(ProbeProcessInfo & probe_process_info) const
{
    Block block = probe_process_info.block;

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_left);

    /// Note that `extractAllKeyNullMap` must be done before `extractNestedColumnsAndNullMap`
    /// because `extractNestedColumnsAndNullMap` will change the nullable column to its nested column.
    ColumnPtr all_key_null_map_holder;
    ConstNullMapPtr all_key_null_map{};
    extractAllKeyNullMap(key_columns, all_key_null_map_holder, all_key_null_map);

    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    ColumnPtr filter_map_holder;
    ConstNullMapPtr filter_map{};
    recordFilteredRows(block, non_equal_conditions.left_filter_column, filter_map_holder, filter_map);

    size_t existing_columns = block.columns();

    /// Add new columns to the block.
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);
        block.insert(src_column);
    }

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
    if (kind == NullAware_Anti && strictness == All)
        joinBlockNullAwareImpl<NullAware_Anti, All, MapsAll>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else if (kind == NullAware_Anti && strictness == Any)
        joinBlockNullAwareImpl<NullAware_Anti, Any, MapsAny>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else if (kind == NullAware_LeftOuterSemi && strictness == All)
        joinBlockNullAwareImpl<NullAware_LeftOuterSemi, All, MapsAll>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else if (kind == NullAware_LeftOuterSemi && strictness == Any)
        joinBlockNullAwareImpl<NullAware_LeftOuterSemi, Any, MapsAny>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else if (kind == NullAware_LeftOuterAnti && strictness == All)
        joinBlockNullAwareImpl<NullAware_LeftOuterAnti, All, MapsAll>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else if (kind == NullAware_LeftOuterAnti && strictness == Any)
        joinBlockNullAwareImpl<NullAware_LeftOuterAnti, Any, MapsAny>(block, existing_columns, key_columns, null_map, filter_map, all_key_null_map);
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);

    /// Null aware join never expand the left block, just handle the whole block at one time is enough
    probe_process_info.all_rows_joined_finish = true;

    return block;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockNullAwareImpl(
    Block & block,
    size_t left_columns,
    const ColumnRawPtrs & key_columns,
    const ConstNullMapPtr & null_map,
    const ConstNullMapPtr & filter_map,
    const ConstNullMapPtr & all_key_null_map) const
{
    size_t rows = block.rows();
    std::vector<RowsNotInsertToMap *> null_rows(partitions.size(), nullptr);
    for (size_t i = 0; i < partitions.size(); ++i)
        null_rows[i] = partitions[i]->getRowsNotInsertedToMap();

    NALeftSideInfo left_side_info(null_map, filter_map, all_key_null_map);
    NARightSideInfo right_side_info(right_has_all_key_null_row.load(std::memory_order_relaxed), right_table_is_empty.load(std::memory_order_relaxed), null_key_check_all_blocks_directly, null_rows);
    auto [res, res_list] = JoinPartition::probeBlockNullAware<KIND, STRICTNESS, Maps>(
        partitions,
        block,
        key_columns,
        key_sizes,
        collators,
        left_side_info,
        right_side_info);

    RUNTIME_ASSERT(res.size() == rows, "NASemiJoinResult size {} must be equal to block size {}", res.size(), rows);

<<<<<<< HEAD
    size_t right_columns = block.columns() - left_columns;
=======
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
>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))

    if (!res_list.empty())
    {
        NASemiJoinHelper<KIND, STRICTNESS, typename Maps::MappedType::Base_t> helper(
            block,
            left_columns,
            right_columns,
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

    MutableColumns added_columns(right_columns);
    for (size_t i = 0; i < right_columns; ++i)
        added_columns[i] = block.getByPosition(i + left_columns).column->cloneEmpty();

    PaddedPODArray<Int8> * left_semi_column_data = nullptr;
    PaddedPODArray<UInt8> * left_semi_null_map = nullptr;

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi || KIND == ASTTableJoin::Kind::NullAware_LeftOuterAnti)
    {
        auto * left_semi_column = typeid_cast<ColumnNullable *>(added_columns[right_columns - 1].get());
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
            if (result == NASemiJoinResultType::TRUE_VALUE)
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
            case NASemiJoinResultType::FALSE_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(0);
                break;
            case NASemiJoinResultType::TRUE_VALUE:
                left_semi_column_data->push_back(1);
                left_semi_null_map->push_back(0);
                break;
            case NASemiJoinResultType::NULL_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(1);
                break;
            }
        }
    }

    for (size_t i = 0; i < right_columns; ++i)
    {
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
            added_columns[i]->insertManyDefaults(rows_for_anti);
        else if (i < right_columns - 1)
        {
            /// The last column is match_helper_name.
            added_columns[i]->insertManyDefaults(rows);
        }
        block.getByPosition(i + left_columns).column = std::move(added_columns[i]);
    }

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(*filter, rows_for_anti);
    }
<<<<<<< HEAD
=======
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
>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))
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

void Join::finishOneBuild()
{
    std::unique_lock lock(build_probe_mutex);
    if (active_build_threads == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_build);
    }
    --active_build_threads;
    if (active_build_threads == 0)
    {
        workAfterBuildFinish();
        build_cv.notify_all();
    }
}

void Join::workAfterBuildFinish()
{
    if (isNullAwareSemiFamily(kind))
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
            null_key_check_all_blocks_directly = static_cast<double>(null_rows_size) > static_cast<double>(total_input_build_rows) / 3.0;
    }

    if (isEnableSpill())
    {
        if (hasPartitionSpilled())
        {
            spillAllBuildPartitions();
            build_spiller->finishSpill();
        }
        for (const auto & partition : partitions)
        {
            if (!partition->isSpill() && partition->hasBuildData())
                has_build_data_in_memory = true;
        }
    }
    else
    {
        has_build_data_in_memory = !original_blocks.empty();
    }
}

void Join::workAfterProbeFinish()
{
    if (isEnableSpill())
    {
        if (hasPartitionSpilled())
        {
            spillAllProbePartitions();
            probe_spiller->finishSpill();
            if (!needScanHashMapAfterProbe(kind))
            {
                releaseAllPartitions();
            }
        }
    }
}

void Join::waitUntilAllBuildFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    build_cv.wait(lock, [&]() {
        return active_build_threads == 0 || meet_error || skip_wait;
    });
    if (meet_error)
        throw Exception(error_message);
}

void Join::finishOneProbe()
{
    std::unique_lock lock(build_probe_mutex);
    if (active_probe_threads == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_probe);
    }
    --active_probe_threads;
    if (active_probe_threads == 0)
    {
        workAfterProbeFinish();
        probe_cv.notify_all();
    }
}

void Join::waitUntilAllProbeFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    probe_cv.wait(lock, [&]() {
        return active_probe_threads == 0 || meet_error || skip_wait;
    });
    if (meet_error)
        throw Exception(error_message);
}


void Join::finishOneNonJoin(size_t partition_index)
{
    if likely (active_build_threads == 0 && active_probe_threads == 0)
    {
        /// only clear hash table if not active build/probe threads
        while (partition_index < build_concurrency)
        {
            partitions[partition_index]->releasePartition();
            partition_index += build_concurrency;
        }
    }
}

Block Join::joinBlock(ProbeProcessInfo & probe_process_info, bool dry_run) const
{
    assert(!probe_process_info.all_rows_joined_finish);
    if unlikely (dry_run)
    {
        assert(probe_process_info.block.rows() == 0);
    }
    else
    {
        if unlikely (active_build_threads != 0)
        {
            /// build is not finished yet, the query must be cancelled, so just return {}
            LOG_WARNING(log, "JoinBlock without non zero active_build_threads, return empty block");
            return {};
        }
    }
    std::shared_lock lock(rwlock);

    Block block{};

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
    if (isCrossJoin(kind))
        block = joinBlockCross(probe_process_info);
    else if (isNullAwareSemiFamily(kind))
        block = joinBlockNullAware(probe_process_info);
    else
        block = joinBlockHash(probe_process_info);

    /// for (cartesian)antiLeftSemi join, the meaning of "match-helper" is `non-matched` instead of `matched`.
    if (kind == LeftOuterAnti || kind == Cross_LeftOuterAnti)
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(block.getByName(match_helper_name).column.get());
        const auto & vec_matched = static_cast<const ColumnVector<Int8> *>(nullable_column->getNestedColumnPtr().get())->getData();

        auto col_non_matched = ColumnInt8::create(vec_matched.size());
        auto & vec_non_matched = col_non_matched->getData();

        for (size_t i = 0; i < vec_matched.size(); ++i)
            vec_non_matched[i] = !vec_matched[i];

        block.getByName(match_helper_name).column = ColumnNullable::create(std::move(col_non_matched), std::move(nullable_column->getNullMapColumnPtr()));
    }

    return block;
}

BlockInputStreamPtr Join::createScanHashMapAfterProbeStream(const Block & left_sample_block, size_t index, size_t step, size_t max_block_size) const
{
    return std::make_shared<ScanHashMapAfterProbeBlockInputStream>(*this, left_sample_block, index, step, max_block_size);
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
    computeDispatchHash(num_rows, key_columns, collators, sort_key_containers, restore_round, hash);
    return hashToSelector(hash);
}

void Join::spillMostMemoryUsedPartitionIfNeed()
{
    Int64 target_partition_index = -1;
    size_t max_bytes = 0;
    Blocks blocks_to_spill;

    {
        std::unique_lock lk(build_probe_mutex);
#ifdef DBMS_PUBLIC_GTEST
        // for join spill to disk gtest
        if (restore_round == 1 && spilled_partition_indexes.size() >= partitions.size() / 2)
            return;
#endif
        if (!disable_spill && restore_round >= 4)
        {
            LOG_INFO(log, fmt::format("restore round reach to 4, spilling will be disabled."));
            disable_spill = true;
            return;
        }
        if ((max_bytes_before_external_join && getTotalByteCount() <= max_bytes_before_external_join) || disable_spill)
        {
            return;
        }
        for (size_t j = 0; j < partitions.size(); ++j)
        {
            if (!partitions[j]->isSpill() && (target_partition_index == -1 || partitions[j]->getMemoryUsage() > max_bytes))
            {
                target_partition_index = j;
                max_bytes = partitions[j]->getMemoryUsage();
            }
        }
        if (target_partition_index == -1)
        {
            return;
        }

        RUNTIME_CHECK_MSG(build_concurrency > 1, "spilling is not is not supported when stream size = 1, please increase max_threads or set max_bytes_before_external_join = 0.");
        is_spilled = true;

        LOG_INFO(log, fmt::format("Join with restore round: {}, used {} bytes, will spill partition: {}.", restore_round, getTotalByteCount(), target_partition_index));

        std::unique_lock partition_lock = partitions[target_partition_index]->lockPartition();
        partitions[target_partition_index]->markSpill();
        partitions[target_partition_index]->releasePartitionPoolAndHashMap(partition_lock);
        blocks_to_spill = partitions[target_partition_index]->trySpillBuildPartition(true, build_spill_config.max_cached_data_bytes_in_spiller, partition_lock);
        spilled_partition_indexes.push_back(target_partition_index);
    }
    build_spiller->spillBlocks(std::move(blocks_to_spill), target_partition_index);
    LOG_DEBUG(log, fmt::format("all bytes used after spill: {}", getTotalByteCount()));
}

bool Join::getPartitionSpilled(size_t partition_index)
{
    return partitions[partition_index]->isSpill();
}


bool Join::hasPartitionSpilledWithLock()
{
    std::unique_lock lk(build_probe_mutex);
    return hasPartitionSpilled();
}

bool Join::hasPartitionSpilled()
{
    return !spilled_partition_indexes.empty();
}

std::optional<RestoreInfo> Join::getOneRestoreStream(size_t max_block_size_)
{
    std::unique_lock lock(build_probe_mutex);
    if (meet_error)
        throw Exception(error_message);
    try
    {
        LOG_TRACE(log, fmt::format("restore_build_streams {}, restore_probe_streams {}, restore_scan_hash_map_streams {}", restore_build_streams.size(), restore_build_streams.size(), restore_scan_hash_map_streams.size()));
        assert(restore_build_streams.size() == restore_probe_streams.size() && restore_build_streams.size() == restore_scan_hash_map_streams.size());
        auto get_back_stream = [](BlockInputStreams & streams) {
            BlockInputStreamPtr stream = streams.back();
            streams.pop_back();
            return stream;
        };
        if (!restore_build_streams.empty())
        {
            auto build_stream = get_back_stream(restore_build_streams);
            auto probe_stream = get_back_stream(restore_probe_streams);
            auto scan_hash_map_stream = get_back_stream(restore_scan_hash_map_streams);
            if (restore_build_streams.empty())
            {
<<<<<<< HEAD
                spilled_partition_indexes.pop_front();
=======
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
>>>>>>> 8aba9f0ce3 (join be aware of cancel signal (#9450))
            }
            return RestoreInfo{restore_join, std::move(scan_hash_map_stream), std::move(build_stream), std::move(probe_stream)};
        }
        if (spilled_partition_indexes.empty())
        {
            return {};
        }
        auto spilled_partition_index = spilled_partition_indexes.front();
        RUNTIME_CHECK_MSG(partitions[spilled_partition_index]->isSpill(), "should not restore unspilled partition.");
        if (restore_join_build_concurrency <= 0)
            restore_join_build_concurrency = getRestoreJoinBuildConcurrency(partitions.size(), spilled_partition_indexes.size(), join_restore_concurrency, probe_concurrency);
        /// for restore join we make sure that the build concurrency is at least 2, so it can be spill again
        assert(restore_join_build_concurrency >= 2);
        LOG_INFO(log, "Begin restore data from disk for hash join, partition {}, restore round {}, build concurrency {}.", spilled_partition_index, restore_round, restore_join_build_concurrency);
        restore_build_streams = build_spiller->restoreBlocks(spilled_partition_index, restore_join_build_concurrency, true);
        restore_probe_streams = probe_spiller->restoreBlocks(spilled_partition_index, restore_join_build_concurrency, true);
        restore_scan_hash_map_streams.resize(restore_join_build_concurrency, nullptr);
        RUNTIME_CHECK_MSG(restore_build_streams.size() == static_cast<size_t>(restore_join_build_concurrency), "restore streams size must equal to restore_join_build_concurrency");
        auto new_max_bytes_before_external_join = static_cast<size_t>(max_bytes_before_external_join * (static_cast<double>(restore_join_build_concurrency) / build_concurrency));
        restore_join = createRestoreJoin(std::max(1, new_max_bytes_before_external_join));
        restore_join->initBuild(build_sample_block, restore_join_build_concurrency);
        restore_join->setInitActiveBuildThreads();
        restore_join->initProbe(probe_sample_block, restore_join_build_concurrency);
        for (Int64 i = 0; i < restore_join_build_concurrency; i++)
        {
            restore_build_streams[i] = std::make_shared<HashJoinBuildBlockInputStream>(restore_build_streams[i], restore_join, i, log->identifier());
        }
        auto build_stream = get_back_stream(restore_build_streams);
        auto probe_stream = get_back_stream(restore_probe_streams);
        if (restore_build_streams.empty())
        {
            spilled_partition_indexes.pop_front();
        }
        if (needScanHashMapAfterProbe(kind))
        {
            for (Int64 i = 0; i < restore_join_build_concurrency; i++)
                restore_scan_hash_map_streams[i] = restore_join->createScanHashMapAfterProbeStream(probe_stream->getHeader(), i, restore_join_build_concurrency, max_block_size_);
        }
        auto scan_hash_map_streams = get_back_stream(restore_scan_hash_map_streams);
        return RestoreInfo{restore_join, std::move(scan_hash_map_streams), std::move(build_stream), std::move(probe_stream)};
    }
    catch (...)
    {
        restore_build_streams.clear();
        restore_probe_streams.clear();
        restore_scan_hash_map_streams.clear();
        auto err_message = getCurrentExceptionMessage(false, true);
        meetErrorImpl(err_message, lock);
        throw Exception(err_message);
    }
}

void Join::dispatchProbeBlock(Block & block, PartitionBlocks & partition_blocks_list)
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
                blocks_to_spill = partitions[i]->trySpillProbePartition(false, probe_spill_config.max_cached_data_bytes_in_spiller, partition_lock);
                need_spill = true;
            }
        }
        if (need_spill)
        {
            probe_spiller->spillBlocks(std::move(blocks_to_spill), i);
        }
        else
        {
            partition_blocks_list.emplace_back(i, std::move(partition_blocks[i]));
        }
    }
}

void Join::spillAllBuildPartitions()
{
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        build_spiller->spillBlocks(partitions[i]->trySpillBuildPartition(true, build_spill_config.max_cached_data_bytes_in_spiller), i);
    }
}

void Join::spillAllProbePartitions()
{
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        probe_spiller->spillBlocks(partitions[i]->trySpillProbePartition(true, probe_spill_config.max_cached_data_bytes_in_spiller), i);
    }
}

void Join::releaseAllPartitions()
{
    for (auto & partition : partitions)
    {
        partition->releasePartition();
    }
}

} // namespace DB
