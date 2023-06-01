// Copyright 2023 PingCAP, Ltd.
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
#include <Core/ColumnNumbers.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/CrossJoinProbeHelper.h>
#include <Interpreters/Join.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/NullableUtils.h>
#include <common/logger_useful.h>

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
std::pair<const ColumnUInt8::Container *, const ColumnUInt8::Container *> getDataAndNullMapVectorFromFilterColumn(ColumnPtr & filter_column)
{
    if (filter_column->isColumnConst())
        filter_column = filter_column->convertToFullColumnIfConst();
    if (filter_column->isColumnNullable())
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(filter_column.get());
        const auto & data_column = nullable_column->getNestedColumnPtr();
        return {&checkAndGetColumn<ColumnUInt8>(data_column.get())->getData(), &nullable_column->getNullMapData()};
    }
    else
    {
        return {&checkAndGetColumn<ColumnUInt8>(filter_column.get())->getData(), nullptr};
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
    const Names & tidb_output_column_names_,
    const TiDB::TiDBCollators & collators_,
    const JoinNonEqualConditions & non_equal_conditions_,
    size_t max_block_size_,
    size_t shallow_copy_cross_probe_threshold_,
    const String & match_helper_name_,
    const String & flag_mapped_entry_helper_name_,
    size_t restore_round_,
    bool is_test_,
    const std::vector<RuntimeFilterPtr> runtime_filter_list_)
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
    , runtime_filter_list(runtime_filter_list_)
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , build_spill_config(build_spill_config_)
    , probe_spill_config(probe_spill_config_)
    , join_restore_concurrency(join_restore_concurrency_)
    , shallow_copy_cross_probe_threshold(shallow_copy_cross_probe_threshold_ > 0 ? shallow_copy_cross_probe_threshold_ : std::max(1, max_block_size / 10))
    , tidb_output_column_names(tidb_output_column_names_)
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

    if (unlikely(kind == ASTTableJoin::Kind::Cross_RightOuter))
        throw Exception("Cross right outer join should be converted to cross Left outer join during compile");
    RUNTIME_CHECK(!(isNecessaryKindToUseRowFlaggedHashMap(kind) && strictness == ASTTableJoin::Strictness::Any));
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
        tidb_output_column_names,
        collators,
        non_equal_conditions,
        max_block_size,
        shallow_copy_cross_probe_threshold,
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
    if (max_bytes_before_external_join > 0)
    {
        if (join_map_method == JoinMapMethod::CROSS)
        {
            /// todo support spill for cross join
            max_bytes_before_external_join = 0;
            LOG_WARNING(log, "Join does not support spill, reason: cross join spill is not supported");
        }
        if (isNullAwareSemiFamily(kind))
        {
            max_bytes_before_external_join = 0;
            LOG_WARNING(log, "Join does not support spill, reason: null aware join spill is not supported");
        }
        if (!Spiller::supportSpill(build_sample_block))
        {
            max_bytes_before_external_join = 0;
            LOG_WARNING(log, "Join does not support spill, reason: input data from build side contains only constant columns");
        }
        if (max_bytes_before_external_join > 0)
            build_spiller = std::make_unique<Spiller>(build_spill_config, false, build_concurrency_, build_sample_block, log);
    }
    setSampleBlock(sample_block);
}

void Join::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    std::unique_lock lock(rwlock);
    setProbeConcurrency(probe_concurrency_);
    probe_sample_block = sample_block;
    if (max_bytes_before_external_join > 0)
    {
        if (!Spiller::supportSpill(probe_sample_block))
        {
            max_bytes_before_external_join = 0;
            build_spiller = nullptr;
            LOG_WARNING(log, "Join does not support spill, reason: input data from probe side contains only constant columns");
        }
        else
        {
            probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, build_concurrency, probe_sample_block, log);
        }
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

    // generator in runtime filter
    generateRuntimeFilterValues(block);
}

void Join::generateRuntimeFilterValues(const Block & block)
{
    LOG_DEBUG(log, "begin to generate rf values for one block in join id, block rows:{}", block.rows());
    for (const auto & rf : runtime_filter_list)
    {
        auto column_with_type_and_name = block.getByName(rf->getSourceColumnName());
        LOG_DEBUG(log, "update rf values in join, values size:{}", column_with_type_and_name.column->size());
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

void mergeNullAndFilterResult(Block & block, ColumnVector<UInt8>::Container & filter_column, const String & filter_column_name, bool null_as_true)
{
    if (filter_column_name.empty())
        return;
    ColumnPtr current_filter_column = block.getByName(filter_column_name).column;
    auto [filter_vec, nullmap_vec] = getDataAndNullMapVectorFromFilterColumn(current_filter_column);
    if (nullmap_vec != nullptr)
    {
        for (size_t i = 0; i < nullmap_vec->size(); ++i)
        {
            if (filter_column[i] == 0)
                continue;
            if ((*nullmap_vec)[i])
                filter_column[i] = null_as_true;
            else
                filter_column[i] = filter_column[i] && (*filter_vec)[i];
        }
    }
    else
    {
        for (size_t i = 0; i < filter_vec->size(); ++i)
            filter_column[i] = filter_column[i] && (*filter_vec)[i];
    }
}

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
void Join::handleOtherConditions(Block & block, std::unique_ptr<IColumn::Filter> & anti_filter, std::unique_ptr<IColumn::Offsets> & offsets_to_replicate, const std::vector<size_t> & right_table_columns) const
{
    non_equal_conditions.other_cond_expr->execute(block);

    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    filter.assign(block.rows(), static_cast<UInt8>(1));
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);

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

        for (size_t i = 0; i < block.columns(); ++i)
            if (i != helper_pos)
                block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        block.safeGetByPosition(helper_pos).column = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
        return;
    }

    /// other_eq_filter_from_in_column is used in anti semi join:
    /// if there is a row that return null or false for other_condition, then for anti semi join, this row should be returned.
    /// otherwise, it will check other_eq_filter_from_in_column, if other_eq_filter_from_in_column return false, this row should
    /// be returned, if other_eq_filter_from_in_column return true or null this row should not be returned.
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));

    if ((isInnerJoin(kind) && original_strictness == ASTTableJoin::Strictness::All) || isNecessaryKindToUseRowFlaggedHashMap(kind))
    {
        /// inner | rightSemi | rightAnti | rightOuter join,  just use other_filter_column to filter result
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

// Now this function only support cross join, todo support hash join
void Join::handleOtherConditionsForOneProbeRow(Block & block, ProbeProcessInfo & probe_process_info) const
{
    assert(kind != ASTTableJoin::Kind::Cross_RightOuter);
    /// inside this function, we can ensure that
    /// 1. probe_process_info.offsets_to_replicate.size() == 1
    /// 2. probe_process_info.offsets_to_replicate[0] == block.rows()
    /// 3. for anti semi join: probe_process_info.filter[0] == 1
    /// 4. for left outer semi join: match_helper_column[0] == 1
    assert(probe_process_info.offsets_to_replicate->size() == 1);
    assert((*probe_process_info.offsets_to_replicate)[0] == block.rows());

    non_equal_conditions.other_cond_expr->execute(block);
    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    filter.assign(block.rows(), static_cast<UInt8>(1));
    mergeNullAndFilterResult(block, filter, non_equal_conditions.other_cond_name, false);
    UInt64 matched_row_count_in_current_block = 0;
    if (isLeftOuterSemiFamily(kind) && !non_equal_conditions.other_eq_cond_from_in_name.empty())
    {
        assert(probe_process_info.has_row_matched == false);
        ColumnPtr eq_in_column = block.getByName(non_equal_conditions.other_eq_cond_from_in_name).column;
        auto [eq_in_vec, eq_in_nullmap] = getDataAndNullMapVectorFromFilterColumn(eq_in_column);
        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (!filter[i])
                continue;
            if (eq_in_nullmap && (*eq_in_nullmap)[i])
                probe_process_info.has_row_null = true;
            else if ((*eq_in_vec)[i])
            {
                probe_process_info.has_row_matched = true;
                break;
            }
        }
    }
    else
    {
        mergeNullAndFilterResult(block, filter, non_equal_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));
        matched_row_count_in_current_block = countBytesInFilter(filter);
        probe_process_info.has_row_matched |= matched_row_count_in_current_block != 0;
    }
    /// case 1, inner join
    if (kind == ASTTableJoin::Kind::Cross && original_strictness == ASTTableJoin::Strictness::All)
    {
        if (matched_row_count_in_current_block > 0)
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, matched_row_count_in_current_block);
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
        assert(original_strictness == ASTTableJoin::Strictness::All);
        if (matched_row_count_in_current_block > 0)
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, matched_row_count_in_current_block);
        }
        else if (probe_process_info.isCurrentProbeRowFinished() && !probe_process_info.has_row_matched)
        {
            /// no matched rows for current row, return the un-matched result
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
            filter.resize(1);
            for (size_t right_table_column : probe_process_info.right_column_index)
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
        }
        else
            block = block.cloneEmpty();
        return;
    }
    /// case 3, semi join
    if (kind == ASTTableJoin::Kind::Cross && original_strictness == ASTTableJoin::Strictness::Any)
    {
        if (probe_process_info.has_row_matched)
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
        if (probe_process_info.has_row_matched)
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
        if (probe_process_info.has_row_matched || probe_process_info.isCurrentProbeRowFinished())
        {
            for (size_t i = 0; i < block.columns(); ++i)
                block.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);
            auto match_col = ColumnInt8::create(1, 0);
            auto & match_vec = match_col->getData();
            auto match_nullmap = ColumnUInt8::create(1, 0);
            auto & match_nullmap_vec = match_nullmap->getData();
            if (probe_process_info.has_row_matched)
                match_vec[0] = 1;
            else if (probe_process_info.has_row_null)
                match_nullmap_vec[0] = 1;
            block.getByName(match_helper_name).column = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
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

Block Join::doJoinBlockHash(ProbeProcessInfo & probe_process_info) const
{
    assert(probe_process_info.prepare_for_probe_done);
    probe_process_info.updateStartRow<false>();
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
    if (useRowFlaggedHashMap(kind, has_other_condition))
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
                offsets_to_replicate->assign(offsets_to_replicate->begin() + probe_process_info.start_row, offsets_to_replicate->begin() + probe_process_info.end_row);
            }
        }
    }

    /// handle other conditions
    if (has_other_condition)
    {
        assert(offsets_to_replicate != nullptr);
        handleOtherConditions(block, filter, offsets_to_replicate, right_table_column_indexes);

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
                block = sample_block_with_columns_to_add;
            }
            else if (kind == ASTTableJoin::Kind::RightOuter)
            {
                block.erase(flag_mapped_entry_helper_name);
                if (!non_equal_conditions.other_cond_name.empty())
                {
                    block.erase(non_equal_conditions.other_cond_name);
                }
                if (!non_equal_conditions.other_eq_cond_from_in_name.empty())
                {
                    block.erase(non_equal_conditions.other_eq_cond_from_in_name);
                }
            }
        }
    }

    return block;
}

Block Join::removeUselessColumn(Block & block) const
{
    Block projected_block;
    for (const auto & name : tidb_output_column_names)
    {
        auto & column = block.getByName(name);
        projected_block.insert(std::move(column));
    }
    return projected_block;
}

Block Join::joinBlockHash(ProbeProcessInfo & probe_process_info) const
{
    std::vector<Block> result_blocks;
    size_t result_rows = 0;
    probe_process_info.prepareForHashProbe(key_names_left, non_equal_conditions.left_filter_column, kind, strictness);
    while (true)
    {
        auto block = doJoinBlockHash(probe_process_info);
        assert(block);
        block = removeUselessColumn(block);
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

Block Join::doJoinBlockCross(ProbeProcessInfo & probe_process_info) const
{
    /// Add new columns to the block.
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
            handleOtherConditions(block, probe_process_info.filter, probe_process_info.offsets_to_replicate, probe_process_info.right_column_index);
        }
        return block;
    }
    else if (cross_probe_mode == CrossProbeMode::SHALLOW_COPY_RIGHT_BLOCK)
    {
        probe_process_info.updateStartRow<true>();
        auto [block, is_matched_rows] = crossProbeBlockShallowCopyRightBlock(kind, strictness, probe_process_info, original_blocks);
        if (is_matched_rows)
        {
            if (non_equal_conditions.other_cond_expr != nullptr)
            {
                probe_process_info.cutFilterAndOffsetVector(0, 1);
                /// for matched rows, each call to `doJoinBlockCross` only handle part of the probed data for one left row, the internal
                /// state is saved in `probe_process_info`
                handleOtherConditionsForOneProbeRow(block, probe_process_info);
            }
            for (size_t i = 0; i < probe_process_info.block.columns(); ++i)
            {
                if (block.getByPosition(i).column->isColumnConst())
                    block.getByPosition(i).column = block.getByPosition(i).column->convertToFullColumnIfConst();
            }
            if (isLeftOuterSemiFamily(kind))
            {
                auto helper_index = probe_process_info.block.columns() + probe_process_info.right_column_index.size() - 1;
                if (block.getByPosition(helper_index).column->isColumnConst())
                    block.getByPosition(helper_index).column = block.getByPosition(helper_index).column->convertToFullColumnIfConst();
            }
        }
        else if (non_equal_conditions.other_cond_expr != nullptr)
        {
            probe_process_info.cutFilterAndOffsetVector(0, block.rows());
            handleOtherConditions(block, probe_process_info.filter, probe_process_info.offsets_to_replicate, probe_process_info.right_column_index);
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
        sample_block_with_columns_to_add,
        right_rows_to_be_added_when_matched_for_cross_join,
        cross_probe_mode,
        blocks.size());

    std::vector<Block> result_blocks;
    size_t result_rows = 0;

    while (true)
    {
        Block block = doJoinBlockCross(probe_process_info);
        assert(block);
        block = removeUselessColumn(block);
        result_rows += block.rows();
        result_blocks.push_back(std::move(block));
        if (probe_process_info.all_rows_joined_finish || (may_probe_side_expanded_after_join && result_rows >= probe_process_info.min_result_block_size))
            break;
    }

    assert(!result_blocks.empty());
    return vstackBlocks(std::move(result_blocks));
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

    return removeUselessColumn(block);
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

    size_t right_columns = block.columns() - left_columns;

    if (!res_list.empty())
    {
        NASemiJoinHelper<KIND, STRICTNESS, typename Maps::MappedType::Base_t> helper(
            block,
            left_columns,
            right_columns,
            blocks,
            null_rows,
            max_block_size,
            non_equal_conditions);

        helper.joinResult(res_list);

        RUNTIME_CHECK_MSG(res_list.empty(), "NASemiJoinResult list must be empty after calculating join result");
    }

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

    if (isCrossJoin(kind))
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
            right_rows_to_be_added_when_matched_for_cross_join = std::min(right_rows_to_be_added_when_matched_for_cross_join, 1);
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

    // set rf is ready
    finalizeRuntimeFilter();
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

bool Join::isAllProbeFinished() const
{
    std::lock_guard lock(build_probe_mutex);
    return active_probe_threads == 0 || meet_error || skip_wait;
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

BlockInputStreamPtr Join::createScanHashMapAfterProbeStream(const Block & left_sample_block, size_t index, size_t step, size_t max_block_size_) const
{
    return std::make_shared<ScanHashMapAfterProbeBlockInputStream>(*this, left_sample_block, index, step, max_block_size_);
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
                spilled_partition_indexes.pop_front();
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
