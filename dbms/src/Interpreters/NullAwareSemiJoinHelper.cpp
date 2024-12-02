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

#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Interpreters/JoinPartition.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/ProbeProcessInfo.h>

#include "Functions/FunctionBinaryArithmetic.h"

namespace DB
{
using enum ASTTableJoin::Strictness;
using enum ASTTableJoin::Kind;

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
NASemiJoinResult<KIND, STRICTNESS>::NASemiJoinResult(size_t row_num_, NASemiJoinStep step_, const void * map_it_)
    : row_num(row_num_)
    , step(step_)
    , step_end(false)
    , result(SemiJoinResultType::NULL_VALUE)
    , pace(2)
    , pos_in_null_rows(0)
    , pos_in_columns_vector(0)
    , pos_in_columns(0)
    , map_it(map_it_)
{
    static_assert(KIND == NullAware_Anti || KIND == NullAware_LeftOuterAnti || KIND == NullAware_LeftOuterSemi);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <typename Mapped, NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::fillRightColumns(
    MutableColumns & added_columns,
    size_t left_columns,
    size_t right_columns,
    const std::vector<size_t> & right_column_indices_to_add,
    const std::vector<RowsNotInsertToMap *> & null_rows,
    size_t & current_offset,
    size_t max_pace)
{
    static_assert(
        STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS || STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS
        || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS);

    RUNTIME_CHECK_MSG(
        step == STEP,
        "current step {} != caller's step {}",
        static_cast<std::underlying_type<NASemiJoinStep>::type>(step),
        static_cast<std::underlying_type<NASemiJoinStep>::type>(STEP));

    static constexpr size_t MAX_PACE = 8192;
    size_t current_pace;
    if (pace > max_pace)
    {
        current_pace = max_pace;
    }
    else
    {
        current_pace = pace;
        pace = std::min(MAX_PACE, pace * 2);
    }

    if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
    {
        static_assert(STRICTNESS == All);

        const auto * iter = static_cast<const Mapped *>(map_it);
        for (size_t i = 0; i < current_pace && iter != nullptr; ++i)
        {
            for (size_t j = 0; j < right_columns; ++j)
                added_columns[j + left_columns]->insertFrom(
                    *iter->block->getByPosition(right_column_indices_to_add[j]).column.get(),
                    iter->row_num);
            ++current_offset;
            iter = iter->next;
        }
        map_it = static_cast<const void *>(iter);

        if (map_it == nullptr)
            step_end = true;
    }
    else if constexpr (
        STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS)
    {
        size_t count = current_pace;
        while (pos_in_null_rows < null_rows.size() && count > 0)
        {
            const auto & rows = *null_rows[pos_in_null_rows];

            while (pos_in_columns_vector < rows.materialized_columns_vec.size() && count > 0)
            {
                /// todo only materialize used columns
                const auto & columns = rows.materialized_columns_vec[pos_in_columns_vector];
                const size_t columns_size = columns[0]->size();

                size_t insert_cnt = std::min(count, columns_size - pos_in_columns);
                for (size_t j = 0; j < right_columns; ++j)
                    added_columns[j + left_columns]->insertRangeFrom(
                        *columns[right_column_indices_to_add[j]].get(),
                        pos_in_columns,
                        insert_cnt);

                pos_in_columns += insert_cnt;
                count -= insert_cnt;

                if (pos_in_columns >= columns_size)
                {
                    ++pos_in_columns_vector;
                    pos_in_columns = 0;
                }
            }

            if (pos_in_columns_vector >= rows.materialized_columns_vec.size())
            {
                ++pos_in_null_rows;
                pos_in_columns_vector = 0;
                pos_in_columns = 0;
            }
        }
        step_end = pos_in_null_rows >= null_rows.size();
        current_offset += current_pace - count;
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::checkExprResult(
    ConstNullMapPtr eq_null_map,
    size_t offset_begin,
    size_t offset_end)
{
    static_assert(STRICTNESS == Any);
    static_assert(STEP != NASemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; ++i)
    {
        /// Only care about null map because if the equal expr can be true, the result
        /// should be known during probing hash table, not here.
        if ((*eq_null_map)[i])
        {
            /// equal expr is NULL, the result is NULL.
            /// E.g. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<SemiJoinResultType::NULL_VALUE>();
            return;
        }
    }

    checkStepEnd<STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::checkExprResult(
    ConstNullMapPtr eq_null_map,
    const ColumnUInt8::Container & other_column,
    ConstNullMapPtr other_null_map,
    size_t offset_begin,
    size_t offset_end)
{
    static_assert(STRICTNESS == All);
    static_assert(STEP != NASemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; ++i)
    {
        if ((other_null_map && (*other_null_map)[i]) || !other_column[i])
        {
            /// If other expr is NULL or 0, this right row is not included in the result set.
            continue;
        }
        if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
        {
            /// other expr is true, so the result is true for this row that has matched right row(s).
            setResult<SemiJoinResultType::TRUE_VALUE>();
            return;
        }
        if ((*eq_null_map)[i])
        {
            /// other expr is true and equal expr is NULL, the result is NULL.
            /// E.g. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<SemiJoinResultType::NULL_VALUE>();
            return;
        }
    }

    checkStepEnd<STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::checkStepEnd()
{
    if (!step_end)
        return;

    RUNTIME_CHECK_MSG(
        step == STEP,
        "current step {} != caller's step {}",
        static_cast<std::underlying_type_t<NASemiJoinStep>>(step),
        static_cast<std::underlying_type<NASemiJoinStep>::type>(STEP));

    if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
    {
        step = NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS;
        step_end = false;
    }
    else if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS)
    {
        /// If it doesn't have null join key, the result is false after checking all right rows
        /// with null join key.
        /// E.g. (1,2) in () or (1,2) in ((1,3),(2,2),(2,null),(null,1)).
        setResult<SemiJoinResultType::FALSE_VALUE>();
    }
    else if constexpr (STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS)
    {
        /// If it has null join key, the next step is to check all block in right table.
        /// Although there are some repeated rows in null rows that have already been checked,
        /// the implementation of checking all blocks is likely to be more efficient than iterating
        /// the hash table and copy them one by one to the block.
        step = NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS;
        step_end = false;
    }
    else if constexpr (STEP == NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS)
    {
        /// If step is CHECK_ALL_BLOCKS, step end is checked by outside.
        RUNTIME_CHECK_MSG(false, "Step of CHECK_ALL_BLOCKS should not have a true step_end");
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
NASemiJoinHelper<KIND, STRICTNESS, Maps>::NASemiJoinHelper(
    size_t input_rows_,
    const BlocksList & right_blocks_,
    std::vector<RowsNotInsertToMap *> && null_rows_,
    size_t max_block_size_,
    const JoinNonEqualConditions & non_equal_conditions_)
    : probe_rows(input_rows_)
    , right_blocks(right_blocks_)
    , null_rows(std::move(null_rows_))
    , max_block_size(max_block_size_)
    , non_equal_conditions(non_equal_conditions_)
{
    static_assert(KIND == NullAware_Anti || KIND == NullAware_LeftOuterAnti || KIND == NullAware_LeftOuterSemi);
    static_assert(STRICTNESS == Any || STRICTNESS == All);
    // init current_check_step
    if constexpr (STRICTNESS == All)
        current_check_step = NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS;
    else
        current_check_step = NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
Block NASemiJoinHelper<KIND, STRICTNESS, Maps>::genJoinResult(const NameSet & output_column_names_set)
{
    assert(undetermined_result_list.empty());

    std::unique_ptr<IColumn::Filter> filter;
    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
        filter = std::make_unique<IColumn::Filter>(probe_rows);

    MutableColumnPtr left_semi_column_ptr = nullptr;
    ColumnInt8::Container * left_semi_column_data = nullptr;
    ColumnUInt8::Container * left_semi_null_map = nullptr;

    if constexpr (
        KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi || KIND == ASTTableJoin::Kind::NullAware_LeftOuterAnti)
    {
        left_semi_column_ptr = res_block.getByPosition(res_block.columns() - 1).column->cloneEmpty();
        auto * left_semi_column = typeid_cast<ColumnNullable *>(left_semi_column_ptr.get());
        left_semi_column_data = &typeid_cast<ColumnVector<Int8> &>(left_semi_column->getNestedColumn()).getData();
        left_semi_null_map = &left_semi_column->getNullMapColumn().getData();
        left_semi_column_data->reserve(probe_rows);
        left_semi_null_map->reserve(probe_rows);
    }

    size_t rows_for_anti = 0;
    for (size_t i = 0; i < probe_rows; ++i)
    {
        auto result = join_result[i].getResult();
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
        res_block.getByPosition(res_block.columns() - 1).column = std::move(left_semi_column_ptr);
    }

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            auto & column = res_block.getByPosition(i);
            if (output_column_names_set.contains(column.name))
                column.column = column.column->filter(*filter, rows_for_anti);
        }
    }
    return std::move(res_block);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void NASemiJoinHelper<KIND, STRICTNESS, Maps>::probeHashTable(
    const JoinPartitions & join_partitions,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    const NALeftSideInfo & left_side_info,
    const NARightSideInfo & right_side_info,
    const ProbeProcessInfo & probe_process_info,
    const NameSet & probe_output_name_set,
    const Block & right_sample_block)
{
    if (is_probe_hash_table_done)
        return;
    std::tie(join_result, undetermined_result_list) = JoinPartition::probeBlockNullAwareSemi<KIND, STRICTNESS, Maps>(
        join_partitions,
        probe_rows,
        key_columns,
        key_sizes,
        collators,
        left_side_info,
        right_side_info);

    RUNTIME_ASSERT(
        join_result.size() == probe_rows,
        "SemiJoinResult size {} must be equal to block size {}",
        join_result.size(),
        probe_rows);

    for (size_t i = 0; i < probe_process_info.block.columns(); ++i)
    {
        const auto & column = probe_process_info.block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
            res_block.insert(column);
    }

    left_columns = res_block.columns();

    /// Add new columns to the block.
    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        const auto & column = right_sample_block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
        {
            RUNTIME_CHECK_MSG(
                !res_block.has(column.name),
                "block from probe side has a column with the same name: {} as a column in right_sample_block",
                column.name);
            res_block.insert(column);
            right_column_indices_to_add.push_back(i);
        }
    }
    right_columns = right_column_indices_to_add.size();
    RUNTIME_CHECK(res_block.columns() == left_columns + right_columns);

    if constexpr (KIND == NullAware_LeftOuterAnti || KIND == NullAware_LeftOuterSemi)
    {
        /// The last column is `match_helper`.
        right_columns -= 1;
    }

    RUNTIME_CHECK(right_columns > 0);
    is_probe_hash_table_done = true;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void NASemiJoinHelper<KIND, STRICTNESS, Maps>::doJoin()
{
    switch (current_check_step)
    {
    case NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS:
        // constructor of NASemiJoinHelper already set the init value of next_step to NOT_NULL_KEY_CHECK_NULL_ROWS
        // if STRICTNESS is Any, we add this check only to avoid static_assert fail
        if constexpr (STRICTNESS == All)
        {
            runStep<NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS>();
            if (undetermined_result_list.empty())
            {
                // go to next step
                undetermined_result_list.swap(next_step_undetermined_result_list);
                next_step_undetermined_result_list.clear();
                current_check_step = NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS;
            }
        }
        else
        {
            // should not reach here
            throw Exception("should not reach here");
        }
        break;
    case NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS:
        runStep<NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS>();
        if (undetermined_result_list.empty())
        {
            // go to next step
            undetermined_result_list.swap(next_step_undetermined_result_list);
            next_step_undetermined_result_list.clear();
            current_check_step = NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS;
        }
        break;
    case NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS:
        runStep<NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS>();
        if (undetermined_result_list.empty())
        {
            // go to next step
            undetermined_result_list.swap(next_step_undetermined_result_list);
            next_step_undetermined_result_list.clear();
            current_check_step = NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS;
        }
        break;
    case NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS:
        runStepAllBlocks();
        break;
    default:
        throw Exception("should not reach here");
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
template <NASemiJoinStep STEP>
void NASemiJoinHelper<KIND, STRICTNESS, Maps>::runStep()
{
    static_assert(
        STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS || STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS
        || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS);

    auto it = undetermined_result_list.begin();
    while (it != undetermined_result_list.end())
    {
        if ((*it)->getStep() != STEP)
        {
            next_step_undetermined_result_list.emplace_back(*it);
            it = undetermined_result_list.erase(it);
            continue;
        }
        ++it;
    }
    if (undetermined_result_list.empty())
        return;

    std::vector<size_t> offsets;
    size_t block_columns = res_block.columns();
    if (!exec_block)
        exec_block = res_block.cloneEmpty();

    MutableColumns columns(block_columns);
    for (size_t i = 0; i < block_columns; ++i)
    {
        /// Reuse the column to avoid memory allocation.
        /// Correctness depends on the fact that equal and other condition expressions do not
        /// removed any column, namely, the columns will not out of order.
        /// TODO: Maybe we can reuse the memory of new columns added by expressions?
        columns[i] = exec_block.safeGetByPosition(i).column->assumeMutable();
        columns[i]->popBack(columns[i]->size());
    }

    size_t current_offset = 0;
    offsets.clear();

    for (auto & res : undetermined_result_list)
    {
        size_t prev_offset = current_offset;
        res->template fillRightColumns<typename Maps::MappedType, STEP>(
            columns,
            left_columns,
            right_columns,
            right_column_indices_to_add,
            null_rows,
            current_offset,
            max_block_size - current_offset);

        /// Note that current_offset - prev_offset may be zero.
        if (current_offset > prev_offset)
        {
            for (size_t i = 0; i < left_columns; ++i)
                columns[i]->insertManyFrom(
                    *res_block.getByPosition(i).column.get(),
                    res->getRowNum(),
                    current_offset - prev_offset);
        }

        offsets.emplace_back(current_offset);
        if (current_offset >= max_block_size)
            break;
    }

    /// Move the columns to exec_block.
    /// Note that this can remove the new columns that are added in the last loop
    /// from equal and other condition expressions.
    exec_block = res_block.cloneWithColumns(std::move(columns));

    runAndCheckExprResult<STEP>(exec_block, offsets);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void NASemiJoinHelper<KIND, STRICTNESS, Maps>::runStepAllBlocks()
{
    if unlikely (undetermined_result_list.empty())
        return;
    if unlikely (undetermined_result_list.front()->getStep() == NASemiJoinStep::DONE)
    {
        while (undetermined_result_list.front()->getStep() == NASemiJoinStep::DONE)
        {
            undetermined_result_list.pop_front();
            if (undetermined_result_list.empty())
                return;
        }
    }
    NASemiJoinHelper::Result * res = *undetermined_result_list.begin();
    auto next_right_block_index = res->getNextRightBlockIndex();
    assert(next_right_block_index < right_blocks.size());
    assert(res->getStep() != NASemiJoinStep::DONE);

    auto all_right_blocks_checked = [&]() {
        // all right blocks are checked, set result to false and remove it from undetermined_result_list
        res->setNextRightBlockIndex(right_blocks.size());
        if (res->getStep() != NASemiJoinStep::DONE)
        {
            /// After iterating to the end of right blocks, the result is false.
            /// E.g. (1,null) in () or (1,null,2) in ((2,null,2),(1,null,3),(null,1,4)).
            res->template setResult<SemiJoinResultType::FALSE_VALUE>();
            undetermined_result_list.pop_front();
        }
    };

    auto right_block_it = right_blocks.begin();
    std::advance(right_block_it, next_right_block_index);

    if unlikely (right_block_it->rows() == 0)
    {
        while (right_block_it->rows() == 0)
        {
            ++right_block_it;
            if (right_block_it == right_blocks.end())
            {
                all_right_blocks_checked();
                return;
            }
        }
    }

    std::vector<size_t> offsets(1);
    const auto & right_block = *right_block_it;
    size_t num = right_block.rows();

    offsets[0] = num;

    Block exec_block = res_block.cloneEmpty();
    for (size_t i = 0; i < left_columns; ++i)
    {
        MutableColumnPtr column = exec_block.getByPosition(i).column->assumeMutable();
        column->insertFrom(*res_block.getByPosition(i).column.get(), res->getRowNum());
        exec_block.getByPosition(i).column = ColumnConst::create(std::move(column), num);
    }
    for (size_t i = 0; i < right_columns; ++i)
        exec_block.getByPosition(i + left_columns).column
            = right_block.getByPosition(right_column_indices_to_add[i]).column;

    runAndCheckExprResult<NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS>(exec_block, offsets);
    if (res->getStep() == NASemiJoinStep::DONE)
    {
        // res is already been removed from res_list inside runAndCheckExprResult
        res->setNextRightBlockIndex(right_blocks.size());
    }
    ++right_block_it;
    if (right_block_it == right_blocks.end())
    {
        all_right_blocks_checked();
    }
    else
    {
        // right blocks is not checked to the end, update next_right_block_index
        res->setNextRightBlockIndex(std::distance(right_blocks.begin(), right_block_it));
    }
    // Should always be empty, just for sanity check.
    RUNTIME_CHECK_MSG(next_step_undetermined_result_list.empty(), "next_res_list should be empty");
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
template <NASemiJoinStep STEP>
void NASemiJoinHelper<KIND, STRICTNESS, Maps>::runAndCheckExprResult(
    Block & exec_block,
    const std::vector<size_t> & offsets)
{
    /// Attention: other_cond_expr must be executed first then null_aware_eq_cond_expr can be executed.
    /// Because the execution order must be the same as the construction order in compiling(in TiFlashJoin::genJoinOtherConditionsAction).
    /// Otherwise a certain expression will throw exception if these two expressions have one added column with the same name.
    ///
    /// There are three cases:
    /// 1. Strictness: Any, which means no other_cond_expr
    ///    - order: null_aware_eq_cond_expr
    /// 2. Strictness: All, Step is NOT_NULL_KEY_CHECK_MATCHED_ROWS
    ///    - order: other_cond_expr
    /// 3. Strictness: All, Step is not NOT_NULL_KEY_CHECK_MATCHED_ROWS
    ///    - order: other_cond_expr -> null_aware_eq_cond_expr
    ///
    /// In summary, it's correct as long as null_aware_eq_cond_expr is not executed solely when other_cond_expr exists.

    if constexpr (STRICTNESS == All)
        non_equal_conditions.other_cond_expr->execute(exec_block);

    ConstNullMapPtr eq_null_map = nullptr;
    ColumnPtr eq_column;
    if constexpr (STEP != NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
    {
        non_equal_conditions.null_aware_eq_cond_expr->execute(exec_block);

        eq_column = exec_block.getByName(non_equal_conditions.null_aware_eq_cond_name).column;
        if (eq_column->isColumnConst())
            eq_column = eq_column->convertToFullColumnIfConst();

        RUNTIME_CHECK_MSG(
            eq_column->isColumnNullable(),
            "The null-aware equal condition column should be nullable, otherwise Anti/LeftAnti/LeftSemi should be used "
            "instead");

        const auto * nullable_eq_column = typeid_cast<const ColumnNullable *>(eq_column.get());
        eq_null_map = &nullable_eq_column->getNullMapData();
    }
    else
    {
        /// If STEP is NOT_NULL_KEY_CHECK_MATCHED_ROWS, it means these right rows have the same join keys to the corresponding left row.
        /// So do not need to run null_aware_eq_cond_expr.
        /// And other conditions must exist so STRICTNESS must be all.
        static_assert(STRICTNESS == All);
    }

    if constexpr (STRICTNESS == Any)
    {
        size_t prev_offset = 0;
        auto it = undetermined_result_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != undetermined_result_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(eq_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == NASemiJoinStep::DONE)
                it = undetermined_result_list.erase(it);
            else if ((*it)->getStep() == STEP)
                ++it;
            else
            {
                next_step_undetermined_result_list.emplace_back(*it);
                it = undetermined_result_list.erase(it);
            }

            prev_offset = offsets[i];
        }
    }
    else
    {
        auto other_column = exec_block.getByName(non_equal_conditions.other_cond_name).column;

        auto [other_column_data, other_null_map] = getDataAndNullMapVectorFromFilterColumn(other_column);

        size_t prev_offset = 0;
        auto it = undetermined_result_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != undetermined_result_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(
                eq_null_map,
                *other_column_data,
                other_null_map,
                prev_offset,
                offsets[i]);

            if ((*it)->getStep() == NASemiJoinStep::DONE)
                it = undetermined_result_list.erase(it);
            else if ((*it)->getStep() == STEP)
                ++it;
            else
            {
                next_step_undetermined_result_list.emplace_back(*it);
                it = undetermined_result_list.erase(it);
            }

            prev_offset = offsets[i];
        }
    }
}

#define M(KIND, STRICTNESS, MAPTYPE) template class NASemiJoinResult<KIND, STRICTNESS>;
APPLY_FOR_NULL_AWARE_SEMI_JOIN(M)
#undef M

#define M(KIND, STRICTNESS, MAPTYPE) template class NASemiJoinHelper<KIND, STRICTNESS, MAPTYPE>;
APPLY_FOR_NULL_AWARE_SEMI_JOIN(M)
#undef M
} // namespace DB
