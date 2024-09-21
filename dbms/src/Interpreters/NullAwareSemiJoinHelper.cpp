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

namespace DB
{
using enum ASTTableJoin::Strictness;
using enum ASTTableJoin::Kind;

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
NASemiJoinResult<KIND, STRICTNESS>::NASemiJoinResult(size_t row_num_, NASemiJoinStep step_, const void * map_it_)
    : row_num(row_num_)
    , step(step_)
    , step_end(false)
    , result(NASemiJoinResultType::NULL_VALUE)
    , pace(1)
    , pos_in_null_rows(0)
    , pos_in_columns_vector(0)
    , pos_in_columns(0)
    , map_it(map_it_)
{
    static_assert(KIND == NullAware_Anti || KIND == NullAware_LeftOuterAnti
                  || KIND == NullAware_LeftOuterSemi);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <typename Mapped, NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::fillRightColumns(MutableColumns & added_columns, size_t left_columns, size_t right_columns, const std::vector<RowsNotInsertToMap *> & null_rows, size_t & current_offset, size_t min_pace)
{
    static_assert(STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS || STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS);

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type<NASemiJoinStep>::type>(step), static_cast<std::underlying_type<NASemiJoinStep>::type>(STEP));

    static constexpr size_t MAX_PACE = 8192;
    pace = std::min(MAX_PACE, std::max(pace, min_pace));

    if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
    {
        static_assert(STRICTNESS == All);

        const auto * iter = static_cast<const Mapped *>(map_it);
        for (size_t i = 0; i < pace && iter != nullptr; ++i)
        {
            for (size_t j = 0; j < right_columns; ++j)
                added_columns[j + left_columns]->insertFrom(*iter->block->getByPosition(j).column.get(), iter->row_num);
            ++current_offset;
            iter = iter->next;
        }
        map_it = static_cast<const void *>(iter);

        if (map_it == nullptr)
            step_end = true;
    }
    else if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS)
    {
        size_t count = pace;
        while (pos_in_null_rows < null_rows.size() && count > 0)
        {
            const auto & rows = *null_rows[pos_in_null_rows];

            while (pos_in_columns_vector < rows.materialized_columns_vec.size() && count > 0)
            {
                const auto & columns = rows.materialized_columns_vec[pos_in_columns_vector];
                const size_t columns_size = columns[0]->size();

                size_t insert_cnt = std::min(count, columns_size - pos_in_columns);
                for (size_t j = 0; j < right_columns; ++j)
                    added_columns[j + left_columns]->insertRangeFrom(*columns[j].get(), pos_in_columns, insert_cnt);

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
        current_offset += pace - count;
    }

    pace = std::min(MAX_PACE, pace * 2);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(STRICTNESS == Any);
    static_assert(STEP != NASemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        /// Only care about null map because if the equal expr can be true, the result
        /// should be known during probing hash table, not here.
        if ((*eq_null_map)[i])
        {
            /// equal expr is NULL, the result is NULL.
            /// E.g. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<NASemiJoinResultType::NULL_VALUE>();
            return;
        }
    }

    checkStepEnd<STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <NASemiJoinStep STEP>
void NASemiJoinResult<KIND, STRICTNESS>::checkExprResult(ConstNullMapPtr eq_null_map, const PaddedPODArray<UInt8> & other_column, ConstNullMapPtr other_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(STRICTNESS == All);
    static_assert(STEP != NASemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        if ((other_null_map && (*other_null_map)[i]) || !other_column[i])
        {
            /// If other expr is NULL or 0, this right row is not included in the result set.
            continue;
        }
        if constexpr (STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
        {
            /// other expr is true, so the result is true for this row that has matched right row(s).
            setResult<NASemiJoinResultType::TRUE_VALUE>();
            return;
        }
        if ((*eq_null_map)[i])
        {
            /// other expr is true and equal expr is NULL, the result is NULL.
            /// E.g. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<NASemiJoinResultType::NULL_VALUE>();
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

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type_t<NASemiJoinStep>>(step), static_cast<std::underlying_type<NASemiJoinStep>::type>(STEP));

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
        setResult<NASemiJoinResultType::FALSE_VALUE>();
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

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
NASemiJoinHelper<KIND, STRICTNESS, Mapped>::NASemiJoinHelper(
    Block & block_,
    size_t left_columns_,
    size_t right_columns_,
    const BlocksList & right_blocks_,
    const std::vector<RowsNotInsertToMap *> & null_rows_,
    size_t max_block_size_,
    const JoinNonEqualConditions & non_equal_conditions_,
    CancellationHook is_cancelled_)
    : block(block_)
    , left_columns(left_columns_)
    , right_columns(right_columns_)
    , right_blocks(right_blocks_)
    , null_rows(null_rows_)
    , max_block_size(max_block_size_)
    , is_cancelled(is_cancelled_)
    , non_equal_conditions(non_equal_conditions_)
{
    static_assert(KIND == NullAware_Anti || KIND == NullAware_LeftOuterAnti
                  || KIND == NullAware_LeftOuterSemi);
    static_assert(STRICTNESS == Any || STRICTNESS == All);

    RUNTIME_CHECK(block.columns() == left_columns + right_columns);

    if constexpr (KIND == NullAware_LeftOuterAnti || KIND == NullAware_LeftOuterSemi)
    {
        /// The last column is `match_helper`.
        right_columns -= 1;
    }

    RUNTIME_CHECK(right_columns > 0);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
void NASemiJoinHelper<KIND, STRICTNESS, Mapped>::joinResult(std::list<NASemiJoinHelper::Result *> & res_list)
{
    std::list<NASemiJoinHelper::Result *> next_step_res_list;

    if constexpr (STRICTNESS == All)
    {
        /// Step of NOT_NULL_KEY_CHECK_MATCHED_ROWS only exist when strictness is all.
        runStep<NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS>(res_list, next_step_res_list);
        res_list.swap(next_step_res_list);
    }

    if (is_cancelled() || res_list.empty())
        return;

    runStep<NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS>(res_list, next_step_res_list);
    res_list.swap(next_step_res_list);
    if (is_cancelled() || res_list.empty())
        return;

    runStep<NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS>(res_list, next_step_res_list);
    res_list.swap(next_step_res_list);
    if (is_cancelled() || res_list.empty())
        return;

    runStepAllBlocks(res_list);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <NASemiJoinStep STEP>
void NASemiJoinHelper<KIND, STRICTNESS, Mapped>::runStep(std::list<NASemiJoinHelper::Result *> & res_list, std::list<NASemiJoinHelper::Result *> & next_res_list)
{
    static_assert(STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS || STEP == NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS || STEP == NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS);

    auto it = res_list.begin();
    while (it != res_list.end())
    {
        if ((*it)->getStep() != STEP)
        {
            next_res_list.emplace_back(*it);
            it = res_list.erase(it);
            continue;
        }
        ++it;
    }

    std::vector<size_t> offsets;
    size_t block_columns = block.columns();
    Block exec_block = block.cloneEmpty();

    while (!res_list.empty())
    {
        if (is_cancelled())
            return;
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

        size_t min_pace = std::max(1, max_block_size / res_list.size());
        size_t current_offset = 0;
        offsets.clear();

        for (auto & res : res_list)
        {
            size_t prev_offset = current_offset;
            res->template fillRightColumns<Mapped, STEP>(columns, left_columns, right_columns, null_rows, current_offset, min_pace);

            /// Note that current_offset - prev_offset may be zero.
            if (current_offset > prev_offset)
            {
                for (size_t i = 0; i < left_columns; ++i)
                    columns[i]->insertManyFrom(*block.getByPosition(i).column.get(), res->getRowNum(), current_offset - prev_offset);
            }

            offsets.push_back(current_offset);
            if (current_offset >= max_block_size)
                break;
        }

        /// Move the columns to exec_block.
        /// Note that this can remove the new columns that are added in the last loop
        /// from equal and other condition expressions.
        exec_block = block.cloneWithColumns(std::move(columns));

        runAndCheckExprResult<STEP>(
            exec_block,
            offsets,
            res_list,
            next_res_list);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
void NASemiJoinHelper<KIND, STRICTNESS, Mapped>::runStepAllBlocks(std::list<NASemiJoinHelper::Result *> & res_list)
{
    std::list<NASemiJoinHelper::Result *> next_res_list;
    std::vector<size_t> offsets(1);
    while (!res_list.empty())
    {
        NASemiJoinHelper::Result * res = *res_list.begin();
        for (const auto & right_block : right_blocks)
        {
            if (is_cancelled())
                return;
            if (res->getStep() == NASemiJoinStep::DONE)
                break;

            size_t num = right_block.rows();
            if (num == 0)
                continue;

            offsets[0] = num;

            Block exec_block = block.cloneEmpty();
            for (size_t i = 0; i < left_columns; ++i)
            {
                MutableColumnPtr column = exec_block.getByPosition(i).column->assumeMutable();
                column->insertFrom(*block.getByPosition(i).column.get(), res->getRowNum());
                exec_block.getByPosition(i).column = ColumnConst::create(std::move(column), num);
            }
            for (size_t i = 0; i < right_columns; ++i)
                exec_block.getByPosition(i + left_columns).column = right_block.getByPosition(i).column;

            runAndCheckExprResult<NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS>(
                exec_block,
                offsets,
                res_list,
                next_res_list);
        }
        if (res->getStep() != NASemiJoinStep::DONE)
        {
            /// After iterating to the end of right blocks, the result is false.
            /// E.g. (1,null) in () or (1,null,2) in ((2,null,2),(1,null,3),(null,1,4)).
            res->template setResult<NASemiJoinResultType::FALSE_VALUE>();
            res_list.pop_front();
        }
    }
    /// Should always be empty, just for sanity check.
    RUNTIME_CHECK_MSG(next_res_list.empty(), "next_res_list should be empty");
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <NASemiJoinStep STEP>
void NASemiJoinHelper<KIND, STRICTNESS, Mapped>::runAndCheckExprResult(Block & exec_block, const std::vector<size_t> & offsets, std::list<NASemiJoinHelper::Result *> & res_list, std::list<NASemiJoinHelper::Result *> & next_res_list)
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
    if constexpr (STEP != NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS)
    {
        non_equal_conditions.null_aware_eq_cond_expr->execute(exec_block);

        auto eq_column = exec_block.getByName(non_equal_conditions.null_aware_eq_cond_name).column;
        if (eq_column->isColumnConst())
        {
            eq_column = eq_column->convertToFullColumnIfConst();
            /// Attention: must set the full column to the original column otherwise eq_null_map will be a dangling pointer.
            exec_block.getByName(non_equal_conditions.null_aware_eq_cond_name).column = eq_column;
        }

        RUNTIME_CHECK_MSG(eq_column->isColumnNullable(), "The null-aware equal condition column should be nullable, otherwise Anti/LeftAnti/LeftSemi should be used instead");

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
        auto it = res_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(eq_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == NASemiJoinStep::DONE)
                it = res_list.erase(it);
            else if ((*it)->getStep() == STEP)
                ++it;
            else
            {
                next_res_list.emplace_back(*it);
                it = res_list.erase(it);
            }

            prev_offset = offsets[i];
        }
    }
    else
    {
        auto other_column = exec_block.getByName(non_equal_conditions.other_cond_name).column;
        if (other_column->isColumnConst())
            other_column = other_column->convertToFullColumnIfConst();

        const PaddedPODArray<UInt8> * other_column_data = nullptr;
        ConstNullMapPtr other_null_map = nullptr;
        if (other_column->isColumnNullable())
        {
            const auto * nullable_other_column = typeid_cast<const ColumnNullable *>(other_column.get());
            other_column_data = &typeid_cast<const ColumnVector<UInt8> *>(nullable_other_column->getNestedColumnPtr().get())->getData();
            other_null_map = &nullable_other_column->getNullMapData();
        }
        else
        {
            other_column_data = &typeid_cast<const ColumnVector<UInt8> *>(other_column.get())->getData();
        }

        size_t prev_offset = 0;
        auto it = res_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(eq_null_map, *other_column_data, other_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == NASemiJoinStep::DONE)
                it = res_list.erase(it);
            else if ((*it)->getStep() == STEP)
                ++it;
            else
            {
                next_res_list.emplace_back(*it);
                it = res_list.erase(it);
            }

            prev_offset = offsets[i];
        }
    }
}

#define M(KIND, STRICTNESS, MAPTYPE) \
    template class NASemiJoinResult<KIND, STRICTNESS>;
APPLY_FOR_NULL_AWARE_JOIN(M)
#undef M

#define M(KIND, STRICTNESS, MAPTYPE) \
    template class NASemiJoinHelper<KIND, STRICTNESS, MAPTYPE::MappedType::Base_t>;
APPLY_FOR_NULL_AWARE_JOIN(M)
#undef M
} // namespace DB
