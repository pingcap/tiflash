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

#include <Columns/ColumnConst.h>
#include <Interpreters/NullAwareSemiJoin.h>

namespace DB
{

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
SemiJoinResult<KIND, STRICTNESS>::SemiJoinResult(size_t row_num, SemiJoinStep step, const void * map_it)
    : row_num(row_num)
    , step(step)
    , step_end(false)
    , result(SemiJoinResultType::NULL_VALUE)
    , null_rows_pos(0)
    , map_it(map_it)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <typename Mapped, SemiJoinStep STEP>
void SemiJoinResult<KIND, STRICTNESS>::fillRightColumns(MutableColumns & added_columns, size_t left_columns, size_t right_columns, const PaddedPODArray<Join::RowRef> & null_rows, size_t & current_offset, size_t max_pace)
{
    static_assert(STEP == SemiJoinStep::CHECK_OTHER_COND || STEP == SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL || STEP == SemiJoinStep::CHECK_NULL_ROWS_NULL);

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type<SemiJoinStep>::type>(step), static_cast<std::underlying_type<SemiJoinStep>::type>(STEP));

    if constexpr (STEP == SemiJoinStep::CHECK_OTHER_COND)
    {
        static_assert(STRICTNESS == ASTTableJoin::Strictness::All);

        const auto * iter = static_cast<const Mapped *>(map_it);
        for (size_t i = 0; i < max_pace && iter != nullptr; ++i)
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
    else if constexpr (STEP == SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL || STEP == SemiJoinStep::CHECK_NULL_ROWS_NULL)
    {
        for (size_t i = 0; i < max_pace; ++i)
        {
            if (null_rows_pos >= null_rows.size())
            {
                step_end = true;
                break;
            }

            for (size_t j = 0; j < right_columns; ++j)
                added_columns[j + left_columns]->insertFrom(*null_rows[null_rows_pos].block->getByPosition(j).column.get(), null_rows[null_rows_pos].row_num);
            ++current_offset;

            ++null_rows_pos;
        }
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <SemiJoinStep STEP>
void SemiJoinResult<KIND, STRICTNESS>::checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(STRICTNESS == ASTTableJoin::Strictness::Any);
    static_assert(STEP != SemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        /// Only care about null map because if the equal expr can be true, the result
        /// should be known during probing hash table, not here.
        if ((*eq_null_map)[i])
        {
            /// equal expr is NULL, the result is NULL.
            /// I.e. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<SemiJoinResultType::NULL_VALUE>();
            return;
        }
    }

    checkStepEnd<STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <SemiJoinStep STEP>
void SemiJoinResult<KIND, STRICTNESS>::checkExprResult(ConstNullMapPtr eq_null_map, const PaddedPODArray<UInt8> & other_column, ConstNullMapPtr other_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(STRICTNESS == ASTTableJoin::Strictness::All);
    static_assert(STEP != SemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        if ((other_null_map && (*other_null_map)[i]) || !other_column[i])
        {
            /// If other expr is NULL or 0, this right row is not included in the result set.
            continue;
        }
        if constexpr (STEP == SemiJoinStep::CHECK_OTHER_COND)
        {
            /// other expr is true, so the result is true for this row that has matched right row(s).
            setResult<SemiJoinResultType::TRUE_VALUE>();
            return;
        }
        if ((*eq_null_map)[i])
        {
            /// other expr is true and equal expr is NULL, the result is NULL.
            /// I.e. (1,2) in ((1,null)) or (1,2) in ((null,2)) or (1,null) in ((1,2)).
            setResult<SemiJoinResultType::NULL_VALUE>();
            return;
        }
    }

    checkStepEnd<STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
template <SemiJoinStep STEP>
void SemiJoinResult<KIND, STRICTNESS>::checkStepEnd()
{
    if (!step_end)
        return;

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type<SemiJoinStep>::type>(step), static_cast<std::underlying_type<SemiJoinStep>::type>(STEP));

    if constexpr (STEP == SemiJoinStep::CHECK_OTHER_COND)
    {
        step = SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL;
        step_end = false;
    }
    else if constexpr (STEP == SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL)
    {
        /// If it doesn't have null join key, the result is false after checking all right rows
        /// with null join key.
        /// I.e. (1,2) in () or (1,2) in ((1,3),(2,2),(2,null),(null,1)).
        setResult<SemiJoinResultType::FALSE_VALUE>();
    }
    else if constexpr (STEP == SemiJoinStep::CHECK_NULL_ROWS_NULL)
    {
        /// If it has null join key, the next step is to check all block in right table.
        /// Although there are some repeated rows in null list that have already been checked,
        /// the implementation of checking all blocks is likely to be more efficient than iterating
        /// the hash table and copy them one by one to the block.
        step = SemiJoinStep::CHECK_ALL_BLOCKS;
        step_end = false;
    }
    else if constexpr (STEP == SemiJoinStep::CHECK_ALL_BLOCKS)
    {
        /// If step is CHECK_ALL_BLOCKS, step end is checked by outside.
        RUNTIME_CHECK_MSG(false, "Step of CHECK_ALL_BLOCKS should not have a true step_end");
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
SemiJoinHelper<KIND, STRICTNESS, Mapped>::SemiJoinHelper(
    Block & block,
    size_t left_columns,
    size_t right_columns,
    const BlocksList & right_blocks,
    const PaddedPODArray<Join::RowRef> & null_rows,
    size_t max_block_size,
    const String & other_filter_column,
    const ExpressionActionsPtr & other_condition_ptr,
    const String & null_aware_eq_column,
    const ExpressionActionsPtr & null_aware_eq_ptr)
    : block(block)
    , left_columns(left_columns)
    , right_columns(right_columns)
    , right_blocks(right_blocks)
    , null_rows(null_rows)
    , max_block_size(max_block_size)
    , other_filter_column(other_filter_column)
    , other_condition_ptr(other_condition_ptr)
    , null_aware_eq_column(null_aware_eq_column)
    , null_aware_eq_ptr(null_aware_eq_ptr)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);
    static_assert(STRICTNESS == ASTTableJoin::Strictness::Any || STRICTNESS == ASTTableJoin::Strictness::All);

    RUNTIME_CHECK(block.columns() == left_columns + right_columns);

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftAnti || KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
    {
        /// The last column is `match_helper`.
        right_columns -= 1;
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
void SemiJoinHelper<KIND, STRICTNESS, Mapped>::joinResult(std::list<SemiJoinHelper::Result *> & res_list)
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        /// Step of CHECK_OTHER_COND only exist when strictness is all.
        std::list<SemiJoinHelper::Result *> next_step_res_list;
        runStep<SemiJoinStep::CHECK_OTHER_COND>(res_list, next_step_res_list);
        res_list.swap(next_step_res_list);
    }

    if (res_list.empty())
        return;

    std::list<SemiJoinHelper::Result *> next_step_res_list;
    runStep<SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL>(res_list, next_step_res_list);
    res_list.swap(next_step_res_list);
    if (res_list.empty())
        return;

    runStep<SemiJoinStep::CHECK_NULL_ROWS_NULL>(res_list, next_step_res_list);
    res_list.swap(next_step_res_list);
    if (res_list.empty())
        return;

    runStepAllBlocks(res_list);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <SemiJoinStep STEP>
void SemiJoinHelper<KIND, STRICTNESS, Mapped>::runStep(std::list<SemiJoinHelper::Result *> & res_list, std::list<SemiJoinHelper::Result *> & next_res_list)
{
    static_assert(STEP == SemiJoinStep::CHECK_OTHER_COND || STEP == SemiJoinStep::CHECK_NULL_ROWS_NOT_NULL || STEP == SemiJoinStep::CHECK_NULL_ROWS_NULL);

    auto it = res_list.begin();
    while (it != res_list.end())
    {
        if ((*it)->getStep() != STEP)
        {
            if ((*it)->getStep() != SemiJoinStep::DONE)
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
        MutableColumns columns(block_columns);
        for (size_t i = 0; i < block_columns; ++i)
        {
            /// Reuse the column to avoid memory allocation.
            /// Correctness depends on the fact that equal and other condition expressions do not
            /// removed any column, namely, the columns will not out of order.
            /// TODO: Maybe we can reuse the memory of new columns added from expressions?
            columns[i] = exec_block.safeGetByPosition(i).column->assumeMutable();
            columns[i]->popBack(columns[i]->size());
        }

        size_t max_pace = max_block_size / res_list.size();
        size_t current_offset = 0;
        offsets.clear();

        for (auto & res : res_list)
        {
            size_t prev_offset = current_offset;
            res->template fillRightColumns<Mapped, STEP>(columns, left_columns, right_columns, null_rows, current_offset, max_pace);

            for (size_t i = 0; i < left_columns; ++i)
                columns[i]->insertManyFrom(*block.getByPosition(i).column.get(), res->getRowNum(), current_offset - prev_offset);

            offsets.push_back(current_offset);
            if (current_offset >= max_block_size)
                break;
        }

        /// Move the columns to exec_block.
        /// Note that this can remove the new columns that are added in the last loop
        /// from equal and other condition expressions.
        exec_block = block.cloneWithColumns(std::move(columns));

        checkAllExprResult<STEP>(
            exec_block,
            offsets,
            res_list,
            next_res_list);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
void SemiJoinHelper<KIND, STRICTNESS, Mapped>::runStepAllBlocks(std::list<SemiJoinHelper::Result *> & res_list)
{
    // Should always be empty, just for sanity check.
    std::list<SemiJoinHelper::Result *> next_res_list;
    std::vector<size_t> offsets(1);
    while (!res_list.empty())
    {
        SemiJoinHelper::Result * res = *res_list.begin();
        for (const auto & right_block : right_blocks)
        {
            if (res->getStep() == SemiJoinStep::DONE)
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

            checkAllExprResult<SemiJoinStep::CHECK_ALL_BLOCKS>(
                exec_block,
                offsets,
                res_list,
                next_res_list);
        }
        if (res->getStep() != SemiJoinStep::DONE)
        {
            /// After iterating to the end of right blocks, the result is false;
            /// I.e. (1,null) in () or (1,null,2) in ((2,null,2),(1,null,3),(null,1,4)).
            res->template setResult<SemiJoinResultType::FALSE_VALUE>();
            res_list.pop_front();
        }
    }
    RUNTIME_CHECK_MSG(next_res_list.empty(), "next_res_list should be empty");
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <SemiJoinStep STEP>
void SemiJoinHelper<KIND, STRICTNESS, Mapped>::checkAllExprResult(Block & exec_block, const std::vector<size_t> & offsets, std::list<SemiJoinHelper::Result *> & res_list, std::list<SemiJoinHelper::Result *> & next_res_list)
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        other_condition_ptr->execute(exec_block);
    }

    ConstNullMapPtr eq_null_map = nullptr;
    if constexpr (STEP != SemiJoinStep::CHECK_OTHER_COND)
    {
        null_aware_eq_ptr->execute(exec_block);

        auto eq_column = exec_block.getByName(null_aware_eq_column).column;
        if (eq_column->isColumnConst())
            eq_column = eq_column->convertToFullColumnIfConst();

        RUNTIME_CHECK_MSG(eq_column->isColumnNullable(), "The equal column of null-aware semi join should be nullable, otherwise Anti/LeftAnti/LeftSemi should be used instead");

        const auto * nullable_eq_column = static_cast<const ColumnNullable *>(eq_column.get());
        eq_null_map = &nullable_eq_column->getNullMapData();
    }
    else
    {
        /// If STEP is CHECK_OTHER_COND, other condition must exist so STRICTNESS must be all.
        static_assert(STRICTNESS == ASTTableJoin::Strictness::All);
    }

    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
    {
        size_t prev_offset = 0;
        auto it = res_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(eq_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == SemiJoinStep::DONE)
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
        auto other_column = exec_block.getByName(other_filter_column).column;
        if (other_column->isColumnConst())
            other_column = other_column->convertToFullColumnIfConst();

        const PaddedPODArray<UInt8> * other_column_data = nullptr;
        ConstNullMapPtr other_null_map = nullptr;
        if (other_column->isColumnNullable())
        {
            const auto * nullable_other_column = static_cast<const ColumnNullable *>(other_column.get());
            other_column_data = &static_cast<const ColumnVector<UInt8> *>(nullable_other_column->getNestedColumnPtr().get())->getData();
            other_null_map = &nullable_other_column->getNullMapData();
        }
        else
        {
            other_column_data = &static_cast<const ColumnVector<UInt8> *>(other_column.get())->getData();
        }

        size_t prev_offset = 0;
        auto it = res_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
        {
            (*it)->template checkExprResult<STEP>(eq_null_map, *other_column_data, other_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == SemiJoinStep::DONE)
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

template class SemiJoinResult<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::Any>;
template class SemiJoinResult<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::All>;

template class SemiJoinResult<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::Any>;
template class SemiJoinResult<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::All>;

template class SemiJoinResult<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::Any>;
template class SemiJoinResult<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::All>;

template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::All, Join::RowRefList>;

template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::All, Join::RowRefList>;

template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class SemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::All, Join::RowRefList>;

} // namespace DB