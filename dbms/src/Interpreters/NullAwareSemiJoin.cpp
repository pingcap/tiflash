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

NullAwareSemiJoinResult::NullAwareSemiJoinResult(size_t row_num, bool has_null_join_key, NullAwareSemiJoinStep step, const void * map_it)
    : row_num(row_num)
    , has_null_join_key(has_null_join_key)
    , step(step)
    , step_end(false)
    , null_pos(0)
    , null_list(nullptr)
    , map_it(map_it)
{}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped, NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinResult::fillRightColumns(MutableColumns & added_columns, size_t left_columns, size_t right_columns, const std::vector<std::unique_ptr<Join::RowRefList>> & null_lists, size_t & current_offset, size_t max_pace)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);

    static_assert(STEP == NullAwareSemiJoinStep::CHECK_OTHER_COND || STEP == NullAwareSemiJoinStep::CHECK_NULL_LIST);

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type<NullAwareSemiJoinStep>::type>(step), static_cast<std::underlying_type<NullAwareSemiJoinStep>::type>(STEP));

    if constexpr (STEP == NullAwareSemiJoinStep::CHECK_OTHER_COND)
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
    else if constexpr (STEP == NullAwareSemiJoinStep::CHECK_NULL_LIST)
    {
        for (size_t i = 0; i < max_pace; ++i)
        {
            while (null_list == nullptr)
            {
                if (null_pos + 1 > null_lists.size())
                {
                    step_end = true;
                    break;
                }
                null_pos += 1;
                null_list = null_lists[null_pos - 1]->next;
            }
            if (step_end)
                break;

            for (size_t j = 0; j < right_columns; ++j)
                added_columns[j + left_columns]->insertFrom(*null_list->block->getByPosition(j).column.get(), null_list->row_num);
            ++current_offset;

            null_list = null_list->next;
        }
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinResult::checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);
    static_assert(STRICTNESS == ASTTableJoin::Strictness::Any);
    static_assert(STEP != NullAwareSemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        // Only care about null map because if the equal expr can be true, the result
        // should be known during probing hash table, not here.
        if ((*eq_null_map)[i])
        {
            setResult(true, false);
            return;
        }
    }

    checkStepEnd<KIND, STRICTNESS, STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinResult::checkExprResult(ConstNullMapPtr eq_null_map, const PaddedPODArray<UInt8> & other_column, ConstNullMapPtr other_null_map, size_t offset_begin, size_t offset_end)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);
    static_assert(STRICTNESS == ASTTableJoin::Strictness::All);
    static_assert(STEP != NullAwareSemiJoinStep::DONE);

    for (size_t i = offset_begin; i < offset_end; i++)
    {
        if (other_null_map && (*other_null_map)[i])
            continue;
        if (!other_column[i])
            continue;
        if constexpr (STEP == NullAwareSemiJoinStep::CHECK_OTHER_COND)
        {
            if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
                setResult(false, true);
            else
                setResult(false, false);
            return;
        }
        if ((*eq_null_map)[i])
        {
            setResult(true, false);
            return;
        }
    }

    checkStepEnd<KIND, STRICTNESS, STEP>();
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinResult::checkStepEnd()
{
    if (!step_end)
        return;

    RUNTIME_CHECK_MSG(step == STEP, "current step {} != caller's step {}", static_cast<std::underlying_type<NullAwareSemiJoinStep>::type>(step), static_cast<std::underlying_type<NullAwareSemiJoinStep>::type>(STEP));

    if constexpr (STEP == NullAwareSemiJoinStep::CHECK_OTHER_COND)
    {
        step = NullAwareSemiJoinStep::CHECK_NULL_LIST;
        step_end = false;
    }
    else if constexpr (STEP == NullAwareSemiJoinStep::CHECK_NULL_LIST)
    {
        if (has_null_join_key)
        {
            // If it has null join key, the next step is to check all block in right table.
            // Although there are some repeated rows in null list that have already been checked,
            // the implementation of checking all blocks is likely to be more efficient than iterating
            // the hash table and copy them one by one to the block.
            step = NullAwareSemiJoinStep::CHECK_ALL_BLOCKS;
            step_end = false;
        }
        else
        {
            if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
                setResult(false, false);
            else
                setResult(false, true);
        }
    }
    else if constexpr (STEP == NullAwareSemiJoinStep::CHECK_ALL_BLOCKS)
    {
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
            setResult(false, false);
        else
            setResult(false, true);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
NullAwareSemiJoinHelper<KIND, STRICTNESS, Mapped>::NullAwareSemiJoinHelper(
    Block & block,
    size_t left_columns,
    size_t right_columns,
    const BlocksList & right_blocks,
    const std::vector<std::unique_ptr<Join::RowRefList>> & null_lists,
    size_t max_block_size,
    const String & other_filter_column,
    const ExpressionActionsPtr & other_condition_ptr,
    const String & null_aware_eq_column,
    const ExpressionActionsPtr & null_aware_eq_ptr)
    : block(block)
    , left_columns(left_columns)
    , right_columns(right_columns)
    , right_blocks(right_blocks)
    , null_lists(null_lists)
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
void NullAwareSemiJoinHelper<KIND, STRICTNESS, Mapped>::joinResult(std::list<NullAwareSemiJoinResult *> & res_list)
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        std::list<NullAwareSemiJoinResult *> next_step_res_list;
        runStep<NullAwareSemiJoinStep::CHECK_OTHER_COND>(res_list, next_step_res_list);
        res_list.swap(next_step_res_list);
    }

    if (res_list.empty())
        return;

    std::list<NullAwareSemiJoinResult *> next_step_res_list;
    runStep<NullAwareSemiJoinStep::CHECK_NULL_LIST>(res_list, next_step_res_list);
    res_list.swap(next_step_res_list);

    if (res_list.empty())
        return;

    runStepAllBlocks(res_list);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinHelper<KIND, STRICTNESS, Mapped>::runStep(std::list<NullAwareSemiJoinResult *> & res_list, std::list<NullAwareSemiJoinResult *> & next_res_list)
{
    static_assert(STEP == NullAwareSemiJoinStep::CHECK_OTHER_COND || STEP == NullAwareSemiJoinStep::CHECK_NULL_LIST);

    auto it = res_list.begin();
    while (it != res_list.end())
    {
        if ((*it)->getStep() != STEP)
        {
            if ((*it)->getStep() != NullAwareSemiJoinStep::DONE)
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
            // Reuse the column to avoid memory allocation.
            // Correctness depends on the fact that equal and other condition expressions do not
            // removed any column, namely, the columns will not out of order.
            // TODO: Maybe we can reuse the memory of new columns added from expressions?
            columns[i] = exec_block.safeGetByPosition(i).column->assumeMutable();
            columns[i]->popBack(columns[i]->size());
        }

        size_t max_pace = max_block_size / res_list.size();
        size_t current_offset = 0;
        offsets.clear();

        for (auto & res : res_list)
        {
            size_t prev_offset = current_offset;
            res->fillRightColumns<KIND, STRICTNESS, Mapped, STEP>(columns, left_columns, right_columns, null_lists, current_offset, max_pace);
            // TODO: optimize performance.
            for (size_t i = 0; i < left_columns; ++i)
            {
                for (size_t j = prev_offset; j < current_offset; ++j)
                {
                    columns[i]->insertFrom(*block.getByPosition(i).column.get(), res->getRowNum());
                }
            }
            offsets.push_back(current_offset);
            if (current_offset >= max_block_size)
                break;
        }

        // Move the columns to exec_block.
        // Note that this can remove the new columns that are added in the last loop
        // from equal and other condition expressions.
        exec_block = block.cloneWithColumns(std::move(columns));

        checkAllExprResult<STEP>(
            exec_block,
            offsets,
            res_list,
            next_res_list);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
void NullAwareSemiJoinHelper<KIND, STRICTNESS, Mapped>::runStepAllBlocks(std::list<NullAwareSemiJoinResult *> & res_list)
{
    // Should always empty, just for sanity check.
    std::list<NullAwareSemiJoinResult *> next_res_list;
    std::vector<size_t> offsets(1);
    while (!res_list.empty())
    {
        NullAwareSemiJoinResult * res = *res_list.begin();
        for (const auto & right_block : right_blocks)
        {
            if (res->getStep() == NullAwareSemiJoinStep::DONE)
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

            checkAllExprResult<NullAwareSemiJoinStep::CHECK_ALL_BLOCKS>(
                exec_block,
                offsets,
                res_list,
                next_res_list);
        }
        if (res->getStep() != NullAwareSemiJoinStep::DONE)
        {
            // After iterating to the end of right blocks, the result is not null.
            // So the result should be false for (left)semi join, true for anti semi join.
            if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
                res->setResult(false, false);
            else
                res->setResult(false, true);

            res_list.pop_front();
        }
    }
    RUNTIME_CHECK_MSG(next_res_list.empty(), "next_res_list should be empty");
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
template <NullAwareSemiJoinStep STEP>
void NullAwareSemiJoinHelper<KIND, STRICTNESS, Mapped>::checkAllExprResult(Block & exec_block, const std::vector<size_t> & offsets, std::list<NullAwareSemiJoinResult *> & res_list, std::list<NullAwareSemiJoinResult *> & next_res_list)
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        other_condition_ptr->execute(exec_block);
    }

    ConstNullMapPtr eq_null_map = nullptr;
    if constexpr (STEP != NullAwareSemiJoinStep::CHECK_OTHER_COND)
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
        // If STEP is CHECK_OTHER_COND, other condition must exist so STRICTNESS must be all.
        static_assert(STRICTNESS == ASTTableJoin::Strictness::All);
    }

    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
    {
        size_t prev_offset = 0;
        auto it = res_list.begin();
        for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
        {
            (*it)->checkExprResult<KIND, STRICTNESS, STEP>(eq_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == NullAwareSemiJoinStep::DONE)
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
            (*it)->checkExprResult<KIND, STRICTNESS, STEP>(eq_null_map, *other_column_data, other_null_map, prev_offset, offsets[i]);

            if ((*it)->getStep() == NullAwareSemiJoinStep::DONE)
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

template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_Anti, ASTTableJoin::Strictness::All, Join::RowRefList>;

template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftSemi, ASTTableJoin::Strictness::All, Join::RowRefList>;

template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::Any, Join::RowRef>;
template class NullAwareSemiJoinHelper<ASTTableJoin::Kind::NullAware_LeftAnti, ASTTableJoin::Strictness::All, Join::RowRefList>;

} // namespace DB