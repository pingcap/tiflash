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

#pragma once

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

enum class SemiJoinStep : UInt8
{
    /// Check other conditions for the right rows whose join key are equal to this left row.
    /// The join keys of this left row must not have null.
    CHECK_OTHER_COND,
    /// Check join key equal condition and other conditions(if any) for the right rows
    /// in null rows(i.e. all rows in right table with null join key).
    /// The join keys of this left row must not have null.
    CHECK_NULL_ROWS_NOT_NULL,
    /// Like `CHECK_NULL_ROWS_NOT_NULL` except the join keys of this left row must have null.
    CHECK_NULL_ROWS_NULL,
    /// Check join key equal condition and other conditions(if any) for all right rows in blocks.
    /// The join keys of this left row must have null.
    CHECK_ALL_BLOCKS,
    /// Work is done.
    DONE,
};

enum class SemiJoinResultType : UInt8
{
    FALSE_VALUE,
    TRUE_VALUE,
    NULL_VALUE,
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
class SemiJoinResult
{
public:
    SemiJoinResult(size_t row_num, SemiJoinStep step, const void * map_it);

    /// For convenience, caller can only consider the result of semi join.
    /// This function will correct the result if it's not semi join.
    template <SemiJoinResultType RES>
    void setResult()
    {
        step = SemiJoinStep::DONE;
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi)
        {
            result = RES;
            return;
        }
        /// For (left) anti semi join
        if constexpr (RES == SemiJoinResultType::FALSE_VALUE)
            result = SemiJoinResultType::TRUE_VALUE;
        else if constexpr (RES == SemiJoinResultType::TRUE_VALUE)
            result = SemiJoinResultType::FALSE_VALUE;
        else
            result = SemiJoinResultType::NULL_VALUE;
    }

    SemiJoinResultType getResult() const
    {
        if (unlikely(step != SemiJoinStep::DONE))
            throw Exception("null-aware semi join result is not ready");
        return result;
    }

    inline SemiJoinStep getStep() const
    {
        return step;
    }

    inline size_t getRowNum() const
    {
        return row_num;
    }

    template <typename Mapped, SemiJoinStep STEP>
    void fillRightColumns(MutableColumns & added_columns, size_t left_columns, size_t right_columns, const PaddedPODArray<Join::RowRef> & null_rows, size_t & current_offset, size_t max_pace);

    template <SemiJoinStep STEP>
    void checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end);

    template <SemiJoinStep STEP>
    void checkExprResult(ConstNullMapPtr eq_null_map, const PaddedPODArray<UInt8> & other_column, ConstNullMapPtr other_null_map, size_t offset_begin, size_t offset_end);

    template <SemiJoinStep STEP>
    void checkStepEnd();

private:
    size_t row_num;

    SemiJoinStep step;
    bool step_end;
    SemiJoinResultType result;

    /// Iterating position of null list.
    size_t null_rows_pos;

    /// Mapped data for one cell.
    const void * map_it;
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
class SemiJoinHelper
{
public:
    using Result = SemiJoinResult<KIND, STRICTNESS>;

    SemiJoinHelper(
        Block & block,
        size_t left_columns,
        size_t right_columns,
        const BlocksList & right_blocks,
        const PaddedPODArray<Join::RowRef> & null_rows,
        size_t max_block_size,
        const JoinConditions & conditions);

    void joinResult(std::list<Result *> & res_list);

private:
    template <SemiJoinStep STEP>
    void runStep(std::list<Result *> & res_list, std::list<Result *> & next_res_list);

    void runStepAllBlocks(std::list<Result *> & res_list);

    template <SemiJoinStep STEP>
    void checkAllExprResult(Block & exec_block, const std::vector<size_t> & offsets, std::list<Result *> & res_list, std::list<Result *> & next_res_list);

private:
    Block & block;
    size_t left_columns;
    size_t right_columns;
    const BlocksList & right_blocks;
    const PaddedPODArray<Join::RowRef> & null_rows;
    size_t max_block_size;

    const JoinConditions & conditions;
};

} // namespace DB
