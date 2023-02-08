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

enum class NullAwareSemiJoinStep
{
    /// Check other conditions for the right rows whose join key are equal to this left row.
    /// The join keys of this left row must not have null.
    CHECK_OTHER_COND,
    /// Check join key equal condition and other conditions(if any) for the right rows
    /// in null list(i.e. all rows in right table with null join key).
    /// The join keys of this left row may have null.
    CHECK_NULL_LIST,
    /// Check join key equal condition and other conditions(if any) for all right rows in blocks.
    /// The join keys of this left row must have null.
    CHECK_ALL_BLOCKS,
    /// Work is done.
    DONE,
};

class NullAwareSemiJoinResult
{
public:
    NullAwareSemiJoinResult(size_t row_num, bool has_null_join_key, NullAwareSemiJoinStep step, const void * map_it);

    void setResult(bool is_null, bool res)
    {
        step = NullAwareSemiJoinStep::DONE;
        result = std::make_pair(is_null, res);
    }

    std::pair<bool, bool> getResult() const
    {
        if (unlikely(step != NullAwareSemiJoinStep::DONE))
            throw Exception("null-aware semi join result is not ready");
        return result;
    }

    inline NullAwareSemiJoinStep getStep() const
    {
        return step;
    }

    inline size_t getRowNum() const
    {
        return row_num;
    }

private:
    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map, NullAwareSemiJoinStep STEP>
    void fillRightColumns(MutableColumns & added_columns, size_t left_columns, size_t right_columns, const std::vector<std::unique_ptr<Join::RowRefList>> & null_lists, size_t & current_offset, size_t max_pace);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
    void checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
    void checkExprResult(ConstNullMapPtr eq_null_map, const PaddedPODArray<UInt8> & other_column, ConstNullMapPtr other_null_map, size_t offset_begin, size_t offset_end);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
    void checkStepEnd();

private:
    size_t row_num;
    bool has_null_join_key;

    NullAwareSemiJoinStep step;
    bool step_end;

    /// Null list
    size_t null_pos;
    Join::RowRefList * null_list;

    /// Mapped data for one cell.
    const void * map_it;

    /// (is_null, result)
    std::pair<bool, bool> result;
};

class NullAwareSemiJoinHelper
{
public:
    NullAwareSemiJoinHelper(
        Block & block,
        size_t left_columns,
        size_t right_columns,
        const BlocksList & right_blocks,
        const std::vector<std::unique_ptr<Join::RowRefList>> & null_lists,
        size_t max_block_size,
        const String & other_filter_column,
        const ExpressionActionsPtr & other_condition_ptr,
        const String & null_aware_eq_column,
        const ExpressionActionsPtr & null_aware_eq_ptr);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
    void joinResult(std::list<NullAwareSemiJoinResult *> & res_list);

private:
    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map, NullAwareSemiJoinStep STEP>
    void runStep(std::list<NullAwareSemiJoinResult *> & res_list, std::list<NullAwareSemiJoinResult *> & next_res_list);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    void runStepAllBlocks(std::list<NullAwareSemiJoinResult *> & res_list);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, NullAwareSemiJoinStep STEP>
    void checkAllExprResult(Block & exec_block, const std::vector<size_t> & offsets, std::list<NullAwareSemiJoinResult *> & res_list, std::list<NullAwareSemiJoinResult *> & next_res_list);

private:
    Block & block;
    size_t left_columns;
    size_t right_columns;
    const BlocksList & right_blocks;
    const std::vector<std::unique_ptr<Join::RowRefList>> & null_lists;
    size_t max_block_size;

    const String & other_filter_column;
    const ExpressionActionsPtr & other_condition_ptr;
    const String & null_aware_eq_column;
    const ExpressionActionsPtr & null_aware_eq_ptr;
};

} // namespace DB
