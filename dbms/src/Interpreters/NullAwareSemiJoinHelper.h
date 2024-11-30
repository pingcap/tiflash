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

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/CancellationHook.h>
#include <Interpreters/SemiJoinHelper.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
struct RowsNotInsertToMap;
enum class NASemiJoinStep : UInt8
{
    /// Check other conditions for the right rows whose join key are equal to this left row.
    /// The join keys of this left row must not have null.
    NOT_NULL_KEY_CHECK_MATCHED_ROWS,
    /// Check join key equal condition and other conditions(if any) for the right rows
    /// with at least one null join key.
    /// The join keys of this left row must not have null.
    NOT_NULL_KEY_CHECK_NULL_ROWS,
    /// Like `CHECK_NULL_ROWS_NOT_NULL` except the join keys of this left row must have null.
    NULL_KEY_CHECK_NULL_ROWS,
    /// Check join key equal condition and other conditions(if any) for all right rows in blocks.
    /// The join keys of this left row must have null.
    NULL_KEY_CHECK_ALL_BLOCKS,
    /// Work is done.
    DONE,
};

struct NARightSideInfo
{
    NARightSideInfo(
        bool has_all_key_null_row_,
        bool is_empty_,
        bool null_key_check_all_blocks_directly_,
        std::vector<RowsNotInsertToMap *> & null_rows_)
        : has_all_key_null_row(has_all_key_null_row_)
        , is_empty(is_empty_)
        , null_key_check_all_blocks_directly(null_key_check_all_blocks_directly_)
        , null_rows(null_rows_)
    {}
    const bool has_all_key_null_row;
    const bool is_empty;
    const bool null_key_check_all_blocks_directly;
    const std::vector<RowsNotInsertToMap *> & null_rows;
};

struct NALeftSideInfo
{
    NALeftSideInfo(
        const ConstNullMapPtr & null_map_,
        const ConstNullMapPtr & filter_map_,
        const ConstNullMapPtr & all_key_null_map_)
        : null_map(null_map_)
        , filter_map(filter_map_)
        , all_key_null_map(all_key_null_map_)
    {}
    const ConstNullMapPtr & null_map;
    const ConstNullMapPtr & filter_map;
    const ConstNullMapPtr & all_key_null_map;
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
class NASemiJoinResult
{
public:
    NASemiJoinResult(size_t row_num, NASemiJoinStep step, const void * map_it);

    /// For convenience, callers can only consider the result of left outer semi join.
    /// This function will correct the result if it's not left outer semi join.
    template <SemiJoinResultType RES>
    void setResult()
    {
        step = NASemiJoinStep::DONE;
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi)
        {
            result = RES;
            return;
        }
        /// For (left outer) anti semi join
        if constexpr (RES == SemiJoinResultType::FALSE_VALUE)
            result = SemiJoinResultType::TRUE_VALUE;
        else if constexpr (RES == SemiJoinResultType::TRUE_VALUE)
            result = SemiJoinResultType::FALSE_VALUE;
        else
            result = SemiJoinResultType::NULL_VALUE;
    }

    SemiJoinResultType getResult() const
    {
        if (unlikely(step != NASemiJoinStep::DONE))
            throw Exception("null-aware semi join result is not ready");
        return result;
    }

    inline NASemiJoinStep getStep() const { return step; }

    inline size_t getRowNum() const { return row_num; }

    template <typename Mapped, NASemiJoinStep STEP>
    void fillRightColumns(
        MutableColumns & added_columns,
        size_t left_columns,
        size_t right_columns,
        const std::vector<size_t> & right_column_indices_to_add,
        const std::vector<RowsNotInsertToMap *> & null_rows,
        size_t & current_offset,
        size_t max_pace);

    template <NASemiJoinStep STEP>
    void checkExprResult(ConstNullMapPtr eq_null_map, size_t offset_begin, size_t offset_end);

    template <NASemiJoinStep STEP>
    void checkExprResult(
        ConstNullMapPtr eq_null_map,
        const ColumnUInt8::Container & other_column,
        ConstNullMapPtr other_null_map,
        size_t offset_begin,
        size_t offset_end);

    template <NASemiJoinStep STEP>
    void checkStepEnd();
    size_t getNextRightBlockIndex() const { return next_right_block_index; }
    void setNextRightBlockIndex(size_t index) { next_right_block_index = index; }

private:
    size_t row_num;

    NASemiJoinStep step;
    bool step_end;
    SemiJoinResultType result;
    size_t next_right_block_index = 0;

    size_t pace;
    /// Position in null rows.
    size_t pos_in_null_rows;
    /// Position in columns vector for a certain null rows.
    size_t pos_in_columns_vector;
    /// Position in columns for a certain columns vector of a certain null rows.
    size_t pos_in_columns;

    /// Mapped data for one cell.
    const void * map_it;
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
class NASemiJoinHelper
{
public:
    using Result = NASemiJoinResult<KIND, STRICTNESS>;

    NASemiJoinHelper(
        size_t input_rows_,
        const BlocksList & right_blocks,
        std::vector<RowsNotInsertToMap *> && null_rows,
        size_t max_block_size,
        const JoinNonEqualConditions & non_equal_conditions);

    void probeHashTable(
        const JoinPartitions & join_partitions,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const NALeftSideInfo & left_side_info,
        const NARightSideInfo & right_side_info,
        const ProbeProcessInfo & probe_process_info,
        const NameSet & probe_output_name_set,
        const Block & right_sample_block);
    void doJoin();
    bool isJoinDone() const { return is_probe_hash_table_done && undetermined_result_list.empty(); }
    bool isProbeHashTableDone() const { return is_probe_hash_table_done; }
    std::vector<RowsNotInsertToMap *> & getNullRows() { return null_rows; }
    Block genJoinResult(const NameSet & output_column_names_set);

private:
    template <NASemiJoinStep STEP>
    void runStep();
    void runStepAllBlocks();

    template <NASemiJoinStep STEP>
    void runAndCheckExprResult(Block & exec_block, const std::vector<size_t> & offsets);

private:
    Block res_block;
    // used to reuse column when evaluating other conditions
    Block exec_block;
    size_t left_columns = 0;
    size_t right_columns = 0;
    std::vector<size_t> right_column_indices_to_add;
    size_t probe_rows;
    const BlocksList & right_blocks;
    std::vector<RowsNotInsertToMap *> null_rows;
    size_t max_block_size;
    PaddedPODArray<Result> join_result;
    std::list<Result *> undetermined_result_list;
    std::list<Result *> next_step_undetermined_result_list;
    bool is_probe_hash_table_done = false;
    NASemiJoinStep current_check_step;

    const JoinNonEqualConditions & non_equal_conditions;
};

#define APPLY_FOR_NULL_AWARE_SEMI_JOIN(M)                                                              \
    M(DB::ASTTableJoin::Kind::NullAware_LeftOuterSemi, DB::ASTTableJoin::Strictness::Any, DB::MapsAny) \
    M(DB::ASTTableJoin::Kind::NullAware_LeftOuterSemi, DB::ASTTableJoin::Strictness::All, DB::MapsAll) \
    M(DB::ASTTableJoin::Kind::NullAware_LeftOuterAnti, DB::ASTTableJoin::Strictness::Any, DB::MapsAny) \
    M(DB::ASTTableJoin::Kind::NullAware_LeftOuterAnti, DB::ASTTableJoin::Strictness::All, DB::MapsAll) \
    M(DB::ASTTableJoin::Kind::NullAware_Anti, DB::ASTTableJoin::Strictness::Any, DB::MapsAny)          \
    M(DB::ASTTableJoin::Kind::NullAware_Anti, DB::ASTTableJoin::Strictness::All, DB::MapsAll)

} // namespace DB
