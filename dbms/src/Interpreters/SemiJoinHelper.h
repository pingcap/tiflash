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

#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/CancellationHook.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include "Core/Block.h"
#include "Core/Names.h"

namespace DB
{

enum class SemiJoinResultType : UInt8
{
    FALSE_VALUE,
    TRUE_VALUE,
    NULL_VALUE,
};

inline bool isTrueSemiJoinResult(SemiJoinResultType result)
{
    return result == SemiJoinResultType::TRUE_VALUE;
}

inline bool isTrueSemiJoinResult(bool result)
{
    return result;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
class SemiJoinResult;

template <ASTTableJoin::Kind KIND>
class SemiJoinResult<KIND, ASTTableJoin::Strictness::Any>
{
public:
    explicit SemiJoinResult(bool res)
    {
        if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::LeftOuterSemi)
        {
            result = res;
        }
        else
        {
            result = !res;
        }
    }
    inline size_t getRowNum() const { return 0; }
    template <SemiJoinResultType RES>
    void setResult()
    {}
    template <typename Mapped>
    void fillRightColumns(MutableColumns &, size_t, size_t, const std::vector<size_t> &, size_t &, size_t)
    {}
    template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
    bool checkExprResult(
        const ColumnUInt8::Container *,
        ConstNullMapPtr,
        const ColumnUInt8::Container *,
        ConstNullMapPtr,
        size_t,
        size_t)
    {}

    bool getResult() const { return result; }

private:
    bool result;
};

template <ASTTableJoin::Kind KIND>
class SemiJoinResult<KIND, ASTTableJoin::Strictness::All>
{
public:
    SemiJoinResult(size_t row_num, const void * map_it);

    /// For convenience, callers can only consider the result of left outer semi join.
    /// This function will correct the result if it's not left outer semi join.
    template <SemiJoinResultType RES>
    void setResult()
    {
        is_done = true;
        if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::LeftOuterSemi)
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
        if (unlikely(!is_done))
            throw Exception("semi join result is not ready");
        return result;
    }

    inline size_t getRowNum() const { return row_num; }

    template <typename Mapped>
    void fillRightColumns(
        MutableColumns & added_columns,
        size_t left_columns,
        size_t right_columns,
        const std::vector<size_t> & right_column_indices_to_add,
        size_t & current_offset,
        size_t max_pace);

    template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
    bool checkExprResult(
        const ColumnUInt8::Container * other_eq_column,
        ConstNullMapPtr other_eq_null_map,
        const ColumnUInt8::Container * other_column,
        ConstNullMapPtr other_null_map,
        size_t offset_begin,
        size_t offset_end);

private:
    size_t row_num;
    bool is_done;
    bool has_null_eq_from_in;
    SemiJoinResultType result;

    size_t pace;
    /// Mapped data for one cell.
    const void * map_it;
};

struct ProbeProcessInfo;
class JoinPartition;
using JoinPartitions = std::vector<std::unique_ptr<JoinPartition>>;

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Mapped>
class SemiJoinHelper
{
public:
    SemiJoinHelper(
        size_t input_rows,
        size_t max_block_size,
        const JoinNonEqualConditions & non_equal_conditions,
        CancellationHook is_cancelled_);

    template <typename Maps>
    void probeHashTable(
        const JoinPartitions & join_partitions,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const JoinBuildInfo & join_build_info,
        const ProbeProcessInfo & probe_process_info,
        const NameSet & probe_output_name_set,
        const Block & right_sample_block);
    void doJoin();

    bool isJoinDone() const { return is_probe_hash_table_finished && probe_res_list.empty(); }

    Block genJoinResult(const NameSet & output_column_names_set);

private:
    template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
    void checkAllExprResult(
        const std::vector<size_t> & offsets,
        const ColumnUInt8::Container * other_eq_column,
        ConstNullMapPtr other_eq_null_map,
        const ColumnUInt8::Container * other_column,
        ConstNullMapPtr other_null_map);

    PaddedPODArray<SemiJoinResult<KIND, STRICTNESS>> probe_res;
    std::list<SemiJoinResult<KIND, STRICTNESS> *> probe_res_list;
    Block result_block;
    size_t left_columns = 0;
    size_t right_columns = 0;
    size_t input_rows;
    std::vector<size_t> right_column_indices_to_add;
    size_t max_block_size;
    CancellationHook is_cancelled;
    bool is_probe_hash_table_finished = false;

    const JoinNonEqualConditions & non_equal_conditions;
};

#define APPLY_FOR_SEMI_JOIN(M)                                                               \
    M(DB::ASTTableJoin::Kind::LeftOuterSemi, DB::ASTTableJoin::Strictness::Any, DB::MapsAny) \
    M(DB::ASTTableJoin::Kind::LeftOuterSemi, DB::ASTTableJoin::Strictness::All, DB::MapsAll) \
    M(DB::ASTTableJoin::Kind::LeftOuterAnti, DB::ASTTableJoin::Strictness::Any, DB::MapsAny) \
    M(DB::ASTTableJoin::Kind::LeftOuterAnti, DB::ASTTableJoin::Strictness::All, DB::MapsAll) \
    M(DB::ASTTableJoin::Kind::Semi, DB::ASTTableJoin::Strictness::Any, DB::MapsAny)          \
    M(DB::ASTTableJoin::Kind::Semi, DB::ASTTableJoin::Strictness::All, DB::MapsAll)          \
    M(DB::ASTTableJoin::Kind::Anti, DB::ASTTableJoin::Strictness::Any, DB::MapsAny)          \
    M(DB::ASTTableJoin::Kind::Anti, DB::ASTTableJoin::Strictness::All, DB::MapsAll)


} // namespace DB
