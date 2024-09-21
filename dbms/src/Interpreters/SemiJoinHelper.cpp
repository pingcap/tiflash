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

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Interpreters/JoinPartition.h>
#include <Interpreters/SemiJoinHelper.h>

namespace DB
{
using enum ASTTableJoin::Strictness;
using enum ASTTableJoin::Kind;

template <ASTTableJoin::Kind KIND>
SemiJoinResult<KIND, All>::SemiJoinResult(size_t row_num_, const void * map_it_)
    : row_num(row_num_)
    , is_done(false)
    , has_null_eq_from_in(false)
    , result(SemiJoinResultType::NULL_VALUE)
    , pace(2)
    , map_it(map_it_)
{
    static_assert(KIND == Semi || KIND == Anti || KIND == LeftOuterAnti || KIND == LeftOuterSemi);
}

template <ASTTableJoin::Kind KIND>
template <typename Mapped>
void SemiJoinResult<KIND, All>::fillRightColumns(
    MutableColumns & added_columns,
    size_t left_columns,
    size_t right_columns,
    const std::vector<size_t> & right_column_indices_to_add,
    size_t & current_offset,
    size_t max_pace)
{
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
}

template <ASTTableJoin::Kind KIND>
template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
bool SemiJoinResult<KIND, All>::checkExprResult(
    const ColumnUInt8::Container * other_eq_column,
    ConstNullMapPtr other_eq_null_map,
    const ColumnUInt8::Container * other_column,
    ConstNullMapPtr other_null_map,
    size_t offset_begin,
    size_t offset_end)
{
    static_assert(has_other_cond || has_other_eq_cond_from_in);
    if unlikely (is_done)
        return true;

    for (size_t i = offset_begin; i < offset_end; ++i)
    {
        if constexpr (has_other_cond)
        {
            if constexpr (has_other_cond_null_map)
            {
                if ((*other_null_map)[i])
                {
                    /// If other expr is NULL, this row is not included in the result set.
                    continue;
                }
            }
            if (!(*other_column)[i])
            {
                /// If other expr is 0, this row is not included in the result set.
                continue;
            }
        }
        if constexpr (has_other_eq_cond_from_in)
        {
            if ((*other_eq_null_map)[i])
            {
                has_null_eq_from_in = true;
                continue;
            }
            if ((*other_eq_column)[i])
            {
                setResult<SemiJoinResultType::TRUE_VALUE>();
                return true;
            }
        }
        else
        {
            /// other expr is true, so the result is true for this row that has matched right row(s).
            setResult<SemiJoinResultType::TRUE_VALUE>();
            return true;
        }
    }

    if (map_it == nullptr)
    {
        if constexpr (has_other_eq_cond_from_in)
        {
            if (has_null_eq_from_in)
            {
                setResult<SemiJoinResultType::NULL_VALUE>();
                return true;
            }
        }

        setResult<SemiJoinResultType::FALSE_VALUE>();
        return true;
    }

    return false;
}

template <ASTTableJoin::Kind KIND, typename Mapped>
SemiJoinHelper<KIND, Mapped>::SemiJoinHelper(
    Block & block_,
    size_t left_columns_,
    const std::vector<size_t> & right_column_indices_to_added_,
    size_t max_block_size_,
    const JoinNonEqualConditions & non_equal_conditions_,
    CancellationHook is_cancelled_)
    : block(block_)
    , left_columns(left_columns_)
    , right_column_indices_to_add(right_column_indices_to_added_)
    , max_block_size(max_block_size_)
    , is_cancelled(is_cancelled_)
    , non_equal_conditions(non_equal_conditions_)
{
    static_assert(KIND == Semi || KIND == Anti || KIND == LeftOuterAnti || KIND == LeftOuterSemi);

    right_columns = right_column_indices_to_add.size();
    RUNTIME_CHECK(block.columns() == left_columns + right_columns);

    if constexpr (KIND == LeftOuterAnti || KIND == LeftOuterSemi)
    {
        /// The last column is `match_helper`.
        right_columns -= 1;
    }

    RUNTIME_CHECK(right_columns > 0);
    RUNTIME_CHECK(non_equal_conditions.other_cond_expr != nullptr);
}

template <ASTTableJoin::Kind KIND, typename Mapped>
void SemiJoinHelper<KIND, Mapped>::joinResult(std::list<Result *> & res_list)
{
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

        size_t current_offset = 0;
        offsets.clear();

        for (auto & res : res_list)
        {
            size_t prev_offset = current_offset;
            res->template fillRightColumns<Mapped>(
                columns,
                left_columns,
                right_columns,
                right_column_indices_to_add,
                current_offset,
                max_block_size - current_offset);

            /// Note that current_offset - prev_offset may be zero.
            if (current_offset > prev_offset)
            {
                for (size_t i = 0; i < left_columns; ++i)
                    columns[i]->insertManyFrom(
                        *block.getByPosition(i).column.get(),
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
        exec_block = block.cloneWithColumns(std::move(columns));

        non_equal_conditions.other_cond_expr->execute(exec_block);

        const ColumnUInt8::Container *other_eq_from_in_column_data = nullptr, *other_column_data = nullptr;
        ConstNullMapPtr other_eq_from_in_null_map = nullptr, other_null_map = nullptr;
        ColumnPtr other_eq_from_in_column, other_column;

        bool has_other_eq_cond_from_in = !non_equal_conditions.other_eq_cond_from_in_name.empty();
        if (has_other_eq_cond_from_in)
        {
            other_eq_from_in_column = exec_block.getByName(non_equal_conditions.other_eq_cond_from_in_name).column;
            auto is_nullable_col = [&]() {
                if (other_eq_from_in_column->isColumnNullable())
                    return true;
                if (other_eq_from_in_column->isColumnConst())
                {
                    const auto & const_col = typeid_cast<const ColumnConst &>(*other_eq_from_in_column);
                    return const_col.getDataColumn().isColumnNullable();
                }
                return false;
            };
            // nullable, const(nullable)
            RUNTIME_CHECK_MSG(
                is_nullable_col(),
                "The equal condition from in column should be nullable, otherwise it should be used as join key");

            std::tie(other_eq_from_in_column_data, other_eq_from_in_null_map)
                = getDataAndNullMapVectorFromFilterColumn(other_eq_from_in_column);
        }

        bool has_other_cond = !non_equal_conditions.other_cond_name.empty();
        bool has_other_cond_null_map = false;
        if (has_other_cond)
        {
            other_column = exec_block.getByName(non_equal_conditions.other_cond_name).column;
            std::tie(other_column_data, other_null_map) = getDataAndNullMapVectorFromFilterColumn(other_column);
            has_other_cond_null_map = other_null_map != nullptr;
        }

#define CALL(has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map)            \
    checkAllExprResult<has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map>( \
        offsets,                                                                            \
        res_list,                                                                           \
        other_eq_from_in_column_data,                                                       \
        other_eq_from_in_null_map,                                                          \
        other_column_data,                                                                  \
        other_null_map);

        if (has_other_eq_cond_from_in)
        {
            if (has_other_cond)
            {
                if (has_other_cond_null_map)
                {
                    CALL(true, true, true);
                }
                else
                {
                    CALL(true, true, false);
                }
            }
            else
            {
                CALL(true, false, false);
            }
        }
        else
        {
            RUNTIME_CHECK(has_other_cond);
            if (has_other_cond_null_map)
            {
                CALL(false, true, true);
            }
            else
            {
                CALL(false, true, false);
            }
        }
#undef CALL
    }
}

template <ASTTableJoin::Kind KIND, typename Mapped>
template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
void SemiJoinHelper<KIND, Mapped>::checkAllExprResult(
    const std::vector<size_t> & offsets,
    std::list<Result *> & res_list,
    const ColumnUInt8::Container * other_eq_column,
    ConstNullMapPtr other_eq_null_map,
    const ColumnUInt8::Container * other_column,
    ConstNullMapPtr other_null_map)
{
    size_t prev_offset = 0;
    auto it = res_list.begin();
    for (size_t i = 0, size = offsets.size(); i < size && it != res_list.end(); ++i)
    {
        if ((*it)->template checkExprResult<has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map>(
                other_eq_column,
                other_eq_null_map,
                other_column,
                other_null_map,
                prev_offset,
                offsets[i]))
        {
            it = res_list.erase(it);
        }
        else
        {
            ++it;
        }

        prev_offset = offsets[i];
    }
}

#define M(KIND, STRICTNESS, MAPTYPE) template class SemiJoinResult<KIND, STRICTNESS>;
APPLY_FOR_SEMI_JOIN(M)
#undef M

template class SemiJoinHelper<Semi, MapsAll::MappedType>;
template class SemiJoinHelper<Anti, MapsAll::MappedType>;
template class SemiJoinHelper<LeftOuterSemi, MapsAll::MappedType>;
template class SemiJoinHelper<LeftOuterAnti, MapsAll::MappedType>;

} // namespace DB
