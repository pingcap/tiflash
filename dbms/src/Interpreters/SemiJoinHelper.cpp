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
#include <Core/Names.h>
#include <Interpreters/JoinPartition.h>
#include <Interpreters/ProbeProcessInfo.h>
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

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
SemiJoinHelper<KIND, STRICTNESS, Maps>::SemiJoinHelper(
    size_t input_rows_,
    size_t max_block_size_,
    const JoinNonEqualConditions & non_equal_conditions_)
    : input_rows(input_rows_)
    , max_block_size(max_block_size_)
    , non_equal_conditions(non_equal_conditions_)
{
    static_assert(KIND == Semi || KIND == Anti || KIND == LeftOuterAnti || KIND == LeftOuterSemi);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void SemiJoinHelper<KIND, STRICTNESS, Maps>::doJoin()
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
    {
        return;
    }
    else
    {
        assert(!undetermined_result_list.empty());
        std::vector<size_t> offsets;
        size_t block_columns = result_block.columns();
        Block exec_block = result_block.cloneEmpty();

        while (!undetermined_result_list.empty())
        {
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
                res->template fillRightColumns<typename Maps::MappedType>(
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
                            *result_block.getByPosition(i).column.get(),
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
            exec_block = result_block.cloneWithColumns(std::move(columns));

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
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void SemiJoinHelper<KIND, STRICTNESS, Maps>::probeHashTable(
    const JoinPartitions & join_partitions,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    const JoinBuildInfo & join_build_info,
    const ProbeProcessInfo & probe_process_info,
    const NameSet & probe_output_name_set,
    const Block & right_sample_block)
{
    if (is_probe_hash_table_done)
        return;
    std::tie(join_result, undetermined_result_list) = JoinPartition::probeBlockSemi<KIND, STRICTNESS, Maps>(
        join_partitions,
        input_rows,
        key_sizes,
        collators,
        join_build_info,
        probe_process_info);

    RUNTIME_ASSERT(
        join_result.size() == input_rows,
        "SemiJoinResult size {} must be equal to block size {}",
        join_result.size(),
        input_rows);
    for (size_t i = 0; i < probe_process_info.block.columns(); ++i)
    {
        const auto & column = probe_process_info.block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
            result_block.insert(column);
    }

    left_columns = result_block.columns();
    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        const auto & column = right_sample_block.getByPosition(i);
        if (probe_output_name_set.contains(column.name))
        {
            RUNTIME_CHECK_MSG(
                !result_block.has(column.name),
                "block from probe side has a column with the same name: {} as a column in right_sample_block",
                column.name);
            result_block.insert(column);
            right_column_indices_to_add.push_back(i);
        }
    }
    right_columns = right_column_indices_to_add.size();
    RUNTIME_CHECK(result_block.columns() == left_columns + right_columns);

    if constexpr (KIND == LeftOuterAnti || KIND == LeftOuterSemi)
    {
        /// The last column is `match_helper`.
        right_columns -= 1;
    }
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
    {
        RUNTIME_CHECK_MSG(undetermined_result_list.empty(), "SemiJoinResult list must be empty for any semi join");
    }
    else
    {
        RUNTIME_CHECK(right_columns > 0);
    }
    is_probe_hash_table_done = true;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
template <bool has_other_eq_cond_from_in, bool has_other_cond, bool has_other_cond_null_map>
void SemiJoinHelper<KIND, STRICTNESS, Maps>::checkAllExprResult(
    const std::vector<size_t> & offsets,
    const ColumnUInt8::Container * other_eq_column,
    ConstNullMapPtr other_eq_null_map,
    const ColumnUInt8::Container * other_column,
    ConstNullMapPtr other_null_map)
{
    size_t prev_offset = 0;
    auto it = undetermined_result_list.begin();
    for (size_t i = 0, size = offsets.size(); i < size && it != undetermined_result_list.end(); ++i)
    {
        if ((*it)->template checkExprResult<has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map>(
                other_eq_column,
                other_eq_null_map,
                other_column,
                other_null_map,
                prev_offset,
                offsets[i]))
        {
            it = undetermined_result_list.erase(it);
        }
        else
        {
            ++it;
        }

        prev_offset = offsets[i];
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
Block SemiJoinHelper<KIND, STRICTNESS, Maps>::genJoinResult(const NameSet & output_column_names_set)
{
    RUNTIME_CHECK_MSG(
        undetermined_result_list.empty(),
        "SemiJoinResult list must be empty when generating join result");
    std::unique_ptr<IColumn::Filter> filter;
    if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
        filter = std::make_unique<IColumn::Filter>(input_rows);

    MutableColumnPtr left_semi_column_ptr = nullptr;
    ColumnInt8::Container * left_semi_column_data = nullptr;
    ColumnUInt8::Container * left_semi_null_map = nullptr;

    if constexpr (KIND == ASTTableJoin::Kind::LeftOuterSemi || KIND == ASTTableJoin::Kind::LeftOuterAnti)
    {
        left_semi_column_ptr = result_block.getByPosition(result_block.columns() - 1).column->cloneEmpty();
        auto * left_semi_column = typeid_cast<ColumnNullable *>(left_semi_column_ptr.get());
        left_semi_column_data = &typeid_cast<ColumnVector<Int8> &>(left_semi_column->getNestedColumn()).getData();
        left_semi_column_data->reserve(input_rows);
        left_semi_null_map = &left_semi_column->getNullMapColumn().getData();
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
        {
            left_semi_null_map->resize_fill(input_rows, 0);
        }
        else
        {
            left_semi_null_map->reserve(input_rows);
        }
    }

    size_t rows_for_semi_anti = 0;
    for (size_t i = 0; i < input_rows; ++i)
    {
        auto result = join_result[i].getResult();
        if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
        {
            if (isTrueSemiJoinResult(result))
            {
                // If the result is true, this row should be kept.
                (*filter)[i] = 1;
                ++rows_for_semi_anti;
            }
            else
            {
                // If the result is null or false, this row should be filtered.
                (*filter)[i] = 0;
            }
        }
        else
        {
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                left_semi_column_data->push_back(result);
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
    }

    if constexpr (KIND == ASTTableJoin::Kind::LeftOuterSemi || KIND == ASTTableJoin::Kind::LeftOuterAnti)
    {
        result_block.getByPosition(result_block.columns() - 1).column = std::move(left_semi_column_ptr);
    }

    if constexpr (KIND == ASTTableJoin::Kind::Semi || KIND == ASTTableJoin::Kind::Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            auto & column = result_block.getByPosition(i);
            if (output_column_names_set.contains(column.name))
                column.column = column.column->filter(*filter, rows_for_semi_anti);
        }
    }
    return std::move(result_block);
}

#define M(KIND, STRICTNESS, MAPTYPE) template class SemiJoinResult<KIND, STRICTNESS>;
APPLY_FOR_SEMI_JOIN(M)
#undef M

#define M(KIND, STRICTNESS, MAPTYPE) template class SemiJoinHelper<KIND, STRICTNESS, MAPTYPE>;
APPLY_FOR_SEMI_JOIN(M)
#undef M
} // namespace DB
