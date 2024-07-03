// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnUtils.h>
#include <Common/Stopwatch.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Interpreters/NullableUtils.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-local-typedef"
namespace DB
{

using enum ASTTableJoin::Kind;

bool JoinProbeContext::isCurrentProbeFinished() const
{
    return start_row_idx >= block.rows();
}

void JoinProbeContext::resetBlock(Block && block_)
{
    block = std::move(block_);
    start_row_idx = 0;
    current_row_probe_head = nullptr;

    is_prepared = false;
    materialized_columns.clear();
    key_columns.clear();
    null_map = nullptr;
    null_map_holder = nullptr;
    current_row_is_matched = false;
}

void JoinProbeContext::prepareForHashProbe(
    HashJoinKeyMethod method,
    ASTTableJoin::Kind kind,
    const Names & key_names,
    const String & filter_column,
    const NameSet & probe_output_name_set,
    const TiDB::TiDBCollators & collators)
{
    if (is_prepared)
        return;

    for (size_t pos = 0; pos < block.columns();)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }

    key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);
    /// Keys with NULL value in any column won't join to anything.
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter won't join to anything
    recordFilteredRows(block, filter_column, null_map_holder, null_map);

    if unlikely (!key_getter)
        key_getter = createHashJoinKeyGetter(method, collators);

    resetHashJoinKeyGetter(method, key_columns, key_getter);

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
     * Because if they are constants, then in the "not joined" rows, they may have different values
     *  - default values, which can differ from the values of these constants.
     */
    if (getFullness(kind))
    {
        size_t existing_columns = block.columns();
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto & col = block.getByPosition(i).column;

            if (ColumnPtr converted = col->convertToFullColumnIfConst())
                col = converted;

            /// convert left columns (except keys) to Nullable
            if (std::end(key_names) == std::find(key_names.begin(), key_names.end(), block.getByPosition(i).name))
                convertColumnToNullable(block.getByPosition(i));
        }
    }

    is_prepared = true;
}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockInner(JoinProbeParameter & param, MutableColumns & added_columns)
{
    using Type = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    Type & key_getter = *static_cast<Type *>(param.context.key_getter.get());

    size_t rows = param.context.block.rows();
    size_t limit_count = rows - param.context.start_row_idx;
    if (limit_count < 1024)
        limit_count = 1024;

    auto & insert_batch = param.wd.insert_batch;
    insert_batch.clear();
    insert_batch.reserve(param.settings.probe_insert_batch_size);
    auto & insert_batch_other = param.wd.insert_batch_other;
    if constexpr (!key_all_raw)
    {
        insert_batch_other.clear();
        insert_batch_other.reserve(param.settings.probe_insert_batch_size);
    }

    size_t current_offset = 0;
    auto & offsets_to_replicate = param.wd.offsets_to_replicate;
    size_t & idx = param.context.start_row_idx;
    RowPtr & head = param.context.current_row_probe_head;
    const auto & pointer_table = param.pointer_table;
    const auto & row_layout = param.row_layout;
    const auto & settings = param.settings;
    for (; idx < rows; ++idx)
    {
        if (has_null_map && (*param.context.null_map)[idx])
        {
            offsets_to_replicate[idx] = current_offset;
            continue;
        }
        const auto & key = key_getter.getJoinKey(idx);
        if likely (head == nullptr)
        {
            size_t hash = static_cast<HashValueType>(Hash()(key));
            head = pointer_table.getHeadPointer(hash);
        }
        while (head)
        {
            const auto & key2 = key_getter.deserializeJoinKey(head + row_layout.join_key_offset);
            if (key_getter.joinKeyIsEqual(key, key2))
            {
                insert_batch.push_back(head + row_layout.join_key_offset);
                if constexpr (!key_all_raw)
                {
                    size_t key_size = key_getter.getJoinKeySize(key2);
                    insert_batch_other.push_back(head + row_layout.join_key_offset + key_size);
                }
                ++current_offset;
                if unlikely (insert_batch.size() >= settings.probe_insert_batch_size)
                {
                    for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
                    {
                        IColumn * column = added_columns[column_index].get();
                        if (has_null_map && is_nullable)
                            column = &static_cast<ColumnNullable &>(*added_columns[column_index]).getNestedColumn();
                        column->deserializeAndInsertFromPos(insert_batch);
                    }
                    for (auto [column_index, _] : row_layout.other_column_indexes)
                    {
                        if constexpr (key_all_raw)
                            added_columns[column_index]->deserializeAndInsertFromPos(insert_batch);
                        else
                            added_columns[column_index]->deserializeAndInsertFromPos(insert_batch_other);
                    }
                }
                if unlikely (current_offset >= param.settings.max_block_size)
                    break;
            }
            head = row_layout.getNextRowPtr<Inner>(head);
        }
        offsets_to_replicate[idx] = current_offset;
        if unlikely (head != nullptr)
        {
            head = row_layout.getNextRowPtr<Inner>(head);
            if (head == nullptr)
                ++idx;
            break;
        }
    }
    if (!insert_batch.empty())
    {
        for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
        {
            IColumn * column = added_columns[column_index].get();
            if (has_null_map && is_nullable)
                column = &static_cast<ColumnNullable &>(*added_columns[column_index]).getNestedColumn();
            column->deserializeAndInsertFromPos(insert_batch);
        }
        for (auto [column_index, _] : row_layout.other_column_indexes)
        {
            if constexpr (key_all_raw)
                added_columns[column_index]->deserializeAndInsertFromPos(insert_batch);
            else
                added_columns[column_index]->deserializeAndInsertFromPos(insert_batch_other);
        }
    }
    if constexpr (has_null_map)
    {
        for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
        {
            if (is_nullable)
            {
                auto & null_map_vec
                    = static_cast<ColumnNullable &>(*added_columns[column_index]).getNullMapColumn().getData();
                null_map_vec.resize_fill(null_map_vec.size() + current_offset, 0);
            }
        }
    }
}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockInnerPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockLeftOuter(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockLeftOuterPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockSemi(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockSemiPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockAnti(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE joinProbeBlockAntiPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightOuter(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightOuterPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightSemi(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightSemiPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightAnti(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw, bool has_other_condition>
void NO_INLINE joinProbeBlockRightAntiPrefetch(JoinProbeParameter & param, MutableColumns & added_columns)
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void joinProbeBlockImpl(JoinProbeParameter & param, MutableColumns & added_columns)
{
#define CALL(JoinType)                                                                                  \
    if (param.pointer_table.enableProbePrefetch())                                                      \
        joinProbeBlock##JoinType##Prefetch<KeyGetter, has_null_map, key_all_raw>(param, added_columns); \
    else                                                                                                \
        joinProbeBlock##JoinType<KeyGetter, has_null_map, key_all_raw>(param, added_columns);

#define CALL2(JoinType, has_other_condition)                                                           \
    if (param.pointer_table.enableProbePrefetch())                                                     \
        joinProbeBlock##JoinType##Prefetch<KeyGetter, has_null_map, key_all_raw, has_other_condition>( \
            param,                                                                                     \
            added_columns);                                                                            \
    else                                                                                               \
        joinProbeBlock##JoinType<KeyGetter, has_null_map, key_all_raw, has_other_condition>(param, added_columns);

    auto kind = param.kind;
    bool has_other_condition = param.non_equal_conditions.other_cond_expr != nullptr;
    if (kind == Inner)
        CALL(Inner)
    else if (kind == LeftOuter)
        CALL(LeftOuter)
    else if (kind == Semi && !has_other_condition)
        CALL(Semi)
    else if (kind == Anti && !has_other_condition)
        CALL(Anti)
    else if (kind == RightOuter && has_other_condition)
        CALL2(RightOuter, true)
    else if (kind == RightOuter)
        CALL2(RightOuter, false)
    else if (kind == RightSemi && has_other_condition)
        CALL2(RightSemi, true)
    else if (kind == RightSemi)
        CALL2(RightSemi, false)
    else if (kind == RightAnti && has_other_condition)
        CALL2(RightAnti, true)
    else if (kind == RightAnti)
        CALL2(RightAnti, false)
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

#undef CALL2
#undef CALL
}

void joinProbeBlock(JoinProbeParameter & param, MutableColumns & added_columns)
{
    if (param.context.block.rows() == 0)
        return;

    switch (param.method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        break;

#define M(METHOD)                                                                             \
    case HashJoinKeyMethod::METHOD:                                                           \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>;    \
        if (param.context.null_map)                                                           \
            if (param.row_layout.join_key_all_raw)                                            \
                joinProbeBlockImpl<KeyGetterType##METHOD, true, true>(param, added_columns);  \
            else                                                                              \
                joinProbeBlockImpl<KeyGetterType##METHOD, true, false>(param, added_columns); \
        else if (param.row_layout.join_key_all_raw)                                           \
            joinProbeBlockImpl<KeyGetterType##METHOD, false, true>(param, added_columns);     \
        else                                                                                  \
            joinProbeBlockImpl<KeyGetterType##METHOD, false, false>(param, added_columns);    \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

#pragma GCC diagnostic pop

} // namespace DB