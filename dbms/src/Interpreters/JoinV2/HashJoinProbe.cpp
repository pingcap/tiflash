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

namespace DB
{

using enum ASTTableJoin::Kind;

bool JoinProbeContext::isCurrentProbeFinished() const
{
    return start_row_idx >= rows && prefetch_active_states == 0;
}

void JoinProbeContext::resetBlock(Block & block_)
{
    block = block_;
    rows = block.rows();
    start_row_idx = 0;
    current_probe_row_ptr = nullptr;
    prefetch_active_states = 0;

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

    key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);
    /// Some useless columns maybe key columns so they must be removed after extracting key columns.
    for (size_t pos = 0; pos < block.columns();)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }

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

#define PREFETCH_READ(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
class JoinProbeBlockHelper
{
public:
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    JoinProbeBlockHelper(
        JoinProbeContext & context,
        JoinProbeWorkerData & wd,
        HashJoinKeyMethod method,
        ASTTableJoin::Kind kind,
        const JoinNonEqualConditions & non_equal_conditions,
        const HashJoinSettings & settings,
        const HashJoinPointerTable & pointer_table,
        const HashJoinRowLayout & row_layout,
        MutableColumns & added_columns)
        : context(context)
        , wd(wd)
        , method(method)
        , kind(kind)
        , non_equal_conditions(non_equal_conditions)
        , settings(settings)
        , pointer_table(pointer_table)
        , row_layout(row_layout)
        , added_columns(added_columns)
    {
        wd.insert_batch.clear();
        wd.insert_batch.reserve(settings.probe_insert_batch_size);
        if constexpr (!key_all_raw)
        {
            wd.insert_batch_other.clear();
            wd.insert_batch_other.reserve(settings.probe_insert_batch_size);
        }

        if (pointer_table.enableProbePrefetch())
        {
            wd.selective_offsets.clear();
            wd.selective_offsets.reserve(context.rows);
            wd.prefetch_states.resize(settings.probe_prefetch_step);
        }
        else
            wd.offsets_to_replicate.resize(context.rows);
    }

    void joinProbeBlockImpl();

    void NO_INLINE joinProbeBlockInner();
    void NO_INLINE joinProbeBlockInnerPrefetch();

    void NO_INLINE joinProbeBlockLeftOuter();
    void NO_INLINE joinProbeBlockLeftOuterPrefetch();

    void NO_INLINE joinProbeBlockSemi();
    void NO_INLINE joinProbeBlockSemiPrefetch();

    void NO_INLINE joinProbeBlockAnti();
    void NO_INLINE joinProbeBlockAntiPrefetch();

    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightOuter();
    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightOuterPrefetch();

    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightSemi();
    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightSemiPrefetch();

    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightAnti();
    template <bool has_other_condition>
    void NO_INLINE joinProbeBlockRightAntiPrefetch();

private:
    void ALWAYS_INLINE insertRowToBatch(RowPtr row_ptr, size_t key_size) const
    {
        wd.insert_batch.push_back(row_ptr);
        if constexpr (!key_all_raw)
        {
            wd.insert_batch_other.push_back(row_ptr + key_size);
        }
        FlushBatchIfNecessary<false>();
    }

    template <bool force>
    void ALWAYS_INLINE FlushBatchIfNecessary() const
    {
        if constexpr (!force)
        {
            if likely (wd.insert_batch.size() < settings.probe_insert_batch_size)
                return;
        }
        for (auto [column_index, is_nullable] : row_layout.raw_required_key_column_indexes)
        {
            IColumn * column = added_columns[column_index].get();
            if (has_null_map && is_nullable)
                column = &static_cast<ColumnNullable &>(*added_columns[column_index]).getNestedColumn();
            column->deserializeAndInsertFromPos(wd.insert_batch);
        }
        for (auto [column_index, _] : row_layout.other_required_column_indexes)
        {
            if constexpr (key_all_raw)
                added_columns[column_index]->deserializeAndInsertFromPos(wd.insert_batch);
            else
                added_columns[column_index]->deserializeAndInsertFromPos(wd.insert_batch_other);
        }

        wd.insert_batch.clear();
        if constexpr (!key_all_raw)
            wd.insert_batch_other.clear();
    }

    void ALWAYS_INLINE FillNullMap(size_t size) const
    {
        if constexpr (has_null_map)
        {
            for (auto [column_index, is_nullable] : row_layout.raw_required_key_column_indexes)
            {
                if (is_nullable)
                {
                    auto & null_map_vec
                        = static_cast<ColumnNullable &>(*added_columns[column_index]).getNullMapColumn().getData();
                    null_map_vec.resize_fill(null_map_vec.size() + size, 0);
                }
            }
        }
    }

private:
    JoinProbeContext & context;
    JoinProbeWorkerData & wd;
    const HashJoinKeyMethod method;
    const ASTTableJoin::Kind kind;
    const JoinNonEqualConditions & non_equal_conditions;
    const HashJoinSettings & settings;
    const HashJoinPointerTable & pointer_table;
    const HashJoinRowLayout & row_layout;
    MutableColumns & added_columns;
};

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockInner()
{
    auto & key_getter = *static_cast<typename KeyGetter::Type *>(context.key_getter.get());
    size_t current_offset = 0;
    auto & offsets_to_replicate = wd.offsets_to_replicate;
    size_t & idx = context.start_row_idx;
    RowPtr & ptr = context.current_probe_row_ptr;
    for (; idx < context.rows; ++idx)
    {
        if (has_null_map && (*context.null_map)[idx])
        {
            offsets_to_replicate[idx] = current_offset;
            continue;
        }
        const auto & key = key_getter.getJoinKey(idx);
        if likely (ptr == nullptr)
        {
            size_t hash = static_cast<HashValueType>(Hash()(key));
            ptr = pointer_table.getHeadPointer(hash);
        }
        while (ptr)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + row_layout.key_offset);
            ///TODO: string compare needs hash value.
            if (key_getter.joinKeyIsEqual(key, key2))
            {
                ++current_offset;
                insertRowToBatch(ptr + row_layout.key_offset, key_getter.getJoinKeySize(key2));
                if unlikely (current_offset >= settings.max_block_size)
                    break;
            }
            ptr = row_layout.getNextRowPtr<Inner>(ptr);
        }
        offsets_to_replicate[idx] = current_offset;
        if unlikely (ptr != nullptr)
        {
            ptr = row_layout.getNextRowPtr<Inner>(ptr);
            if (ptr == nullptr)
                ++idx;
            break;
        }
    }
    FlushBatchIfNecessary<true>();
    FillNullMap(current_offset);
}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockInnerPrefetch()
{
    auto & key_getter = *static_cast<typename KeyGetter::Type *>(context.key_getter.get());
    auto & selective_offsets = wd.selective_offsets;

    size_t & idx = context.start_row_idx;
    size_t & active_states = context.prefetch_active_states;
    size_t & k = wd.prefetch_iter;
    size_t current_offset = 0;
    while (idx < context.rows || active_states > 0)
    {
        k = k == settings.probe_prefetch_step ? 0 : k;
        auto * state = &wd.prefetch_states[k];
        if (state->stage == ProbePrefetchStage::FindNext)
        {
            RowPtr ptr = state->ptr;
            RowPtr next_ptr = row_layout.getNextRowPtr<Inner>(ptr);
            if (next_ptr)
            {
                state->ptr = next_ptr;
                PREFETCH_READ(next_ptr + row_layout.next_pointer_offset);
            }

            const auto & key = key_getter.getJoinKeyWithBufferHint(state->index);
            const auto & key2 = key_getter.deserializeJoinKey(ptr + row_layout.key_offset);
            if (key_getter.joinKeyIsEqual(key, key2))
            {
                ++current_offset;
                selective_offsets.push_back(state->index);
                insertRowToBatch(ptr + row_layout.key_offset, key_getter.getJoinKeySize(key2));
                if unlikely (current_offset >= settings.max_block_size)
                    break;
            }
            if (next_ptr)
            {
                ++k;
                continue;
            }

            state->stage = ProbePrefetchStage::None;
            --active_states;
        }
        else if (state->stage == ProbePrefetchStage::FindHeader)
        {
            RowPtr ptr = state->pointer_ptr->load(std::memory_order_relaxed);
            if (ptr)
            {
                PREFETCH_READ(ptr + row_layout.next_pointer_offset);
                state->ptr = ptr;
                state->stage = ProbePrefetchStage::FindNext;
                ++k;
                continue;
            }

            state->stage = ProbePrefetchStage::None;
            --active_states;
        }

        assert(state->stage == ProbePrefetchStage::None);

        if constexpr (has_null_map)
        {
            while (idx < context.rows)
            {
                if (!(*context.null_map)[idx])
                    break;
                ++idx;
            }
        }

        if unlikely (idx >= context.rows)
        {
            ++k;
            continue;
        }

        const auto & key = key_getter.getJoinKeyWithBufferHint(idx);
        size_t hash = static_cast<HashValueType>(Hash()(key));
        size_t bucket = pointer_table.getBucketNum(hash);
        state->pointer_ptr = pointer_table.getPointerTable() + bucket;
        PREFETCH_READ(state->pointer_ptr);

        state->index = idx;
        state->stage = ProbePrefetchStage::FindHeader;
        ++active_states;
        ++idx;
        ++k;
    }

    FlushBatchIfNecessary<true>();
    FillNullMap(current_offset);
}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockLeftOuter()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockLeftOuterPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockSemi()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockSemiPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockAnti()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockAntiPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightOuter()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightOuterPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightSemi()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightSemiPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightAnti()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
template <bool has_other_condition>
void NO_INLINE JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockRightAntiPrefetch()
{}

template <typename KeyGetter, bool has_null_map, bool key_all_raw>
void JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_raw>::joinProbeBlockImpl()
{
#define CALL(JoinType)                        \
    if (pointer_table.enableProbePrefetch())  \
        joinProbeBlock##JoinType##Prefetch(); \
    else                                      \
        joinProbeBlock##JoinType();

#define CALL2(JoinType, has_other_condition)                       \
    if (pointer_table.enableProbePrefetch())                       \
        joinProbeBlock##JoinType##Prefetch<has_other_condition>(); \
    else                                                           \
        joinProbeBlock##JoinType<has_other_condition>();

    bool has_other_condition = non_equal_conditions.other_cond_expr != nullptr;
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

void joinProbeBlock(
    JoinProbeContext & context,
    JoinProbeWorkerData & wd,
    HashJoinKeyMethod method,
    ASTTableJoin::Kind kind,
    const JoinNonEqualConditions & non_equal_conditions,
    const HashJoinSettings & settings,
    const HashJoinPointerTable & pointer_table,
    const HashJoinRowLayout & row_layout,
    MutableColumns & added_columns)
{
    if (context.rows == 0)
        return;

    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        break;

#define CALL(KeyGetter, has_null_map, key_all_row)              \
    JoinProbeBlockHelper<KeyGetter, has_null_map, key_all_row>( \
        context,                                                \
        wd,                                                     \
        method,                                                 \
        kind,                                                   \
        non_equal_conditions,                                   \
        settings,                                               \
        pointer_table,                                          \
        row_layout,                                             \
        added_columns)                                          \
        .joinProbeBlockImpl();

#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        if (context.null_map)                                                              \
        {                                                                                  \
            if (row_layout.key_all_raw_required)                                           \
            {                                                                              \
                CALL(KeyGetterType##METHOD, true, true);                                   \
            }                                                                              \
            else                                                                           \
            {                                                                              \
                CALL(KeyGetterType##METHOD, true, false);                                  \
            }                                                                              \
        }                                                                                  \
        else                                                                               \
        {                                                                                  \
            if (row_layout.key_all_raw_required)                                           \
            {                                                                              \
                CALL(KeyGetterType##METHOD, false, true);                                  \
            }                                                                              \
            else                                                                           \
            {                                                                              \
                CALL(KeyGetterType##METHOD, false, false);                                 \
            }                                                                              \
        }                                                                                  \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

#undef CALL

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

} // namespace DB
