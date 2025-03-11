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
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/HashJoinProbe.h>
#include <Interpreters/NullableUtils.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

using enum ASTTableJoin::Kind;

bool JoinProbeContext::isCurrentProbeFinished() const
{
    return start_row_idx >= rows && prefetch_active_states == 0 && rows_is_matched.empty();
}

void JoinProbeContext::resetBlock(Block & block_)
{
    block = block_;
    orignal_block = block_;
    rows = block.rows();
    start_row_idx = 0;
    current_row_ptr = nullptr;
    current_row_is_matched = false;
    rows_is_matched.clear();

    prefetch_active_states = 0;

    is_prepared = false;
    materialized_columns.clear();
    key_columns.clear();
    null_map = nullptr;
    null_map_holder = nullptr;
}

void JoinProbeContext::prepareForHashProbe(
    HashJoinKeyMethod method,
    ASTTableJoin::Kind kind,
    bool has_other_condition,
    const Names & key_names,
    const String & filter_column,
    const NameSet & probe_output_name_set,
    const Block & sample_block_pruned,
    const TiDB::TiDBCollators & collators,
    const HashJoinRowLayout & row_layout)
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
    resetHashJoinKeyGetter(method, key_getter, key_columns, row_layout);

    block = materializeBlock(block);

    /// In case of RIGHT and FULL joins, convert left columns to Nullable.
    if (getFullness(kind))
    {
        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            convertColumnToNullable(block.getByPosition(i));
    }

    assertBlocksHaveEqualStructure(block, sample_block_pruned, "Join Probe");

    if ((kind == LeftOuter || kind == Semi || kind == Anti) && has_other_condition)
    {
        rows_is_matched.clear();
        rows_is_matched.resize_fill_zero(block.rows());
    }

    is_prepared = true;
}

#define PREFETCH_READ(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)

JoinProbeBlockHelper::JoinProbeBlockHelper(const HashJoin * join, bool late_materialization)
    : join(join)
    , late_materialization(late_materialization)
    , kind(join->kind)
    , settings(join->settings)
    , pointer_table(join->pointer_table)
    , row_layout(join->row_layout)
{
#define CALL3(KeyGetter, JoinType, tagged_pointer, has_other_condition, late_materialization) \
    {                                                                                         \
        if (pointer_table.enableProbePrefetch())                                              \
        {                                                                                     \
            func_ptr_has_null = &JoinProbeBlockHelper::joinProbeBlock##JoinType##Prefetch<    \
                KeyGetter,                                                                    \
                true,                                                                         \
                tagged_pointer,                                                               \
                has_other_condition,                                                          \
                late_materialization>;                                                        \
            func_ptr_no_null = &JoinProbeBlockHelper::joinProbeBlock##JoinType##Prefetch<     \
                KeyGetter,                                                                    \
                false,                                                                        \
                tagged_pointer,                                                               \
                has_other_condition,                                                          \
                late_materialization>;                                                        \
        }                                                                                     \
        else                                                                                  \
        {                                                                                     \
            func_ptr_has_null = &JoinProbeBlockHelper::joinProbeBlock##JoinType<              \
                KeyGetter,                                                                    \
                true,                                                                         \
                tagged_pointer,                                                               \
                has_other_condition,                                                          \
                late_materialization>;                                                        \
            func_ptr_no_null = &JoinProbeBlockHelper::joinProbeBlock##JoinType<               \
                KeyGetter,                                                                    \
                false,                                                                        \
                tagged_pointer,                                                               \
                has_other_condition,                                                          \
                late_materialization>;                                                        \
        }                                                                                     \
    }

#define CALL2(KeyGetter, JoinType, tagged_pointer)                      \
    {                                                                   \
        if (join->has_other_condition)                                  \
        {                                                               \
            if (late_materialization)                                   \
                CALL3(KeyGetter, JoinType, tagged_pointer, true, true)  \
            else                                                        \
                CALL3(KeyGetter, JoinType, tagged_pointer, true, false) \
        }                                                               \
        else                                                            \
            CALL3(KeyGetter, JoinType, tagged_pointer, false, false)    \
    }

#define CALL1(KeyGetter, JoinType)               \
    {                                            \
        if (pointer_table.enableTaggedPointer()) \
            CALL2(KeyGetter, JoinType, true)     \
        else                                     \
            CALL2(KeyGetter, JoinType, false)    \
    }

#define CALL(KeyGetter)                                                                               \
    {                                                                                                 \
        if (kind == Inner)                                                                            \
            CALL1(KeyGetter, Inner)                                                                   \
        else if (kind == LeftOuter)                                                                   \
            CALL1(KeyGetter, LeftOuter)                                                               \
        /*else if (kind == Semi) \
            CALL(Semi) \
        else if (kind == Anti) \
            CALL(Anti) \
        else if (kind == RightOuter) \
            CALL(RightOuter) \
        else if (kind == RightSemi) \
            CALL(RightSemi) \
        else if (kind == RightAnti) \
            CALL(RightAnti) */                                                                    \
        else                                                                                          \
            throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR); \
    }

    switch (join->method)
    {
#define M(METHOD)                                                                          \
    case HashJoinKeyMethod::METHOD:                                                        \
        using KeyGetterType##METHOD = HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>; \
        CALL(KeyGetterType##METHOD);                                                       \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception(
            fmt::format("Unknown JOIN keys variant {}.", magic_enum::enum_name(join->method)),
            ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
#undef CALL
#undef CALL1
#undef CALL2
#undef CALL3
}

Block JoinProbeBlockHelper::joinProbeBlock(JoinProbeContext & context, JoinProbeWorkerData & wd)
{
    if (join->has_other_condition)
    {
        if (late_materialization)
            return joinProbeBlockImpl<true, true>(context, wd);
        else
            return joinProbeBlockImpl<true, false>(context, wd);
    }
    else
    {
        return joinProbeBlockImpl<false, false>(context, wd);
    }
}

template <bool has_other_condition, bool late_materialization>
Block JoinProbeBlockHelper::joinProbeBlockImpl(JoinProbeContext & context, JoinProbeWorkerData & wd)
{
    static_assert(has_other_condition || !late_materialization);

    if unlikely (context.rows == 0)
        return join->output_block_after_finalize;

    wd.insert_batch.clear();
    wd.insert_batch.reserve(settings.probe_insert_batch_size);
    wd.selective_offsets.clear();
    wd.selective_offsets.reserve(settings.max_block_size);
    if constexpr (late_materialization)
    {
        wd.row_ptrs_for_lm.clear();
        wd.row_ptrs_for_lm.reserve(settings.max_block_size);
    }

    size_t left_columns = join->left_sample_block_pruned.columns();
    size_t right_columns = join->right_sample_block_pruned.columns();
    if (!wd.result_block)
    {
        RUNTIME_CHECK(left_columns + right_columns == join->all_sample_block_pruned.columns());
        for (size_t i = 0; i < left_columns + right_columns; ++i)
        {
            ColumnWithTypeAndName new_column = join->all_sample_block_pruned.safeGetByPosition(i).cloneEmpty();
            new_column.column->assumeMutable()->reserveAlign(settings.max_block_size, FULL_VECTOR_SIZE_AVX2);
            wd.result_block.insert(std::move(new_column));
        }
    }

    MutableColumns added_columns;
    if constexpr (late_materialization)
    {
        for (auto [column_index, _] : row_layout.raw_required_key_column_indexes)
            added_columns.emplace_back(
                wd.result_block.safeGetByPosition(left_columns + column_index).column->assumeMutable());
        for (size_t i = 0; i < row_layout.other_required_count_for_other_condition; ++i)
        {
            size_t column_index = row_layout.other_required_column_indexes[i].first;
            added_columns.emplace_back(
                wd.result_block.safeGetByPosition(left_columns + column_index).column->assumeMutable());
        }
    }
    else
    {
        added_columns.resize(right_columns);
        for (size_t i = 0; i < right_columns; ++i)
            added_columns[i] = wd.result_block.safeGetByPosition(left_columns + i).column->assumeMutable();
    }

    Stopwatch watch;
    if (context.null_map)
        (this->*func_ptr_has_null)(context, wd, added_columns);
    else
        (this->*func_ptr_no_null)(context, wd, added_columns);

    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    if constexpr (late_materialization)
    {
        size_t idx = 0;
        for (auto [column_index, _] : row_layout.raw_required_key_column_indexes)
            wd.result_block.safeGetByPosition(left_columns + column_index).column = std::move(added_columns[idx++]);
        for (size_t i = 0; i < row_layout.other_required_count_for_other_condition; ++i)
        {
            size_t column_index = row_layout.other_required_column_indexes[i].first;
            wd.result_block.safeGetByPosition(left_columns + column_index).column = std::move(added_columns[idx++]);
        }
    }
    else
    {
        for (size_t i = 0; i < right_columns; ++i)
            wd.result_block.safeGetByPosition(left_columns + i).column = std::move(added_columns[i]);
    }

    if (wd.selective_offsets.empty())
        return join->output_block_after_finalize;

    if constexpr (has_other_condition)
    {
        // Always using late materialization for left side
        for (size_t i = 0; i < left_columns; ++i)
        {
            if (!join->left_required_flag_for_other_condition[i])
                continue;
            wd.result_block.safeGetByPosition(i).column->assumeMutable()->insertSelectiveFrom(
                *context.block.safeGetByPosition(i).column.get(),
                wd.selective_offsets);
        }
    }
    else
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            wd.result_block.safeGetByPosition(i).column->assumeMutable()->insertSelectiveFrom(
                *context.block.safeGetByPosition(i).column.get(),
                wd.selective_offsets);
        }
    }

    wd.replicate_time += watch.elapsedFromLastTime();

    if constexpr (has_other_condition)
    {
        auto res_block = handleOtherConditions<late_materialization>(context, wd);
        wd.other_condition_time += watch.elapsedFromLastTime();
        return res_block;
    }

    if (wd.result_block.rows() >= settings.max_block_size)
    {
        auto res_block = join->removeUselessColumnForOutput(wd.result_block);
        wd.result_block = {};
        return res_block;
    }
    return join->output_block_after_finalize;
}

JOIN_PROBE_TEMPLATE
void NO_INLINE JoinProbeBlockHelper::joinProbeBlockInner(
    JoinProbeContext & context,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    auto & key_getter = *static_cast<KeyGetterType *>(context.key_getter.get());
    size_t prev_offset = wd.result_block.rows();
    size_t current_offset = wd.result_block.rows();
    auto & selective_offsets = wd.selective_offsets;
    size_t idx = context.start_row_idx;
    RowPtr ptr = context.current_row_ptr;
    size_t collision = 0;
    size_t key_offset = sizeof(RowPtr);
    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
    {
        key_offset += sizeof(HashValueType);
    }
    for (; idx < context.rows; ++idx)
    {
        if (has_null_map && (*context.null_map)[idx])
            continue;

        const auto & key = key_getter.getJoinKey(idx);
        auto hash = static_cast<HashValueType>(Hash()(key));
        UInt16 hash_tag = hash & ROW_PTR_TAG_MASK;
        if likely (ptr == nullptr)
        {
            ptr = pointer_table.getHeadPointer(hash);
            if (ptr == nullptr)
                continue;

            if constexpr (tagged_pointer)
            {
                if (!containOtherTag(ptr, hash_tag))
                {
                    ptr = nullptr;
                    continue;
                }
                ptr = removeRowPtrTag(ptr);
            }
        }
        while (true)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, key, key2, hash, ptr);
            collision += !key_is_equal;
            if (key_is_equal)
            {
                ++current_offset;
                selective_offsets.push_back(idx);
                insertRowToBatch<has_null_map, late_materialization>(
                    wd,
                    added_columns,
                    key_getter,
                    ptr + key_offset,
                    key2);
                if unlikely (current_offset >= settings.max_block_size)
                    break;
            }

            ptr = HashJoinRowLayout::getNextRowPtr(ptr);
            if (ptr == nullptr)
                break;
        }
        if unlikely (ptr != nullptr)
        {
            ptr = HashJoinRowLayout::getNextRowPtr(ptr);
            if (ptr == nullptr)
                ++idx;
            break;
        }
    }
    flushBatchIfNecessary<has_null_map, late_materialization, true>(wd, added_columns);
    fillNullMapWithZero<has_null_map>(added_columns, current_offset - prev_offset);

    context.start_row_idx = idx;
    context.current_row_ptr = ptr;
    wd.collision += collision;
}

JOIN_PROBE_TEMPLATE
void NO_INLINE JoinProbeBlockHelper::joinProbeBlockInnerPrefetch(
    JoinProbeContext & context,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    auto & key_getter = *static_cast<KeyGetterType *>(context.key_getter.get());
    initPrefetchStates<KeyGetter>(context);
    auto * states = static_cast<ProbePrefetchState<KeyGetter> *>(context.prefetch_states.get());
    auto & selective_offsets = wd.selective_offsets;

    size_t idx = context.start_row_idx;
    size_t active_states = context.prefetch_active_states;
    size_t k = context.prefetch_iter;
    size_t prev_offset = wd.result_block.rows();
    size_t current_offset = wd.result_block.rows();
    size_t collision = 0;
    size_t key_offset = sizeof(RowPtr);
    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
    {
        key_offset += sizeof(HashValueType);
    }
    const size_t probe_prefetch_step = settings.probe_prefetch_step;
    while (idx < context.rows || active_states > 0)
    {
        k = k == probe_prefetch_step ? 0 : k;
        auto * state = &states[k];
        if (state->stage == ProbePrefetchStage::FindNext)
        {
            RowPtr ptr = state->ptr;
            RowPtr next_ptr = HashJoinRowLayout::getNextRowPtr(ptr);
            if (next_ptr)
            {
                state->ptr = next_ptr;
                PREFETCH_READ(next_ptr);
            }

            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, state->key, key2, state->hash, ptr);
            collision += !key_is_equal;
            if (key_is_equal)
            {
                ++current_offset;
                selective_offsets.push_back(state->index);
                insertRowToBatch<has_null_map, late_materialization>(
                    wd,
                    added_columns,
                    key_getter,
                    ptr + key_offset,
                    key2);
                if unlikely (current_offset >= settings.max_block_size)
                {
                    if (!next_ptr)
                    {
                        state->stage = ProbePrefetchStage::None;
                        --active_states;
                    }
                    break;
                }
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
                bool forward = true;
                if constexpr (tagged_pointer)
                {
                    if (containOtherTag(ptr, state->hash_tag))
                        ptr = removeRowPtrTag(ptr);
                    else
                        forward = false;
                }
                if (forward)
                {
                    PREFETCH_READ(ptr);
                    state->ptr = ptr;
                    state->stage = ProbePrefetchStage::FindNext;
                    ++k;
                    continue;
                }
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

        const auto & key = key_getter.getJoinKeyWithBuffer(idx);
        auto hash = static_cast<HashValueType>(Hash()(key));
        size_t bucket = pointer_table.getBucketNum(hash);
        state->pointer_ptr = pointer_table.getPointerTable() + bucket;
        PREFETCH_READ(state->pointer_ptr);

        state->key = key;
        if constexpr (tagged_pointer)
            state->hash_tag = hash & ROW_PTR_TAG_MASK;
        if constexpr (KeyGetterType::joinKeyCompareHashFirst())
            state->hash = hash;
        state->index = idx;
        state->stage = ProbePrefetchStage::FindHeader;
        ++active_states;
        ++idx;
        ++k;
    }

    flushBatchIfNecessary<has_null_map, late_materialization, true>(wd, added_columns);
    fillNullMapWithZero<has_null_map>(added_columns, current_offset - prev_offset);

    context.start_row_idx = idx;
    context.prefetch_active_states = active_states;
    context.prefetch_iter = k;
    wd.collision += collision;
}

JOIN_PROBE_TEMPLATE
void NO_INLINE
JoinProbeBlockHelper::joinProbeBlockLeftOuter(JoinProbeContext &, JoinProbeWorkerData &, MutableColumns &)
{}

JOIN_PROBE_TEMPLATE
void NO_INLINE
JoinProbeBlockHelper::joinProbeBlockLeftOuterPrefetch(JoinProbeContext &, JoinProbeWorkerData &, MutableColumns &)
{}

template <bool late_materialization>
Block JoinProbeBlockHelper::handleOtherConditions(JoinProbeContext & context, JoinProbeWorkerData & wd)
{
    const auto & left_sample_block_pruned = join->left_sample_block_pruned;
    const auto & right_sample_block_pruned = join->right_sample_block_pruned;
    const auto & output_block_after_finalize = join->output_block_after_finalize;
    const auto & non_equal_conditions = join->non_equal_conditions;
    const auto & left_required_flag_for_other_condition = join->left_required_flag_for_other_condition;

    size_t left_columns = left_sample_block_pruned.columns();
    size_t right_columns = right_sample_block_pruned.columns();
    // Some columns in wd.result_block may be empty so need to create another block to execute other condition expressions
    Block exec_block;
    for (size_t i = 0; i < left_columns; ++i)
    {
        if (left_required_flag_for_other_condition[i])
            exec_block.insert(wd.result_block.getByPosition(i));
    }
    if constexpr (late_materialization)
    {
        for (auto [column_index, _] : row_layout.raw_required_key_column_indexes)
            exec_block.insert(wd.result_block.getByPosition(left_columns + column_index));
        for (size_t i = 0; i < row_layout.other_required_count_for_other_condition; ++i)
        {
            size_t column_index = row_layout.other_required_column_indexes[i].first;
            exec_block.insert(wd.result_block.getByPosition(left_columns + column_index));
        }
    }
    else
    {
        for (size_t i = 0; i < right_columns; ++i)
            exec_block.insert(wd.result_block.getByPosition(left_columns + i));
    }

    non_equal_conditions.other_cond_expr->execute(exec_block);

    size_t rows = exec_block.rows();
    RUNTIME_CHECK_MSG(
        rows <= settings.max_block_size,
        "exec_block rows {} > max_block_size {}",
        rows,
        settings.max_block_size);

    wd.filter.clear();
    mergeNullAndFilterResult(exec_block, wd.filter, non_equal_conditions.other_cond_name, false);

    size_t output_columns = output_block_after_finalize.columns();

    auto init_result_block_for_other_condition = [&]() {
        wd.result_block_for_other_condition = {};
        for (size_t i = 0; i < output_columns; ++i)
        {
            ColumnWithTypeAndName new_column = output_block_after_finalize.safeGetByPosition(i).cloneEmpty();
            new_column.column->assumeMutable()->reserveAlign(settings.max_block_size, FULL_VECTOR_SIZE_AVX2);
            wd.result_block_for_other_condition.insert(std::move(new_column));
        }
    };

    if (!wd.result_block_for_other_condition)
        init_result_block_for_other_condition();

    RUNTIME_CHECK_MSG(
        wd.result_block_for_other_condition.rows() < settings.max_block_size,
        "result_block_for_other_condition rows {} >= max_block_size {}",
        wd.result_block_for_other_condition.rows(),
        settings.max_block_size);
    size_t remaining_insert_size = settings.max_block_size - wd.result_block_for_other_condition.rows();
    size_t result_size = countBytesInFilter(wd.filter);

    bool filter_offsets_is_initialized = false;
    auto init_filter_offsets = [&]() {
        RUNTIME_CHECK(wd.filter.size() == rows);
        wd.filter_offsets.clear();
        wd.filter_offsets.reserve(result_size);
        filterImpl(&wd.filter[0], &wd.filter[rows], &join->base_offsets[0], wd.filter_offsets);
        RUNTIME_CHECK(wd.filter_offsets.size() == result_size);
        filter_offsets_is_initialized = true;
    };

    bool filter_selective_offsets_is_initialized = false;
    auto init_filter_selective_offsets = [&]() {
        RUNTIME_CHECK(wd.selective_offsets.size() == rows);
        wd.filter_selective_offsets.clear();
        wd.filter_selective_offsets.reserve(result_size);
        filterImpl(&wd.filter[0], &wd.filter[rows], &wd.selective_offsets[0], wd.filter_selective_offsets);
        RUNTIME_CHECK(wd.filter_selective_offsets.size() == result_size);
        filter_selective_offsets_is_initialized = true;
    };

    bool filter_row_ptrs_for_lm_is_initialized = false;
    auto init_filter_row_ptrs_for_lm = [&]() {
        RUNTIME_CHECK(wd.row_ptrs_for_lm.size() == rows);
        wd.filter_row_ptrs_for_lm.clear();
        wd.filter_row_ptrs_for_lm.reserve(result_size);
        filterImpl(&wd.filter[0], &wd.filter[rows], &wd.row_ptrs_for_lm[0], wd.filter_row_ptrs_for_lm);
        RUNTIME_CHECK(wd.filter_row_ptrs_for_lm.size() == result_size);
        filter_row_ptrs_for_lm_is_initialized = true;
    };

    auto fill_block = [&](size_t start, size_t length) {
        if constexpr (late_materialization)
        {
            for (auto [column_index, _] : row_layout.raw_required_key_column_indexes)
            {
                const auto & name = right_sample_block_pruned.getByPosition(column_index).name;
                if (!wd.result_block_for_other_condition.has(name))
                    continue;
                if unlikely (!filter_offsets_is_initialized)
                    init_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.getByName(name);
                auto & src_column = exec_block.getByName(name);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.filter_offsets, start, length);
            }
            for (size_t i = 0; i < row_layout.other_required_count_for_other_condition; ++i)
            {
                size_t column_index = row_layout.other_required_column_indexes[i].first;
                const auto & name = right_sample_block_pruned.getByPosition(column_index).name;
                if (!wd.result_block_for_other_condition.has(name))
                    continue;
                if unlikely (!filter_offsets_is_initialized)
                    init_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.getByName(name);
                auto & src_column = exec_block.getByName(name);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.filter_offsets, start, length);
            }

            if (!filter_row_ptrs_for_lm_is_initialized)
                init_filter_row_ptrs_for_lm();

            std::vector<size_t> actual_column_indexes;
            for (size_t i = row_layout.other_required_count_for_other_condition;
                 i < row_layout.other_required_column_indexes.size();
                 ++i)
            {
                size_t column_index = row_layout.other_required_column_indexes[i].first;
                const auto & name = right_sample_block_pruned.getByPosition(column_index).name;
                size_t actual_column_index = wd.result_block_for_other_condition.getPositionByName(name);
                actual_column_indexes.emplace_back(actual_column_index);
            }

            constexpr size_t step = 256;
            for (size_t i = start; i < start + length; i += step)
            {
                size_t end = i + step > start + length ? start + length : i + step;
                wd.insert_batch.clear();
                wd.insert_batch.insert(&wd.row_ptrs_for_lm[i], &wd.row_ptrs_for_lm[end]);
                for (auto column_index : actual_column_indexes)
                {
                    auto & des_column = wd.result_block_for_other_condition.getByPosition(column_index);
                    des_column.column->assumeMutable()->deserializeAndInsertFromPos(wd.insert_batch, true);
                }
            }
            for (auto column_index : actual_column_indexes)
            {
                auto & des_column = wd.result_block_for_other_condition.getByPosition(column_index);
                des_column.column->assumeMutable()->flushNTAlignBuffer();
            }
        }
        else
        {
            for (size_t i = 0; i < right_columns; ++i)
            {
                const auto & name = right_sample_block_pruned.getByPosition(i).name;
                if (!wd.result_block_for_other_condition.has(name))
                    continue;
                if unlikely (!filter_offsets_is_initialized)
                    init_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.getByName(name);
                auto & src_column = exec_block.getByName(name);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.filter_offsets, start, length);
            }
        }

        for (size_t i = 0; i < left_columns; ++i)
        {
            const auto & name = left_sample_block_pruned.getByPosition(i).name;
            if (!wd.result_block_for_other_condition.has(name))
                continue;
            auto & des_column = wd.result_block_for_other_condition.getByName(name);
            if (left_required_flag_for_other_condition[i])
            {
                if unlikely (!filter_offsets_is_initialized && !filter_selective_offsets_is_initialized)
                    init_filter_selective_offsets();
                if (filter_offsets_is_initialized)
                {
                    auto & src_column = exec_block.getByName(name);
                    des_column.column->assumeMutable()
                        ->insertSelectiveRangeFrom(*src_column.column.get(), wd.filter_offsets, start, length);
                }
                else
                {
                    auto & src_column = context.block.safeGetByPosition(i);
                    des_column.column->assumeMutable()->insertSelectiveRangeFrom(
                        *src_column.column.get(),
                        wd.filter_selective_offsets,
                        start,
                        length);
                }
                continue;
            }
            if unlikely (!filter_selective_offsets_is_initialized)
                init_filter_selective_offsets();
            auto & src_column = context.block.safeGetByPosition(i);
            des_column.column->assumeMutable()
                ->insertSelectiveRangeFrom(*src_column.column.get(), wd.filter_selective_offsets, start, length);
        }
    };

    size_t length = result_size > remaining_insert_size ? remaining_insert_size : result_size;
    fill_block(0, length);

    Block res_block;
    if (result_size >= remaining_insert_size)
    {
        res_block = wd.result_block_for_other_condition;
        init_result_block_for_other_condition();
        if (result_size > remaining_insert_size)
            fill_block(remaining_insert_size, result_size - remaining_insert_size);
    }
    else
    {
        res_block = output_block_after_finalize;
    }

    exec_block.clear();
    /// Remove the new column added from other condition expressions.
    join->removeUselessColumn(wd.result_block);

    assertBlocksHaveEqualStructure(
        wd.result_block,
        join->all_sample_block_pruned,
        "Join Probe reuses result_block for other condition");

    /// Clear the data in result_block.
    for (size_t i = 0; i < wd.result_block.columns(); ++i)
    {
        auto column = wd.result_block.getByPosition(i).column->assumeMutable();
        column->popBack(column->size());
        wd.result_block.getByPosition(i).column = std::move(column);
    }

    return res_block;
}

void joinProbeBlock(
    JoinProbeContext &,
    JoinProbeWorkerData &,
    HashJoinKeyMethod,
    ASTTableJoin::Kind,
    bool,
    const JoinNonEqualConditions &,
    const HashJoinSettings &,
    const HashJoinPointerTable &,
    const HashJoinRowLayout &,
    MutableColumns &,
    size_t)
{
    return;
}

} // namespace DB
