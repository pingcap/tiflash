// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/JoinV2/SemiJoinProbe.h>

#include "Interpreters/JoinV2/SemiJoinProbeList.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "common/defines.h"
#include "ext/scope_guard.h"

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

using enum ASTTableJoin::Kind;

namespace
{
template <ASTTableJoin::Kind kind>
void ALWAYS_INLINE setMatched(JoinProbeContext & ctx, size_t idx)
{
    static_assert(kind == Semi || kind == Anti || kind == LeftOuterSemi || kind == LeftOuterAnti);
    if constexpr (kind == Semi)
    {
        ctx.semi_selective_offsets.push_back(idx);
    }
    else if constexpr (kind == LeftOuterSemi)
    {
        ctx.left_semi_match_res[idx] = 1;
    }
}

template <ASTTableJoin::Kind kind, bool check_other_eq_from_in_cond>
void ALWAYS_INLINE setNotMatched(JoinProbeContext & ctx, size_t idx, bool has_null_eq_from_in = false)
{
    static_assert(kind == Semi || kind == Anti || kind == LeftOuterSemi || kind == LeftOuterAnti);
    if constexpr (check_other_eq_from_in_cond)
    {
        if constexpr (kind == Anti)
        {
            if (!has_null_eq_from_in)
                ctx.semi_selective_offsets.push_back(idx);
        }
        else if constexpr (kind == LeftOuterSemi)
        {
            ctx.left_semi_match_null_res[idx] = has_null_eq_from_in;
        }
        else if constexpr (kind == LeftOuterAnti)
        {
            ctx.left_semi_match_res[idx] = 1;
            ctx.left_semi_match_null_res[idx] = has_null_eq_from_in;
        }
    }
    else
    {
        if constexpr (kind == Anti)
        {
            ctx.semi_selective_offsets.push_back(idx);
        }
        else if constexpr (kind == LeftOuterAnti)
        {
            ctx.left_semi_match_res[idx] = 1;
        }
    }
}

} // namespace

SemiJoinProbeHelper::SemiJoinProbeHelper(const HashJoin * join)
    : JoinProbeHelperUtil(join->settings, join->row_layout)
    , join(join)
    , pointer_table(join->pointer_table)
{
    // SemiJoinProbeHelper only handles semi join with other conditions
    RUNTIME_CHECK(join->has_other_condition);

#define CALL3(KeyGetter, JoinType, has_other_eq_cond_from_in, tagged_pointer)                                         \
    {                                                                                                                 \
        func_ptr_has_null                                                                                             \
            = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, true, has_other_eq_cond_from_in, tagged_pointer>;  \
        func_ptr_no_null                                                                                              \
            = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, false, has_other_eq_cond_from_in, tagged_pointer>; \
    }

#define CALL2(KeyGetter, JoinType, has_other_eq_cond_from_in)            \
    {                                                                    \
        if (pointer_table.enableTaggedPointer())                         \
            CALL3(KeyGetter, JoinType, has_other_eq_cond_from_in, true)  \
        else                                                             \
            CALL3(KeyGetter, JoinType, has_other_eq_cond_from_in, false) \
    }

#define CALL1(KeyGetter, JoinType)                                         \
    {                                                                      \
        if (join->non_equal_conditions.other_eq_cond_from_in_name.empty()) \
            CALL2(KeyGetter, JoinType, false)                              \
        else                                                               \
            CALL2(KeyGetter, JoinType, true)                               \
    }

#define CALL(KeyGetter)                                                                                    \
    {                                                                                                      \
        auto kind = join->kind;                                                                            \
        if (kind == Semi)                                                                                  \
            CALL1(KeyGetter, Semi)                                                                         \
        else if (kind == Anti)                                                                             \
            CALL1(KeyGetter, Anti)                                                                         \
        else if (kind == LeftOuterSemi)                                                                    \
            CALL1(KeyGetter, LeftOuterSemi)                                                                \
        else if (kind == LeftOuterAnti)                                                                    \
            CALL1(KeyGetter, LeftOuterAnti)                                                                \
        else                                                                                               \
            throw Exception(                                                                               \
                fmt::format("Logical error: unknown combination of JOIN {}", magic_enum::enum_name(kind)), \
                ErrorCodes::LOGICAL_ERROR);                                                                \
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
}

bool SemiJoinProbeHelper::isSupported(ASTTableJoin::Kind kind, bool has_other_condition)
{
    return has_other_condition && (kind == Semi || kind == Anti || kind == LeftOuterSemi || kind == LeftOuterAnti);
}

Block SemiJoinProbeHelper::probe(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    if (ctx.null_map)
        return (this->*func_ptr_has_null)(ctx, wd);
    else
        return (this->*func_ptr_no_null)(ctx, wd);
}

SEMI_JOIN_PROBE_HELPER_TEMPLATE
Block SemiJoinProbeHelper::probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    static_assert(kind == Semi || kind == Anti || kind == LeftOuterAnti || kind == LeftOuterSemi);

    if unlikely (ctx.rows == 0)
        return join->output_block_after_finalize;

    if constexpr (kind == LeftOuterSemi || kind == LeftOuterAnti)
    {
        // Sanity check
        RUNTIME_CHECK(ctx.left_semi_match_res.size() == ctx.rows);
        if (!join->non_equal_conditions.other_eq_cond_from_in_name.empty())
            RUNTIME_CHECK(ctx.left_semi_match_null_res.size() == ctx.rows);
    }

    wd.selective_offsets.clear();
    wd.selective_offsets.reserve(settings.max_block_size);

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

    MutableColumns added_columns(right_columns);
    for (size_t i = 0; i < right_columns; ++i)
        added_columns[i] = wd.result_block.safeGetByPosition(left_columns + i).column->assumeMutable();

    Stopwatch watch;
    if (pointer_table.enableProbePrefetch())
    {
        probeFillColumnsPrefetch<KeyGetter, kind, has_null_map, has_other_eq_cond_from_in, tagged_pointer>(
            ctx,
            wd,
            added_columns);
    }
    else
    {
        probeFillColumns<KeyGetter, kind, has_null_map, has_other_eq_cond_from_in, tagged_pointer>(
            ctx,
            wd,
            added_columns);
    }
    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    // Move the mutable column pointers back into the wd.result_block, dropping the extra reference (ref_count 2→1).
    // Alternative: added_columns.clear(); but that is less explicit and may misleadingly imply the columns are discarded.
    for (size_t i = 0; i < right_columns; ++i)
        wd.result_block.safeGetByPosition(left_columns + i).column = std::move(added_columns[i]);

    if (!wd.selective_offsets.empty())
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            if (!join->left_required_flag_for_other_condition[i])
                continue;
            wd.result_block.safeGetByPosition(i).column->assumeMutable()->insertSelectiveFrom(
                *ctx.block.safeGetByPosition(i).column.get(),
                wd.selective_offsets);
        }

        wd.replicate_time += watch.elapsedFromLastTime();

        handleOtherConditions<KeyGetter, kind>(ctx, wd);
    }

    if (ctx.isProbeFinished())
    {
        if constexpr (kind == Semi || kind == Anti)
            return genResultBlockForSemi(ctx);
        else
            return genResultBlockForLeftOuterSemi(ctx, has_other_eq_cond_from_in);
    }

    return join->output_block_after_finalize;
}

static constexpr UInt16 INITIAL_PACE = 2;
static constexpr UInt16 MAX_PACE = 8192;

SEMI_JOIN_PROBE_HELPER_TEMPLATE
void NO_INLINE
SemiJoinProbeHelper::probeFillColumns(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    auto & key_getter = *static_cast<KeyGetterType *>(ctx.key_getter.get());
    auto * probe_list = static_cast<SemiJoinProbeList<KeyGetter> *>(ctx.semi_join_probe_list.get());
    RUNTIME_CHECK(probe_list->slotCapacity() == ctx.rows);
    size_t current_offset = wd.result_block.rows();
    size_t collision = 0;
    size_t key_offset = sizeof(RowPtr);
    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
    {
        key_offset += sizeof(HashValueType);
    }

    SCOPE_EXIT({
        flushInsertBatch<false, true>(wd, added_columns);
        fillNullMapWithZero<false>(added_columns);

        wd.collision += collision;
    });

    auto iter_end = probe_list->end();
    for (auto iter = probe_list->begin(); iter != iter_end;)
    {
        auto & probe_row = *iter;
        RowPtr ptr = probe_row.build_row_ptr;
        auto idx = iter.getIndex();
        size_t end_offset = std::min(settings.max_block_size, current_offset + probe_row.pace);
        size_t prev_offset = current_offset;
        while (ptr != nullptr)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, probe_row.key, key2, probe_row.hash, ptr);
            collision += !key_is_equal;
            current_offset += key_is_equal;
            if (key_is_equal)
            {
                wd.selective_offsets.push_back(idx);
                insertRowToBatch<false>(wd, added_columns, ptr + key_offset + key_getter.getRequiredKeyOffset(key2));
                if unlikely (current_offset >= end_offset)
                {
                    ptr = getNextRowPtr(ptr);
                    break;
                }
            }

            ptr = getNextRowPtr(ptr);
        }
        if (prev_offset == current_offset)
        {
            setNotMatched<kind, has_other_eq_cond_from_in>(ctx, idx, probe_row.has_null_eq_from_in);
            iter = probe_list->remove(iter);
            continue;
        }
        probe_row.build_row_ptr = ptr;
        if (current_offset - prev_offset >= probe_row.pace)
            probe_row.pace = std::min<uint32_t>(MAX_PACE, probe_row.pace * 2U);
        if unlikely (current_offset >= settings.max_block_size)
            break;

        ++iter;
    }

    if (current_offset >= settings.max_block_size)
        return;

    size_t idx = ctx.current_row_idx;
    for (; idx < ctx.rows; ++idx)
    {
        if constexpr (has_null_map)
        {
            if ((*ctx.null_map)[idx])
            {
                setNotMatched<kind, false>(ctx, idx);
                continue;
            }
        }

        const auto & key = key_getter.getJoinKeyWithBuffer(idx);
        auto hash = static_cast<HashValueType>(Hash()(key));
        UInt16 hash_tag = hash & ROW_PTR_TAG_MASK;
        RowPtr ptr = pointer_table.getHeadPointer(hash);
        if (ptr == nullptr)
        {
            setNotMatched<kind, false>(ctx, idx);
            continue;
        }

        if constexpr (tagged_pointer)
        {
            if (!containOtherTag(ptr, hash_tag))
            {
                setNotMatched<kind, false>(ctx, idx);
                continue;
            }
            ptr = removeRowPtrTag(ptr);
        }

        size_t end_offset = std::min(settings.max_block_size, current_offset + INITIAL_PACE);
        size_t prev_offset = current_offset;
        while (true)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, key, key2, hash, ptr);
            collision += !key_is_equal;
            current_offset += key_is_equal;
            if (key_is_equal)
            {
                wd.selective_offsets.push_back(idx);
                insertRowToBatch<false>(wd, added_columns, ptr + key_offset + key_getter.getRequiredKeyOffset(key2));
                if unlikely (current_offset >= end_offset)
                {
                    ptr = getNextRowPtr(ptr);
                    break;
                }
            }

            ptr = getNextRowPtr(ptr);
            if (ptr == nullptr)
                break;
        }
        if (prev_offset == current_offset)
        {
            setNotMatched<kind, false>(ctx, idx);
            continue;
        }
        probe_list->append(idx);
        auto & probe_row = probe_list->at(idx);
        probe_row.build_row_ptr = ptr;
        if constexpr (has_other_eq_cond_from_in)
            probe_row.has_null_eq_from_in = false;
        probe_row.pace = std::min<uint32_t>(MAX_PACE, INITIAL_PACE * 2U);
        probe_row.hash = hash;
        probe_row.key = key;
        if unlikely (current_offset >= settings.max_block_size)
        {
            ++idx;
            break;
        }
    }
    ctx.current_row_idx = idx;
}

enum class ProbePrefetchStage : UInt8
{
    None,
    FindHeader,
    FindNext,
};

template <typename KeyGetter>
struct ProbePrefetchState
{
    using KeyGetterType = typename KeyGetter::Type;
    using KeyType = typename KeyGetterType::KeyType;
    using HashValueType = typename KeyGetter::HashValueType;

    ProbePrefetchStage stage = ProbePrefetchStage::None;
    bool is_matched : 1 = false;
    bool has_null_eq_from_in : 1 = false;
    union
    {
        /// Used when stage is FindHeader
        UInt16 hash_tag = 0;
        /// Used when stage is FindNext
        UInt16 remaining_pace;
    };
    UInt32 index = 0;
    HashValueType hash = 0;
    KeyType key{};
    union
    {
        /// Used when stage is FindHeader
        std::atomic<RowPtr> * pointer_ptr = nullptr;
        /// Used when stage is FindNext
        RowPtr ptr;
    };
};

#define PREFETCH_READ(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)

SEMI_JOIN_PROBE_HELPER_TEMPLATE
void NO_INLINE SemiJoinProbeHelper::probeFillColumnsPrefetch(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;

    auto & key_getter = *static_cast<KeyGetterType *>(ctx.key_getter.get());
    auto * probe_list = static_cast<SemiJoinProbeList<KeyGetter> *>(ctx.semi_join_probe_list.get());
    RUNTIME_CHECK(probe_list->slotCapacity() == ctx.rows);
    const size_t probe_prefetch_step = settings.probe_prefetch_step;
    if unlikely (!ctx.prefetch_states)
    {
        ctx.prefetch_states = decltype(ctx.prefetch_states)(
            static_cast<void *>(new ProbePrefetchState<KeyGetter>[probe_prefetch_step]),
            [](void * ptr) { delete[] static_cast<ProbePrefetchState<KeyGetter> *>(ptr); });
    }
    auto * states = static_cast<ProbePrefetchState<KeyGetter> *>(ctx.prefetch_states.get());

    size_t idx = ctx.current_row_idx;
    size_t active_states = ctx.prefetch_active_states;
    size_t k = ctx.prefetch_iter;
    size_t current_offset = wd.result_block.rows();
    size_t collision = 0;
    constexpr size_t key_offset
        = sizeof(RowPtr) + (KeyGetterType::joinKeyCompareHashFirst() ? sizeof(HashValueType) : 0);

    size_t list_active_slots = probe_list->activeSlots();
    auto iter = probe_list->begin();
    auto iter_end = probe_list->end();
    while (idx < ctx.rows || active_states > 0 || list_active_slots > 0)
    {
        k = k == probe_prefetch_step ? 0 : k;
        auto * state = &states[k];
        if (state->stage == ProbePrefetchStage::FindNext)
        {
            RowPtr ptr = state->ptr;
            RowPtr next_ptr = getNextRowPtr(ptr);
            state->ptr = next_ptr;

            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, state->key, key2, state->hash, ptr);
            collision += !key_is_equal;
            state->is_matched |= key_is_equal;
            current_offset += key_is_equal;
            state->remaining_pace -= key_is_equal;
            bool remaining_pace_is_zero = false;
            if (key_is_equal)
            {
                wd.selective_offsets.push_back(state->index);
                insertRowToBatch<false>(wd, added_columns, ptr + key_offset + key_getter.getRequiredKeyOffset(key2));
                if unlikely (current_offset >= settings.max_block_size)
                {
                    probe_list->at(state->index).build_row_ptr = next_ptr;
                    state->stage = ProbePrefetchStage::None;
                    --active_states;
                    break;
                }
                if unlikely (state->remaining_pace == 0)
                {
                    auto & probe_row = probe_list->at(state->index);
                    probe_row.build_row_ptr = next_ptr;
                    probe_row.pace = std::min<uint32_t>(MAX_PACE, probe_row.pace * 2U);
                    remaining_pace_is_zero = true;
                }
            }

            if likely (!remaining_pace_is_zero)
            {
                if (next_ptr)
                {
                    PREFETCH_READ(next_ptr);
                    ++k;
                    continue;
                }

                probe_list->at(state->index).build_row_ptr = next_ptr;
                if (!state->is_matched)
                {
                    setNotMatched<kind, has_other_eq_cond_from_in>(ctx, state->index, state->has_null_eq_from_in);
                    probe_list->remove(state->index);
                }
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
                    state->stage = ProbePrefetchStage::FindNext;
                    state->is_matched = false;
                    if constexpr (has_other_eq_cond_from_in)
                        state->has_null_eq_from_in = false;
                    state->remaining_pace = INITIAL_PACE;
                    state->ptr = ptr;
                    ++k;

                    probe_list->append(state->index);
                    auto & probe_row = probe_list->at(state->index);
                    probe_row.build_row_ptr = ptr;
                    if constexpr (has_other_eq_cond_from_in)
                        probe_row.has_null_eq_from_in = false;
                    probe_row.pace = INITIAL_PACE;
                    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
                        probe_row.hash = state->hash;
                    probe_row.key = state->key;
                    continue;
                }
            }

            state->stage = ProbePrefetchStage::None;
            --active_states;

            setNotMatched<kind, false>(ctx, state->index);
        }

        assert(state->stage == ProbePrefetchStage::None);

        while (list_active_slots > 0 && !iter->build_row_ptr)
        {
            setNotMatched<kind, has_other_eq_cond_from_in>(ctx, iter.getIndex(), iter->has_null_eq_from_in);
            iter = probe_list->remove(iter);
            --list_active_slots;
        }

        if (list_active_slots > 0)
        {
            assert(iter != iter_end);
            assert(iter->build_row_ptr);

            auto & probe_row = *iter;
            PREFETCH_READ(probe_row.build_row_ptr);
            state->stage = ProbePrefetchStage::FindNext;
            state->is_matched = false;
            if constexpr (has_other_eq_cond_from_in)
                state->has_null_eq_from_in = probe_row.has_null_eq_from_in;
            state->remaining_pace = probe_row.pace;
            if constexpr (KeyGetterType::joinKeyCompareHashFirst())
                state->hash = probe_row.hash;
            state->key = probe_row.key;
            state->ptr = probe_row.build_row_ptr;

            ++iter;
            --list_active_slots;
            ++active_states;
            ++k;
            continue;
        }

        if constexpr (has_null_map)
        {
            while (idx < ctx.rows)
            {
                if (!(*ctx.null_map)[idx])
                    break;

                setNotMatched<kind, false>(ctx, idx);
                ++idx;
            }
        }

        if unlikely (idx >= ctx.rows)
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

    for (size_t i = 0; i < probe_prefetch_step; ++i)
    {
        auto * state = &states[i];
        if (state->stage == ProbePrefetchStage::FindNext)
        {
            auto & probe_row = probe_list->at(state->index);
            probe_row.build_row_ptr = state->ptr;
        }
    }

    flushInsertBatch<false, true>(wd, added_columns);
    fillNullMapWithZero<false>(added_columns);

    ctx.current_row_idx = idx;
    ctx.prefetch_active_states = active_states;
    ctx.prefetch_iter = k;
    wd.collision += collision;
}

template <typename KeyGetter, ASTTableJoin::Kind kind>
void SemiJoinProbeHelper::handleOtherConditions(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    const auto & left_sample_block_pruned = join->left_sample_block_pruned;
    const auto & right_sample_block_pruned = join->right_sample_block_pruned;
    const auto & non_equal_conditions = join->non_equal_conditions;
    const auto & left_required_flag_for_other_condition = join->left_required_flag_for_other_condition;

    size_t left_columns = left_sample_block_pruned.columns();
    size_t right_columns = right_sample_block_pruned.columns();
    // Some columns in wd.result_block may be empty so need to create another block to execute other condition expressions
    Block exec_block;
    RUNTIME_CHECK(wd.result_block.columns() == left_columns + right_columns);
    for (size_t i = 0; i < left_columns; ++i)
    {
        if (left_required_flag_for_other_condition[i])
            exec_block.insert(wd.result_block.getByPosition(i));
    }
    for (size_t i = 0; i < right_columns; ++i)
        exec_block.insert(wd.result_block.getByPosition(left_columns + i));

    non_equal_conditions.other_cond_expr->execute(exec_block);

    SCOPE_EXIT({
        RUNTIME_CHECK(wd.result_block.columns() == left_columns + right_columns);
        /// Clear the data in result_block.
        for (size_t i = 0; i < left_columns + right_columns; ++i)
        {
            auto column = wd.result_block.getByPosition(i).column->assumeMutable();
            column->popBack(column->size());
            wd.result_block.getByPosition(i).column = std::move(column);
        }
    });

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

#define CALL(has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map)                               \
    {                                                                                                          \
        checkExprResults<KeyGetter, kind, has_other_eq_cond_from_in, has_other_cond, has_other_cond_null_map>( \
            ctx,                                                                                               \
            wd.selective_offsets,                                                                              \
            other_eq_from_in_column_data,                                                                      \
            other_eq_from_in_null_map,                                                                         \
            other_column_data,                                                                                 \
            other_null_map);                                                                                   \
    }

    if (has_other_eq_cond_from_in)
    {
        if (has_other_cond)
        {
            if (has_other_cond_null_map)
                CALL(true, true, true)
            else
                CALL(true, true, false)
        }
        else
            CALL(true, false, false)
    }
    else
    {
        RUNTIME_CHECK(has_other_cond);
        if (has_other_cond_null_map)
            CALL(false, true, true)
        else
            CALL(false, true, false)
    }
#undef CALL
}

template <
    typename KeyGetter,
    ASTTableJoin::Kind kind,
    bool has_other_eq_cond_from_in,
    bool has_other_cond,
    bool has_other_cond_null_map>
void SemiJoinProbeHelper::checkExprResults(
    JoinProbeContext & ctx,
    IColumn::Offsets & selective_offsets,
    const ColumnUInt8::Container * other_eq_column,
    ConstNullMapPtr other_eq_null_map,
    const ColumnUInt8::Container * other_column,
    ConstNullMapPtr other_null_map)
{
    static_assert(has_other_cond || has_other_eq_cond_from_in);
    auto * probe_list = static_cast<SemiJoinProbeList<KeyGetter> *>(ctx.semi_join_probe_list.get());
    size_t sz = selective_offsets.size();
    if constexpr (has_other_eq_cond_from_in)
    {
        RUNTIME_CHECK(sz == other_eq_column->size());
        RUNTIME_CHECK(sz == other_eq_null_map->size());
    }
    if constexpr (has_other_cond)
    {
        RUNTIME_CHECK(sz == other_column->size());
        if constexpr (has_other_cond_null_map)
            RUNTIME_CHECK(sz == other_null_map->size());
    }
    for (size_t i = 0; i < sz; ++i)
    {
        auto index = selective_offsets[i];
        if (!probe_list->contains(index))
            continue;
        if constexpr (has_other_cond)
        {
            if constexpr (has_other_cond_null_map)
            {
                if ((*other_null_map)[i])
                {
                    // If other expr is NULL, this row is not included in the result set.
                    continue;
                }
            }
            if (!(*other_column)[i])
            {
                // If other expr is 0, this row is not included in the result set.
                continue;
            }
        }
        if constexpr (has_other_eq_cond_from_in)
        {
            auto & probe_row = probe_list->at(index);
            bool is_eq_null = (*other_eq_null_map)[i];
            probe_row.has_null_eq_from_in |= is_eq_null;
            if (!is_eq_null && (*other_eq_column)[i])
            {
                setMatched<kind>(ctx, index);
                probe_list->remove(index);
            }
        }
        else
        {
            // other expr is true, so the result is true for this row that has matched right row(s).
            setMatched<kind>(ctx, index);
            probe_list->remove(index);
        }
    }
}

Block SemiJoinProbeHelper::genResultBlockForSemi(JoinProbeContext & ctx)
{
    RUNTIME_CHECK(join->kind == Semi || join->kind == Anti);
    RUNTIME_CHECK(ctx.isProbeFinished());

    Block res_block = join->output_block_after_finalize.cloneEmpty();
    size_t columns = res_block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & dst_column = res_block.getByPosition(i);
        dst_column.column->assumeMutable()->insertSelectiveFrom(
            *ctx.block.getByName(dst_column.name).column.get(),
            ctx.semi_selective_offsets);
    }

    return res_block;
}

Block SemiJoinProbeHelper::genResultBlockForLeftOuterSemi(JoinProbeContext & ctx, bool has_other_eq_cond_from_in)
{
    RUNTIME_CHECK(join->kind == LeftOuterSemi || join->kind == LeftOuterAnti);
    RUNTIME_CHECK(ctx.isProbeFinished());

    Block res_block = join->output_block_after_finalize.cloneEmpty();
    size_t columns = res_block.columns();
    size_t match_helper_column_index = res_block.getPositionByName(join->match_helper_name);
    for (size_t i = 0; i < columns; ++i)
    {
        if (i == match_helper_column_index)
            continue;
        res_block.getByPosition(i) = ctx.block.getByName(res_block.getByPosition(i).name);
    }

    MutableColumnPtr match_helper_column_ptr = res_block.getByPosition(match_helper_column_index).column->cloneEmpty();
    auto * match_helper_column = typeid_cast<ColumnNullable *>(match_helper_column_ptr.get());
    if (has_other_eq_cond_from_in)
        match_helper_column->getNullMapColumn().getData().swap(ctx.left_semi_match_null_res);
    else
        match_helper_column->getNullMapColumn().getData().resize_fill_zero(ctx.rows);
    auto * match_helper_res = &typeid_cast<ColumnVector<Int8> &>(match_helper_column->getNestedColumn()).getData();
    match_helper_res->swap(ctx.left_semi_match_res);

    res_block.getByPosition(match_helper_column_index).column = std::move(match_helper_column_ptr);

    return res_block;
}

} // namespace DB
