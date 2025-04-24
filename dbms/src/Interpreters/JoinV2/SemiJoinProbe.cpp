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

#define CALL3(KeyGetter, JoinType, has_other_eq_from_in_cond, tagged_pointer)                                         \
    {                                                                                                                 \
        func_ptr_has_null                                                                                             \
            = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, true, has_other_eq_from_in_cond, tagged_pointer>;  \
        func_ptr_no_null                                                                                              \
            = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, false, has_other_eq_from_in_cond, tagged_pointer>; \
    }

#define CALL2(KeyGetter, JoinType, has_other_eq_from_in_cond)            \
    {                                                                    \
        if (pointer_table.enableTaggedPointer())                         \
            CALL3(KeyGetter, JoinType, has_other_eq_from_in_cond, true)  \
        else                                                             \
            CALL3(KeyGetter, JoinType, has_other_eq_from_in_cond, false) \
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
    if unlikely (ctx.rows == 0)
        return join->output_block_after_finalize;

    if constexpr (kind == LeftOuterSemi || kind == LeftOuterAnti)
    {
        // Sanity check
        RUNTIME_CHECK(ctx.left_semi_match_res.size() == ctx.rows);
        if (!join->non_equal_conditions.other_eq_cond_from_in_name.empty())
            RUNTIME_CHECK(ctx.left_semi_match_null_res.size() == ctx.rows);
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

    MutableColumns added_columns(right_columns);
    for (size_t i = 0; i < right_columns; ++i)
        added_columns[i] = wd.result_block.safeGetByPosition(left_columns + i).column->assumeMutable();

    Stopwatch watch;
    if (pointer_table.enableProbePrefetch())
    {
        //probeFillColumnsPrefetch<KeyGetter, kind, has_null_map, has_other_eq_from_in_cond, tagged_pointer>(
        //    ctx,
        //    wd,
        //    added_columns);
    }
    else
    {
        probeFillColumnsFromList<KeyGetter, kind, has_null_map, has_other_eq_from_in_cond, tagged_pointer>(
            ctx,
            wd,
            added_columns);
        if (wd.result_block.rows() < settings.max_block_size)
            probeFillColumns<KeyGetter, kind, has_null_map, has_other_eq_from_in_cond, tagged_pointer>(
                ctx,
                wd,
                added_columns);
    }
    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    // Move the mutable column pointers back into the wd.result_block, dropping the extra reference (ref_count 2→1).
    // Alternative: added_columns.clear(); but that is less explicit and may misleadingly imply the columns are discarded.
    for (size_t i = 0; i < right_columns; ++i)
        wd.result_block.safeGetByPosition(left_columns + i).column = std::move(added_columns[i]);

    if (ctx.isProbeFinished()) {}
    return join->output_block_after_finalize;
}

static constexpr UInt16 INITIAL_PACE = 4;
static constexpr UInt16 MAX_PACE = 8192;

SEMI_JOIN_PROBE_HELPER_TEMPLATE
void SemiJoinProbeHelper::probeFillColumns(
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
    size_t current_offset = wd.result_block.rows();
    size_t idx = ctx.current_row_idx;
    size_t collision = 0;
    size_t key_offset = sizeof(RowPtr);
    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
    {
        key_offset += sizeof(HashValueType);
    }

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
                ptr = nullptr;
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
            if (key_is_equal)
            {
                ++current_offset;
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
        probe_row.has_null_eq_from_in = false;
        probe_row.pace = INITIAL_PACE * 2;
        probe_row.hash = hash;
        probe_row.key = key;
        if unlikely (current_offset >= settings.max_block_size)
        {
            if (ptr == nullptr)
                ++idx;
            break;
        }
    }

    ctx.current_row_idx = idx;
    wd.collision += collision;
}

SEMI_JOIN_PROBE_HELPER_TEMPLATE
void NO_INLINE SemiJoinProbeHelper::probeFillColumnsFromList(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
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
    auto iter_end = probe_list->end();
    for (auto iter = probe_list->begin(); iter != iter_end;)
    {
        auto & probe_row = *iter;
        RowPtr ptr = probe_row.build_row_ptr;
        auto idx = iter.getIndex();
        if (ptr == nullptr)
        {
            setNotMatched<kind, has_other_eq_from_in_cond>(ctx, idx, probe_row.has_null_eq_from_in);
            ++iter;
            probe_list->remove(idx);
            continue;
        }
        size_t end_offset = std::min(settings.max_block_size, current_offset + probe_row.pace);
        size_t prev_offset = current_offset;
        while (true)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, probe_row.key, key2, probe_row.hash, ptr);
            collision += !key_is_equal;
            if (key_is_equal)
            {
                ++current_offset;
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
            setNotMatched<kind, has_other_eq_from_in_cond>(ctx, idx, probe_row.has_null_eq_from_in);
            auto idx = iter.getIndex();
            ++iter;
            probe_list->remove(idx);
            continue;
        }
        probe_row.build_row_ptr = ptr;
        if (current_offset - prev_offset >= probe_row.pace)
            probe_row.pace = std::min<uint32_t>(MAX_PACE, probe_row.pace * 2U);
        if unlikely (current_offset >= settings.max_block_size)
            break;

        ++iter;
    }

    wd.collision += collision;
}

} // namespace DB
