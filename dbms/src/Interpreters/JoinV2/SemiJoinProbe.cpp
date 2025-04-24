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

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

using enum ASTTableJoin::Kind;

enum class SemiJoinProbeResType : UInt8
{
    FALSE_VALUE,
    TRUE_VALUE,
    NULL_VALUE,
};

template <>
struct SemiJoinProbeAdder<Semi>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = true;

    static bool ALWAYS_INLINE addMatched(
        SemiJoinProbeHelper & helper,
        JoinProbeWorkerData & wd,
        MutableColumns &,
        size_t idx,
        size_t & current_offset,
        RowPtr,
        size_t)
    {
        ++current_offset;
        wd.selective_offsets.push_back(idx);
        return current_offset >= helper.settings.max_block_size;
    }

    static bool ALWAYS_INLINE addNotMatched(SemiJoinProbeHelper &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(SemiJoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

SemiJoinProbeHelper::SemiJoinProbeHelper(const HashJoin * join)
    : JoinProbeHelperUtil(join->settings, join->row_layout)
    , join(join)
    , pointer_table(join->pointer_table)
{
    RUNTIME_CHECK(join->has_other_condition);

#define CALL2(KeyGetter, JoinType, tagged_pointer)                                                      \
    {                                                                                                   \
        func_ptr_has_null = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, true, tagged_pointer>; \
        func_ptr_no_null = &SemiJoinProbeHelper::probeImpl<KeyGetter, JoinType, false, tagged_pointer>; \
    }

#define CALL1(KeyGetter, JoinType)               \
    {                                            \
        if (pointer_table.enableTaggedPointer()) \
            CALL2(KeyGetter, JoinType, true)     \
        else                                     \
            CALL2(KeyGetter, JoinType, false)    \
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

template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer>
Block SemiJoinProbeHelper::probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    if unlikely (ctx.rows == 0)
        return join->output_block_after_finalize;

    auto * probe_list = static_cast<SemiJoinPendingProbeList *>(ctx.semi_join_pending_probe_list.get());
    RUNTIME_CHECK(probe_list->slotSize() == ctx.rows);

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
        probeFillColumnsPrefetch<KeyGetter, kind, has_null_map, tagged_pointer, true>(ctx, wd, added_columns);

        probeFillColumnsPrefetch<KeyGetter, kind, has_null_map, tagged_pointer, false>(ctx, wd, added_columns);
    }
    else
    {
        probeFillColumns<KeyGetter, kind, has_null_map, tagged_pointer, true>(ctx, wd, added_columns);

        probeFillColumns<KeyGetter, kind, has_null_map, tagged_pointer, false>(ctx, wd, added_columns);
    }
    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    // Move the mutable column pointers back into the wd.result_block, dropping the extra reference (ref_count 2→1).
    // Alternative: added_columns.clear(); but that is less explicit and may misleadingly imply the columns are discarded.
    for (size_t i = 0; i < right_columns; ++i)
        wd.result_block.safeGetByPosition(left_columns + i).column = std::move(added_columns[i]);

    if (ctx.isProbeFinished()) 
    {

    }
    return join->output_block_after_finalize;
}

template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer, bool fill_list>
void SemiJoinProbeHelper::probeFillColumns(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    using Adder = SemiJoinProbeAdder<kind>;

    auto & key_getter = *static_cast<KeyGetterType *>(ctx.key_getter.get());
    size_t current_offset = wd.result_block.rows();
    size_t idx = ctx.current_row_idx;
    RowPtr ptr = ctx.current_build_row_ptr;
    bool is_matched = ctx.current_row_is_matched;
    size_t collision = 0;
    size_t key_offset = sizeof(RowPtr);
    if constexpr (KeyGetterType::joinKeyCompareHashFirst())
    {
        key_offset += sizeof(HashValueType);
    }

#define NOT_MATCHED(not_matched)                                                \
    if constexpr (Adder::need_not_matched)                                      \
    {                                                                           \
        assert(ptr == nullptr);                                                 \
        if (not_matched)                                                        \
        {                                                                       \
            bool is_end = Adder::addNotMatched(*this, wd, idx, current_offset); \
            if unlikely (is_end)                                                \
            {                                                                   \
                ++idx;                                                          \
                break;                                                          \
            }                                                                   \
        }                                                                       \
    }

    for (; idx < ctx.rows; ++idx)
    {
        if constexpr (has_null_map)
        {
            if ((*ctx.null_map)[idx])
            {
                NOT_MATCHED(true)
                continue;
            }
        }

        const auto & key = key_getter.getJoinKey(idx);
        auto hash = static_cast<HashValueType>(Hash()(key));
        UInt16 hash_tag = hash & ROW_PTR_TAG_MASK;
        if likely (ptr == nullptr)
        {
            ptr = pointer_table.getHeadPointer(hash);
            if (ptr == nullptr)
            {
                NOT_MATCHED(true)
                continue;
            }

            if constexpr (tagged_pointer)
            {
                if (!containOtherTag(ptr, hash_tag))
                {
                    ptr = nullptr;
                    NOT_MATCHED(true)
                    continue;
                }
                ptr = removeRowPtrTag(ptr);
            }
            if constexpr (Adder::need_not_matched)
                is_matched = false;
        }
        while (true)
        {
            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, key, key2, hash, ptr);
            collision += !key_is_equal;
            if (key_is_equal)
            {
                if constexpr (Adder::need_not_matched)
                    is_matched = true;

                if constexpr (Adder::need_matched)
                {
                    bool is_end = Adder::addMatched(
                        *this,
                        wd,
                        added_columns,
                        idx,
                        current_offset,
                        ptr,
                        key_offset + key_getter.getRequiredKeyOffset(key2));

                    if unlikely (is_end)
                    {
                        if constexpr (Adder::break_on_first_match)
                            ptr = nullptr;
                        break;
                    }
                }

                if constexpr (Adder::break_on_first_match)
                {
                    ptr = nullptr;
                    break;
                }
            }

            ptr = getNextRowPtr(ptr);
            if (ptr == nullptr)
                break;
        }
        if unlikely (ptr != nullptr)
        {
            ptr = getNextRowPtr(ptr);
            if (ptr == nullptr)
                ++idx;
            break;
        }
        NOT_MATCHED(!is_matched)
    }

    Adder::flush(*this, wd, added_columns);

    ctx.current_row_idx = idx;
    ctx.current_build_row_ptr = ptr;
    ctx.current_row_is_matched = is_matched;
    wd.collision += collision;

#undef NOT_MATCHED
}

template <typename KeyGetter, ASTTableJoin::Kind kind, bool has_null_map, bool tagged_pointer, bool fill_list>
void SemiJoinProbeHelper::probeFillColumnsPrefetch(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{}

} // namespace DB
