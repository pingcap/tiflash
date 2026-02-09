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
#include <Interpreters/JoinV2/SemiJoinProbe.h>
#include <Interpreters/NullableUtils.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <ext/scope_guard.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

using enum ASTTableJoin::Kind;

bool JoinProbeContext::isProbeFinished() const
{
    return current_row_idx >= rows
        // For prefetching
        && prefetch_active_states == 0
        // For (left outer) (anti) semi join with other conditions
        && (semi_join_probe_list == nullptr || semi_join_probe_list->activeSlots() == 0);
}

bool JoinProbeContext::isAllFinished() const
{
    return isProbeFinished()
        // For left outer with other conditions
        && rows_not_matched.empty();
}

void JoinProbeContext::resetBlock(Block & block_)
{
    block = block_;
    orignal_block = block_;
    rows = block.rows();
    current_row_idx = 0;
    current_build_row_ptr = nullptr;
    current_row_is_matched = false;

    prefetch_active_states = 0;

    is_prepared = false;
    materialized_key_columns.clear();
    key_columns.clear();
    null_map = nullptr;
    null_map_holder = nullptr;
}

void JoinProbeContext::prepareForHashProbe(
    HashJoinKeyMethod method,
    ASTTableJoin::Kind kind,
    bool has_other_condition,
    bool has_other_eq_cond_from_in,
    const Names & key_names,
    const String & filter_column,
    const NameSet & probe_output_name_set,
    const Block & sample_block_pruned,
    const TiDB::TiDBCollators & collators,
    const HashJoinRowLayout & row_layout)
{
    if (is_prepared)
        return;

    key_columns = extractAndMaterializeKeyColumns(block, materialized_key_columns, key_names);
    /// Keys with NULL value in any column won't join to anything.
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter won't join to anything
    recordFilteredRows(block, filter_column, null_map_holder, null_map);
    /// Some useless columns maybe key columns and filter column so they must be removed after extracting.
    for (size_t pos = 0; pos < block.columns();)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }

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

    if (kind == LeftOuter && has_other_condition)
    {
        rows_not_matched.clear();
        rows_not_matched.resize_fill(rows, 1);
        not_matched_offsets_idx = -1;
        not_matched_offsets.clear();
    }
    if (kind == LeftOuterSemi || kind == LeftOuterAnti)
    {
        left_semi_match_res.clear();
        left_semi_match_res.resize_fill_zero(rows);
        if (has_other_eq_cond_from_in)
        {
            left_semi_match_null_res.clear();
            left_semi_match_null_res.resize_fill_zero(rows);
        }
    }
    if ((kind == Semi || kind == Anti) && has_other_condition)
    {
        semi_selective_offsets.clear();
        semi_selective_offsets.reserve(rows);
    }

    if (SemiJoinProbeHelper::isSupported(kind, has_other_condition))
    {
        if unlikely (!semi_join_probe_list)
            semi_join_probe_list = createSemiJoinProbeList(method);
        semi_join_probe_list->reset(rows);
    }

    is_prepared = true;
}

template <bool late_materialization, bool is_right_semi_join, bool last_flush>
void JoinProbeHelperUtil::flushInsertBatch(JoinProbeWorkerData & wd, MutableColumns & added_columns) const
{
    for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
    {
        IColumn * column = added_columns[column_index].get();
        if (is_nullable)
            column = &static_cast<ColumnNullable &>(*column).getNestedColumn();
        column->deserializeAndInsertFromPos(wd.insert_batch, true);
        if constexpr (last_flush)
            column->flushNTAlignBuffer();
    }

    size_t add_size;
    if constexpr (late_materialization || is_right_semi_join)
        add_size = row_layout.other_column_count_for_other_condition;
    else
        add_size = row_layout.other_column_indexes.size();
    for (size_t i = 0; i < add_size; ++i)
    {
        size_t column_index = row_layout.other_column_indexes[i].first;
        added_columns[column_index]->deserializeAndInsertFromPos(wd.insert_batch, true);
        if constexpr (last_flush)
            added_columns[column_index]->flushNTAlignBuffer();
    }
    if constexpr (late_materialization && !is_right_semi_join)
        wd.row_ptrs_for_lm.insert(wd.insert_batch.begin(), wd.insert_batch.end());

    wd.insert_batch.clear();
}

void JoinProbeHelperUtil::fillNullMapWithZero(MutableColumns & added_columns) const
{
    for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
    {
        if (is_nullable)
        {
            RUNTIME_CHECK(added_columns[column_index]->isColumnNullable());
            auto & nullable_column = static_cast<ColumnNullable &>(*added_columns[column_index]);
            size_t data_size = nullable_column.getNestedColumn().size();
            size_t nullmap_size = nullable_column.getNullMapColumn().size();
            RUNTIME_CHECK(nullmap_size <= data_size);
            nullable_column.getNullMapColumn().getData().resize_fill_zero(data_size);
        }
    }
}

template <bool has_other_condition, bool late_materialization>
struct JoinProbeAdder<Inner, has_other_condition, late_materialization>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = false;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        MutableColumns & added_columns,
        size_t idx,
        size_t & current_offset,
        RowPtr row_ptr,
        size_t ptr_offset)
    {
        ++current_offset;
        wd.selective_offsets.push_back(idx);
        helper.insertRowToBatch<late_materialization, false>(wd, added_columns, row_ptr + ptr_offset);
        return current_offset >= helper.settings.max_block_size;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper & helper, JoinProbeWorkerData & wd, MutableColumns & added_columns)
    {
        helper.flushInsertBatch<late_materialization, false, true>(wd, added_columns);
        helper.fillNullMapWithZero(added_columns);
    }
};

template <bool has_other_condition, bool late_materialization>
struct JoinProbeAdder<LeftOuter, has_other_condition, late_materialization>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = !has_other_condition;
    static constexpr bool break_on_first_match = false;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        MutableColumns & added_columns,
        size_t idx,
        size_t & current_offset,
        RowPtr row_ptr,
        size_t ptr_offset)
    {
        ++current_offset;
        wd.selective_offsets.push_back(idx);
        helper.insertRowToBatch<late_materialization, false>(wd, added_columns, row_ptr + ptr_offset);
        return current_offset >= helper.settings.max_block_size;
    }

    static bool ALWAYS_INLINE addNotMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        size_t idx,
        size_t & current_offset)
    {
        if constexpr (!has_other_condition)
        {
            ++current_offset;
            wd.not_matched_selective_offsets.push_back(idx);
            return current_offset >= helper.settings.max_block_size;
        }
        return false;
    }

    static void flush(JoinProbeHelper & helper, JoinProbeWorkerData & wd, MutableColumns & added_columns)
    {
        helper.flushInsertBatch<late_materialization, false, true>(wd, added_columns);
        helper.fillNullMapWithZero(added_columns);

        if constexpr (!has_other_condition)
        {
            if (!wd.not_matched_selective_offsets.empty())
            {
                size_t null_size = wd.not_matched_selective_offsets.size();
                for (auto & column : added_columns)
                    column->insertManyDefaults(null_size);
                wd.selective_offsets.insert(
                    wd.not_matched_selective_offsets.begin(),
                    wd.not_matched_selective_offsets.end());
                wd.not_matched_selective_offsets.clear();
            }
        }
    }
};

template <>
struct JoinProbeAdder<Semi, false, false>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = true;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
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

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

template <>
struct JoinProbeAdder<Anti, false, false>
{
    static constexpr bool need_matched = false;
    static constexpr bool need_not_matched = true;
    static constexpr bool break_on_first_match = true;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper &,
        JoinProbeContext &,
        JoinProbeWorkerData &,
        MutableColumns &,
        size_t,
        size_t &,
        RowPtr,
        size_t)
    {
        return false;
    }

    static bool ALWAYS_INLINE addNotMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        size_t idx,
        size_t & current_offset)
    {
        ++current_offset;
        wd.selective_offsets.push_back(idx);
        return current_offset >= helper.settings.max_block_size;
    }

    static void flush(JoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

template <>
struct JoinProbeAdder<LeftOuterSemi, false, false>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = true;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper &,
        JoinProbeContext & ctx,
        JoinProbeWorkerData &,
        MutableColumns &,
        size_t idx,
        size_t &,
        RowPtr,
        size_t)
    {
        ctx.left_semi_match_res[idx] = 1;
        return false;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

template <>
struct JoinProbeAdder<LeftOuterAnti, false, false>
{
    static constexpr bool need_matched = false;
    static constexpr bool need_not_matched = true;
    static constexpr bool break_on_first_match = true;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper &,
        JoinProbeContext &,
        JoinProbeWorkerData &,
        MutableColumns &,
        size_t,
        size_t &,
        RowPtr,
        size_t)
    {
        return false;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext & ctx, JoinProbeWorkerData &, size_t idx, size_t &)
    {
        ctx.left_semi_match_res[idx] = 1;
        return false;
    }

    static void flush(JoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

template <bool has_other_condition, bool late_materialization>
struct JoinProbeAdder<RightOuter, has_other_condition, late_materialization>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = false;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        MutableColumns & added_columns,
        size_t idx,
        size_t & current_offset,
        RowPtr row_ptr,
        size_t ptr_offset)
    {
        if constexpr (has_other_condition)
        {
            wd.right_join_row_ptrs.push_back(hasRowPtrMatchedFlag(row_ptr) ? nullptr : row_ptr);
        }
        else
        {
            setRowPtrMatchedFlag(row_ptr);
        }

        ++current_offset;
        wd.selective_offsets.push_back(idx);
        helper.insertRowToBatch<late_materialization, false>(wd, added_columns, row_ptr + ptr_offset);
        return current_offset >= helper.settings.max_block_size;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper & helper, JoinProbeWorkerData & wd, MutableColumns & added_columns)
    {
        helper.flushInsertBatch<late_materialization, false, true>(wd, added_columns);
        helper.fillNullMapWithZero(added_columns);
    }
};

template <ASTTableJoin::Kind kind>
    requires(kind == RightSemi || kind == RightAnti)
struct JoinProbeAdder<kind, false, false>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = false;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper &,
        JoinProbeContext &,
        JoinProbeWorkerData &,
        MutableColumns &,
        size_t,
        size_t &,
        RowPtr row_ptr,
        size_t)
    {
        setRowPtrMatchedFlag(row_ptr);
        return false;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper &, JoinProbeWorkerData &, MutableColumns &) {}
};

template <ASTTableJoin::Kind kind>
    requires(kind == RightSemi || kind == RightAnti)
struct JoinProbeAdder<kind, true, false>
{
    static constexpr bool need_matched = true;
    static constexpr bool need_not_matched = false;
    static constexpr bool break_on_first_match = false;

    static bool ALWAYS_INLINE addMatched(
        JoinProbeHelper & helper,
        JoinProbeContext &,
        JoinProbeWorkerData & wd,
        MutableColumns & added_columns,
        size_t idx,
        size_t & current_offset,
        RowPtr row_ptr,
        size_t ptr_offset)
    {
        if (hasRowPtrMatchedFlag(row_ptr))
            return false;
        ++current_offset;
        wd.selective_offsets.push_back(idx);
        wd.right_join_row_ptrs.push_back(row_ptr);
        helper.insertRowToBatch<false, true>(wd, added_columns, row_ptr + ptr_offset);
        return current_offset >= helper.settings.max_block_size;
    }

    static bool ALWAYS_INLINE
    addNotMatched(JoinProbeHelper &, JoinProbeContext &, JoinProbeWorkerData &, size_t, size_t &)
    {
        return false;
    }

    static void flush(JoinProbeHelper & helper, JoinProbeWorkerData & wd, MutableColumns & added_columns)
    {
        helper.flushInsertBatch<false, true, true>(wd, added_columns);
        helper.fillNullMapWithZero(added_columns);
    }
};

JoinProbeHelper::JoinProbeHelper(const HashJoin * join, bool late_materialization)
    : JoinProbeHelperUtil(join->settings, join->row_layout)
    , join(join)
    , pointer_table(join->pointer_table)
{
#define SET_FUNC_PTR(KeyGetter, JoinType, has_other_condition, late_materialization, tagged_pointer)                \
    {                                                                                                               \
        func_ptr_has_null                                                                                           \
            = &JoinProbeHelper::                                                                                    \
                  probeImpl<KeyGetter, JoinType, true, has_other_condition, late_materialization, tagged_pointer>;  \
        func_ptr_no_null                                                                                            \
            = &JoinProbeHelper::                                                                                    \
                  probeImpl<KeyGetter, JoinType, false, has_other_condition, late_materialization, tagged_pointer>; \
    }

#define CALL2(KeyGetter, JoinType, has_other_condition, late_materialization)                   \
    {                                                                                           \
        if (pointer_table.enableTaggedPointer())                                                \
            SET_FUNC_PTR(KeyGetter, JoinType, has_other_condition, late_materialization, true)  \
        else                                                                                    \
            SET_FUNC_PTR(KeyGetter, JoinType, has_other_condition, late_materialization, false) \
    }

#define CALL1(KeyGetter, JoinType)                      \
    {                                                   \
        if (join->has_other_condition)                  \
        {                                               \
            if (late_materialization)                   \
                CALL2(KeyGetter, JoinType, true, true)  \
            else                                        \
                CALL2(KeyGetter, JoinType, true, false) \
        }                                               \
        else                                            \
            CALL2(KeyGetter, JoinType, false, false)    \
    }

#define CALL(KeyGetter)                                                                                    \
    {                                                                                                      \
        auto kind = join->kind;                                                                            \
        bool has_other_condition = join->has_other_condition;                                              \
        if (kind == Inner)                                                                                 \
            CALL1(KeyGetter, Inner)                                                                        \
        else if (kind == LeftOuter)                                                                        \
            CALL1(KeyGetter, LeftOuter)                                                                    \
        else if (kind == RightOuter)                                                                       \
            CALL1(KeyGetter, RightOuter)                                                                   \
        else if (kind == Semi && !has_other_condition)                                                     \
            CALL2(KeyGetter, Semi, false, false)                                                           \
        else if (kind == Anti && !has_other_condition)                                                     \
            CALL2(KeyGetter, Anti, false, false)                                                           \
        else if (kind == LeftOuterSemi && !has_other_condition)                                            \
            CALL2(KeyGetter, LeftOuterSemi, false, false)                                                  \
        else if (kind == LeftOuterAnti && !has_other_condition)                                            \
            CALL2(KeyGetter, LeftOuterAnti, false, false)                                                  \
        else if (kind == RightSemi && has_other_condition)                                                 \
            CALL2(KeyGetter, RightSemi, true, false)                                                       \
        else if (kind == RightSemi && !has_other_condition)                                                \
            CALL2(KeyGetter, RightSemi, false, false)                                                      \
        else if (kind == RightAnti && has_other_condition)                                                 \
            CALL2(KeyGetter, RightAnti, true, false)                                                       \
        else if (kind == RightAnti && !has_other_condition)                                                \
            CALL2(KeyGetter, RightAnti, false, false)                                                      \
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
#undef SET_FUNC_PTR
}

Block JoinProbeHelper::probe(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    if (ctx.null_map)
        return (this->*func_ptr_has_null)(ctx, wd);
    else
        return (this->*func_ptr_no_null)(ctx, wd);
}

JOIN_PROBE_HELPER_TEMPLATE
Block JoinProbeHelper::probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    static_assert(has_other_condition || !late_materialization);

    if unlikely (ctx.rows == 0)
        return join->output_block_after_finalize;

    if constexpr (kind == LeftOuter && has_other_condition)
    {
        if (ctx.isProbeFinished())
            return fillNotMatchedRowsForLeftOuter(ctx, wd);
    }
    if constexpr (kind == LeftOuterSemi || kind == LeftOuterAnti)
    {
        // Sanity check
        RUNTIME_CHECK(ctx.left_semi_match_res.size() == ctx.rows);
    }

    wd.insert_batch.clear();
    wd.insert_batch.reserve(settings.probe_insert_batch_size);
    wd.selective_offsets.clear();
    wd.selective_offsets.reserve(settings.max_block_size);
    if constexpr (kind == LeftOuter && !has_other_condition)
    {
        wd.not_matched_selective_offsets.clear();
        wd.not_matched_selective_offsets.reserve(settings.max_block_size);
    }
    if constexpr (late_materialization)
    {
        wd.row_ptrs_for_lm.clear();
        wd.row_ptrs_for_lm.reserve(settings.max_block_size);
    }
    if constexpr (kind == RightSemi || kind == RightAnti)
    {
        wd.right_join_row_ptrs.clear();
        wd.right_join_row_ptrs.reserve(settings.max_block_size);
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
        probeFillColumnsPrefetch<
            KeyGetter,
            kind,
            has_null_map,
            has_other_condition,
            late_materialization,
            tagged_pointer>(ctx, wd, added_columns);
    else
        probeFillColumns<KeyGetter, kind, has_null_map, has_other_condition, late_materialization, tagged_pointer>(
            ctx,
            wd,
            added_columns);
    wd.probe_hash_table_time += watch.elapsedFromLastTime();

    // Move the mutable column pointers back into the wd.result_block, dropping the extra reference (ref_count 2→1).
    // Alternative: added_columns.clear(); but that is less explicit and may misleadingly imply the columns are discarded.
    for (size_t i = 0; i < right_columns; ++i)
        wd.result_block.safeGetByPosition(left_columns + i).column = std::move(added_columns[i]);

    if constexpr (
        kind == Inner || kind == LeftOuter || kind == RightOuter || kind == Semi || kind == Anti || kind == RightSemi
        || kind == RightAnti)
    {
        if (wd.selective_offsets.empty())
            return join->output_block_after_finalize;
    }

    if constexpr (kind == LeftOuterSemi || kind == LeftOuterAnti)
    {
        return genResultBlockForLeftOuterSemi(ctx);
    }

    if constexpr (has_other_condition)
    {
        // Always using late materialization for left side columns
        for (size_t i = 0; i < left_columns; ++i)
        {
            if (!join->left_required_flag_for_other_condition[i])
                continue;
            wd.result_block.safeGetByPosition(i).column->assumeMutable()->insertSelectiveFrom(
                *ctx.block.safeGetByPosition(i).column.get(),
                wd.selective_offsets);
        }
    }
    else
    {
        for (size_t i = 0; i < left_columns; ++i)
        {
            wd.result_block.safeGetByPosition(i).column->assumeMutable()->insertSelectiveFrom(
                *ctx.block.safeGetByPosition(i).column.get(),
                wd.selective_offsets);
        }
    }

    wd.replicate_time += watch.elapsedFromLastTime();

    if constexpr (has_other_condition)
    {
        auto res_block = handleOtherConditions(ctx, wd, kind, late_materialization);
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

JOIN_PROBE_HELPER_TEMPLATE
void JoinProbeHelper::probeFillColumns(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    using Adder = JoinProbeAdder<kind, has_other_condition, late_materialization>;

    auto & key_getter = *static_cast<KeyGetterType *>(ctx.key_getter.get());
    // Some columns in wd.result_block may remain empty due to late materialization for join with other conditions.
    // But since all columns are cleared after handling other conditions, wd.result_block.rows() is always 0.
    size_t current_offset = wd.result_block.rows();
    if constexpr (has_other_condition)
        RUNTIME_CHECK(current_offset == 0);
    size_t idx = ctx.current_row_idx;
    RowPtr ptr = ctx.current_build_row_ptr;
    bool is_matched = ctx.current_row_is_matched;
    size_t collision = 0;
    constexpr size_t key_offset
        = sizeof(RowPtr) + (KeyGetterType::joinKeyCompareHashFirst() ? sizeof(HashValueType) : 0);

#define NOT_MATCHED(not_matched)                                                     \
    if constexpr (Adder::need_not_matched)                                           \
    {                                                                                \
        assert(ptr == nullptr);                                                      \
        if (not_matched)                                                             \
        {                                                                            \
            bool is_end = Adder::addNotMatched(*this, ctx, wd, idx, current_offset); \
            if unlikely (is_end)                                                     \
            {                                                                        \
                ++idx;                                                               \
                break;                                                               \
            }                                                                        \
        }                                                                            \
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
            if constexpr (Adder::need_not_matched)
                is_matched |= key_is_equal;
            if (key_is_equal)
            {
                if constexpr ((kind == RightSemi || kind == RightAnti) && !has_other_condition)
                {
                    if (hasRowPtrMatchedFlag(ptr))
                    {
                        ptr = nullptr;
                        break;
                    }
                }

                if constexpr (Adder::need_matched)
                {
                    bool is_end = Adder::addMatched(
                        *this,
                        ctx,
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

            ptr = getNextRowPtr<kind>(ptr);
            if (ptr == nullptr)
                break;
        }
        if unlikely (ptr != nullptr)
        {
            ptr = getNextRowPtr<kind>(ptr);
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

/// The implemtation of prefetching in join probe process is inspired by a paper named
/// `Asynchronous Memory Access Chaining` in vldb-15.
/// Ref: https://www.vldb.org/pvldb/vol9/p252-kocberber.pdf
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
    bool is_matched = false;
    UInt16 hash_tag = 0;
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

JOIN_PROBE_HELPER_TEMPLATE
void JoinProbeHelper::probeFillColumnsPrefetch(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    MutableColumns & added_columns)
{
    using KeyGetterType = typename KeyGetter::Type;
    using Hash = typename KeyGetter::Hash;
    using HashValueType = typename KeyGetter::HashValueType;
    using Adder = JoinProbeAdder<kind, has_other_condition, late_materialization>;

    auto & key_getter = *static_cast<KeyGetterType *>(ctx.key_getter.get());
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
    // Some columns in wd.result_block may remain empty due to late materialization for join with other conditions.
    // But since all columns are cleared after handling other conditions, wd.result_block.rows() is always 0.
    size_t current_offset = wd.result_block.rows();
    if constexpr (has_other_condition)
        RUNTIME_CHECK(current_offset == 0);
    size_t collision = 0;
    constexpr size_t key_offset
        = sizeof(RowPtr) + (KeyGetterType::joinKeyCompareHashFirst() ? sizeof(HashValueType) : 0);

#define NOT_MATCHED(not_matched, idx)                                                \
    if constexpr (Adder::need_not_matched)                                           \
    {                                                                                \
        if (not_matched)                                                             \
        {                                                                            \
            bool is_end = Adder::addNotMatched(*this, ctx, wd, idx, current_offset); \
            if unlikely (is_end)                                                     \
                break;                                                               \
        }                                                                            \
    }

    while (idx < ctx.rows || active_states > 0)
    {
        k = k == probe_prefetch_step ? 0 : k;
        auto * state = &states[k];
        if (state->stage == ProbePrefetchStage::FindNext)
        {
            RowPtr ptr = state->ptr;
            RowPtr next_ptr = getNextRowPtr<kind>(ptr);
            state->ptr = next_ptr;

            const auto & key2 = key_getter.deserializeJoinKey(ptr + key_offset);
            bool key_is_equal = joinKeyIsEqual(key_getter, state->key, key2, state->hash, ptr);
            collision += !key_is_equal;
            if constexpr (Adder::need_not_matched)
                state->is_matched |= key_is_equal;
            if constexpr ((kind == RightSemi || kind == RightAnti) && !has_other_condition)
            {
                if (key_is_equal && hasRowPtrMatchedFlag(ptr))
                {
                    next_ptr = nullptr;
                    key_is_equal = false;
                }
            }
            if (key_is_equal)
            {
                if constexpr (Adder::need_matched)
                {
                    bool is_end = Adder::addMatched(
                        *this,
                        ctx,
                        wd,
                        added_columns,
                        state->index,
                        current_offset,
                        ptr,
                        key_offset + key_getter.getRequiredKeyOffset(key2));
                    if unlikely (is_end)
                    {
                        if constexpr (Adder::break_on_first_match)
                            next_ptr = nullptr;

                        if (!next_ptr)
                        {
                            state->stage = ProbePrefetchStage::None;
                            --active_states;
                        }
                        break;
                    }
                }

                if constexpr (Adder::break_on_first_match)
                    next_ptr = nullptr;
            }

            if (next_ptr)
            {
                PREFETCH_READ(next_ptr);
                ++k;
                continue;
            }

            state->stage = ProbePrefetchStage::None;
            --active_states;

            NOT_MATCHED(!state->is_matched, state->index);
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

            NOT_MATCHED(true, state->index);
        }

        assert(state->stage == ProbePrefetchStage::None);

        if constexpr (has_null_map)
        {
            bool is_end = false;
            while (idx < ctx.rows)
            {
                if (!(*ctx.null_map)[idx])
                    break;

                if constexpr (Adder::need_not_matched)
                {
                    is_end = Adder::addNotMatched(*this, ctx, wd, idx, current_offset);
                    if unlikely (is_end)
                    {
                        ++idx;
                        break;
                    }
                }

                ++idx;
            }
            if constexpr (Adder::need_not_matched)
            {
                if unlikely (is_end)
                    break;
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
        if constexpr (Adder::need_not_matched)
            state->is_matched = false;
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

    Adder::flush(*this, wd, added_columns);

    ctx.current_row_idx = idx;
    ctx.prefetch_active_states = active_states;
    ctx.prefetch_iter = k;
    wd.collision += collision;

#undef NOT_MATCHED
}

Block JoinProbeHelper::handleOtherConditions(
    JoinProbeContext & ctx,
    JoinProbeWorkerData & wd,
    ASTTableJoin::Kind kind,
    bool late_materialization)
{
    const auto & left_sample_block_pruned = join->left_sample_block_pruned;
    const auto & right_sample_block_pruned = join->right_sample_block_pruned;
    const auto & output_block_after_finalize = join->output_block_after_finalize;
    const auto & non_equal_conditions = join->non_equal_conditions;
    const auto & output_column_indexes = join->output_column_indexes;
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
    if (late_materialization)
    {
        for (auto [column_index, _] : row_layout.raw_key_column_indexes)
            exec_block.insert(wd.result_block.getByPosition(left_columns + column_index));
        for (size_t i = 0; i < row_layout.other_column_count_for_other_condition; ++i)
        {
            size_t column_index = row_layout.other_column_indexes[i].first;
            exec_block.insert(wd.result_block.getByPosition(left_columns + column_index));
        }
    }
    else
    {
        for (size_t i = 0; i < right_columns; ++i)
            exec_block.insert(wd.result_block.getByPosition(left_columns + i));
    }

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

    size_t rows = exec_block.rows();
    // Ensure BASE_OFFSETS is accessed within bound.
    // It must be true because max_block_size <= BASE_OFFSETS.size(HASH_JOIN_MAX_BLOCK_SIZE_UPPER_BOUND).
    RUNTIME_CHECK_MSG(
        rows <= BASE_OFFSETS.size(),
        "exec_block rows {} > base_offsets size {}",
        rows,
        BASE_OFFSETS.size());

    wd.filter.clear();
    mergeNullAndFilterResult(exec_block, wd.filter, non_equal_conditions.other_cond_name, false);
    exec_block.clear();

    if (kind == LeftOuter)
    {
        RUNTIME_CHECK(wd.selective_offsets.size() == rows);
        RUNTIME_CHECK(wd.filter.size() == rows);
        for (size_t i = 0; i < rows; ++i)
        {
            size_t idx = wd.selective_offsets[i];
            bool is_matched = wd.filter[i];
            ctx.rows_not_matched[idx] &= !is_matched;
        }
    }
    else if (kind == RightOuter)
    {
        RUNTIME_CHECK(wd.right_join_row_ptrs.size() == rows);
        RUNTIME_CHECK(wd.filter.size() == rows);
        for (size_t i = 0; i < rows; ++i)
        {
            bool is_matched = wd.filter[i];
            if (is_matched && wd.right_join_row_ptrs[i])
                setRowPtrMatchedFlag(wd.right_join_row_ptrs[i]);
        }
    }
    else if (isRightSemiFamily(kind))
    {
        RUNTIME_CHECK(wd.right_join_row_ptrs.size() == rows);
        RUNTIME_CHECK(wd.filter.size() == rows);
        for (size_t i = 0; i < rows; ++i)
        {
            bool is_matched = wd.filter[i];
            if (is_matched)
                setRowPtrMatchedFlag(wd.right_join_row_ptrs[i]);
        }
        return output_block_after_finalize;
    }

    join->initOutputBlock(wd.result_block_for_other_condition);

    RUNTIME_CHECK_MSG(
        wd.result_block_for_other_condition.rows() < settings.max_block_size,
        "result_block_for_other_condition rows {} >= max_block_size {}",
        wd.result_block_for_other_condition.rows(),
        settings.max_block_size);
    size_t remaining_insert_size = settings.max_block_size - wd.result_block_for_other_condition.rows();
    size_t result_size = countBytesInFilter(wd.filter);

    bool block_filter_offsets_is_initialized = false;
    auto init_block_filter_offsets = [&]() {
        RUNTIME_CHECK(wd.filter.size() == rows);
        wd.block_filter_offsets.clear();
        wd.block_filter_offsets.reserve(result_size);
        filterImpl(&wd.filter[0], &wd.filter[rows], &BASE_OFFSETS[0], wd.block_filter_offsets);
        RUNTIME_CHECK(wd.block_filter_offsets.size() == result_size);
        block_filter_offsets_is_initialized = true;
    };

    bool result_block_filter_offsets_is_initialized = false;
    auto init_result_block_filter_offsets = [&]() {
        RUNTIME_CHECK(wd.selective_offsets.size() == rows);
        wd.result_block_filter_offsets.clear();
        wd.result_block_filter_offsets.reserve(result_size);
        filterImpl(&wd.filter[0], &wd.filter[rows], &wd.selective_offsets[0], wd.result_block_filter_offsets);
        RUNTIME_CHECK(wd.result_block_filter_offsets.size() == result_size);
        result_block_filter_offsets_is_initialized = true;
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

    auto fill_matched = [&](size_t start, size_t length) {
        if (length == 0)
            return;

        if (late_materialization)
        {
            for (auto [column_index, _] : row_layout.raw_key_column_indexes)
            {
                auto output_index = output_column_indexes.at(left_columns + column_index);
                if (output_index < 0)
                    continue;
                if unlikely (!block_filter_offsets_is_initialized)
                    init_block_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
                auto & src_column = wd.result_block.safeGetByPosition(left_columns + column_index);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.block_filter_offsets, start, length);
            }
            for (size_t i = 0; i < row_layout.other_column_count_for_other_condition; ++i)
            {
                size_t column_index = row_layout.other_column_indexes[i].first;
                auto output_index = output_column_indexes.at(left_columns + column_index);
                if (output_index < 0)
                    continue;
                if unlikely (!block_filter_offsets_is_initialized)
                    init_block_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
                auto & src_column = wd.result_block.safeGetByPosition(left_columns + column_index);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.block_filter_offsets, start, length);
            }

            if (!filter_row_ptrs_for_lm_is_initialized)
                init_filter_row_ptrs_for_lm();

            if (row_layout.other_column_count_for_other_condition < row_layout.other_column_indexes.size())
            {
                auto other_column_indexes_start = row_layout.other_column_count_for_other_condition;
                auto other_column_indexes_size = row_layout.other_column_indexes.size();
                // Sanity check: all columns after other_column_indexes_start should be included in wd.result_block_for_other_condition.
                for (size_t i = other_column_indexes_start; i < other_column_indexes_size; ++i)
                {
                    size_t column_index = row_layout.other_column_indexes[i].first;
                    auto output_index = output_column_indexes.at(left_columns + column_index);
                    RUNTIME_CHECK(output_index >= 0);
                    RUNTIME_CHECK(static_cast<size_t>(output_index) < wd.result_block_for_other_condition.columns());
                }
                constexpr size_t step = 256;
                for (size_t pos = start; pos < start + length; pos += step)
                {
                    size_t end = pos + step > start + length ? start + length : pos + step;
                    wd.insert_batch.clear();
                    wd.insert_batch.insert(&wd.filter_row_ptrs_for_lm[pos], &wd.filter_row_ptrs_for_lm[end]);
                    for (size_t i = other_column_indexes_start; i < other_column_indexes_size; ++i)
                    {
                        size_t column_index = row_layout.other_column_indexes[i].first;
                        auto output_index = output_column_indexes[left_columns + column_index];
                        auto & des_column = wd.result_block_for_other_condition.getByPosition(output_index);
                        des_column.column->assumeMutable()->deserializeAndInsertFromPos(wd.insert_batch, true);
                    }
                }
                for (size_t i = other_column_indexes_start; i < other_column_indexes_size; ++i)
                {
                    size_t column_index = row_layout.other_column_indexes[i].first;
                    auto output_index = output_column_indexes[left_columns + column_index];
                    auto & des_column = wd.result_block_for_other_condition.getByPosition(output_index);
                    des_column.column->assumeMutable()->flushNTAlignBuffer();
                }
            }
        }
        else
        {
            for (size_t i = 0; i < right_columns; ++i)
            {
                auto output_index = output_column_indexes.at(left_columns + i);
                if (output_index < 0)
                    continue;
                if unlikely (!block_filter_offsets_is_initialized)
                    init_block_filter_offsets();
                auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
                auto & src_column = wd.result_block.safeGetByPosition(left_columns + i);
                des_column.column->assumeMutable()
                    ->insertSelectiveRangeFrom(*src_column.column.get(), wd.block_filter_offsets, start, length);
            }
        }

        for (size_t i = 0; i < left_columns; ++i)
        {
            auto output_index = output_column_indexes.at(i);
            if (output_index < 0)
                continue;
            auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
            if (left_required_flag_for_other_condition[i])
            {
                if unlikely (!block_filter_offsets_is_initialized && !result_block_filter_offsets_is_initialized)
                    init_result_block_filter_offsets();
                if (block_filter_offsets_is_initialized)
                {
                    auto & src_column = wd.result_block.safeGetByPosition(i);
                    des_column.column->assumeMutable()
                        ->insertSelectiveRangeFrom(*src_column.column.get(), wd.block_filter_offsets, start, length);
                }
                else
                {
                    auto & src_column = ctx.block.safeGetByPosition(i);
                    des_column.column->assumeMutable()->insertSelectiveRangeFrom(
                        *src_column.column.get(),
                        wd.result_block_filter_offsets,
                        start,
                        length);
                }
                continue;
            }
            if unlikely (!result_block_filter_offsets_is_initialized)
                init_result_block_filter_offsets();
            auto & src_column = ctx.block.safeGetByPosition(i);
            des_column.column->assumeMutable()
                ->insertSelectiveRangeFrom(*src_column.column.get(), wd.result_block_filter_offsets, start, length);
        }
    };

    size_t length = std::min(result_size, remaining_insert_size);
    fill_matched(0, length);
    if (result_size >= remaining_insert_size)
    {
        Block res_block;
        res_block.swap(wd.result_block_for_other_condition);
        if (result_size > remaining_insert_size)
        {
            join->initOutputBlock(wd.result_block_for_other_condition);
            fill_matched(remaining_insert_size, result_size - remaining_insert_size);
        }

        return res_block;
    }

    if (kind == LeftOuter && ctx.isProbeFinished())
        return fillNotMatchedRowsForLeftOuter(ctx, wd);

    return output_block_after_finalize;
}

Block JoinProbeHelper::fillNotMatchedRowsForLeftOuter(JoinProbeContext & ctx, JoinProbeWorkerData & wd)
{
    RUNTIME_CHECK(join->kind == LeftOuter);
    RUNTIME_CHECK(join->has_other_condition);
    RUNTIME_CHECK(ctx.isProbeFinished());
    if (ctx.not_matched_offsets_idx < 0)
    {
        size_t rows = ctx.rows;
        size_t not_matched_result_size = countBytesInFilter(ctx.rows_not_matched);
        auto & offsets = ctx.not_matched_offsets;

        offsets.clear();
        offsets.reserve(not_matched_result_size);
        if likely (rows <= BASE_OFFSETS.size())
        {
            filterImpl(&ctx.rows_not_matched[0], &ctx.rows_not_matched[rows], &BASE_OFFSETS[0], offsets);
            RUNTIME_CHECK(offsets.size() == not_matched_result_size);
        }
        else
        {
            for (size_t i = 0; i < rows; ++i)
            {
                if (ctx.rows_not_matched[i])
                    offsets.push_back(i);
            }
        }

        ctx.not_matched_offsets_idx = 0;
    }
    const auto & output_block_after_finalize = join->output_block_after_finalize;

    if (static_cast<size_t>(ctx.not_matched_offsets_idx) >= ctx.not_matched_offsets.size())
    {
        // JoinProbeContext::isAllFinished checks if all not matched rows have been output
        // by verifying whether rows_not_matched is empty.
        ctx.rows_not_matched.clear();
        return output_block_after_finalize;
    }

    join->initOutputBlock(wd.result_block_for_other_condition);

    size_t left_columns = join->left_sample_block_pruned.columns();
    size_t right_columns = join->right_sample_block_pruned.columns();

    if (wd.result_block_for_other_condition.rows() >= settings.max_block_size)
    {
        Block res_block;
        res_block.swap(wd.result_block_for_other_condition);
        return res_block;
    }

    size_t remaining_insert_size = settings.max_block_size - wd.result_block_for_other_condition.rows();
    size_t result_size = ctx.not_matched_offsets.size() - ctx.not_matched_offsets_idx;
    size_t length = std::min(result_size, remaining_insert_size);

    const auto & output_column_indexes = join->output_column_indexes;
    for (size_t i = 0; i < right_columns; ++i)
    {
        auto output_index = output_column_indexes.at(left_columns + i);
        if (output_index < 0)
            continue;
        auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
        des_column.column->assumeMutable()->insertManyDefaults(length);
    }

    for (size_t i = 0; i < left_columns; ++i)
    {
        auto output_index = output_column_indexes.at(i);
        if (output_index < 0)
            continue;
        auto & des_column = wd.result_block_for_other_condition.safeGetByPosition(output_index);
        auto & src_column = ctx.block.safeGetByPosition(i);
        des_column.column->assumeMutable()->insertSelectiveRangeFrom(
            *src_column.column.get(),
            ctx.not_matched_offsets,
            ctx.not_matched_offsets_idx,
            length);
    }
    ctx.not_matched_offsets_idx += length;

    if (static_cast<size_t>(ctx.not_matched_offsets_idx) >= ctx.not_matched_offsets.size())
    {
        // JoinProbeContext::isAllFinished checks if all not matched rows have been output
        // by verifying whether rows_not_matched is empty.
        ctx.rows_not_matched.clear();
    }

    Block res_block;
    res_block.swap(wd.result_block_for_other_condition);
    return res_block;
}

Block JoinProbeHelper::genResultBlockForLeftOuterSemi(JoinProbeContext & ctx)
{
    RUNTIME_CHECK(join->kind == LeftOuterSemi || join->kind == LeftOuterAnti);
    RUNTIME_CHECK(!join->has_other_condition);
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
    match_helper_column->getNullMapColumn().getData().resize_fill_zero(ctx.rows);
    auto * match_helper_res = &typeid_cast<ColumnVector<Int8> &>(match_helper_column->getNestedColumn()).getData();
    match_helper_res->swap(ctx.left_semi_match_res);

    res_block.getByPosition(match_helper_column_index).column = std::move(match_helper_column_ptr);

    return res_block;
}

// SemiJoinProbe.cpp calls this function
template void DB::JoinProbeHelperUtil::flushInsertBatch<false, false, true>(
    DB::JoinProbeWorkerData & wd,
    DB::MutableColumns & added_columns) const;

} // namespace DB
