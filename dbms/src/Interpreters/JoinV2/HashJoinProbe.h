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

#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/JoinV2/HashJoinKey.h>
#include <Interpreters/JoinV2/HashJoinPointerTable.h>
#include <Interpreters/JoinV2/HashJoinSettings.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <absl/base/optimization.h>


namespace DB
{

struct JoinProbeContext
{
    Block block;
    /// original_block ensures that the reference counts for the key columns are never zero.
    Block orignal_block;
    size_t rows = 0;
    size_t start_row_idx = 0;
    RowPtr current_row_ptr = nullptr;
    /// For left outer/(left outer) (anti) semi join without other conditions.
    bool current_row_is_matched = false;
    /// For left outer/(left outer) (anti) semi join with other conditions.
    IColumn::Filter rows_not_matched;
    /// < 0 means not_matched_offsets is not initialized.
    ssize_t not_matched_offsets_idx = -1;
    IColumn::Offsets not_matched_offsets;

    size_t prefetch_active_states = 0;
    size_t prefetch_iter = 0;
    std::unique_ptr<void, std::function<void(void *)>> prefetch_states;

    bool is_prepared = false;
    Columns materialized_columns;
    ColumnRawPtrs key_columns;
    ColumnPtr null_map_holder = nullptr;
    ConstNullMapPtr null_map = nullptr;
    std::unique_ptr<void, std::function<void(void *)>> key_getter;

    bool input_is_finished = false;

    bool isProbeFinished() const;
    bool isAllFinished() const;
    void resetBlock(Block & block_);

    void prepareForHashProbe(
        HashJoinKeyMethod method,
        ASTTableJoin::Kind kind,
        bool has_other_condition,
        const Names & key_names,
        const String & filter_column,
        const NameSet & probe_output_name_set,
        const Block & sample_block_pruned,
        const TiDB::TiDBCollators & collators,
        const HashJoinRowLayout & row_layout);
};

struct alignas(CPU_CACHE_LINE_SIZE) JoinProbeWorkerData
{
    IColumn::Offsets selective_offsets;
    /// For left outer join with no other condition
    IColumn::Offsets not_matched_selective_offsets;
    /// For left outer (anti) semi join with no other condition
    PaddedPODArray<Int8> match_helper_res;

    RowPtrs insert_batch;

    /// For other condition
    ColumnVector<UInt8>::Container filter;
    IColumn::Offsets filter_offsets;
    IColumn::Offsets filter_selective_offsets;
    /// For late materialization
    RowPtrs row_ptrs_for_lm;
    RowPtrs filter_row_ptrs_for_lm;

    /// Schema: HashJoin::all_sample_block_pruned
    Block result_block;
    /// Schema: HashJoin::output_block_after_finalize
    Block result_block_for_other_condition;

    /// Metrics
    size_t probe_handle_rows = 0;
    size_t probe_time = 0;
    size_t probe_hash_table_time = 0;
    size_t replicate_time = 0;
    size_t other_condition_time = 0;
    size_t collision = 0;
};

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
    HashValueType hash = 0;
    size_t index = 0;
    union
    {
        RowPtr ptr = nullptr;
        std::atomic<RowPtr> * pointer_ptr;
    };
    KeyType key{};
};

template <ASTTableJoin::Kind kind, bool has_other_condition, bool late_materialization>
struct ProbeAdder;

#define JOIN_PROBE_TEMPLATE        \
    template <                     \
        typename KeyGetter,        \
        ASTTableJoin::Kind kind,   \
        bool has_null_map,         \
        bool has_other_condition,  \
        bool late_materialization, \
        bool tagged_pointer>

class HashJoin;
class JoinProbeBlockHelper
{
public:
    JoinProbeBlockHelper(const HashJoin * join, bool late_materialization);

    Block probe(JoinProbeContext & context, JoinProbeWorkerData & wd);

private:
    JOIN_PROBE_TEMPLATE
    Block probeImpl(JoinProbeContext & context, JoinProbeWorkerData & wd);

    JOIN_PROBE_TEMPLATE
    void NO_INLINE
    probeFillColumns(JoinProbeContext & context, JoinProbeWorkerData & wd, MutableColumns & added_columns);
    JOIN_PROBE_TEMPLATE
    void NO_INLINE
    probeFillColumnsPrefetch(JoinProbeContext & context, JoinProbeWorkerData & wd, MutableColumns & added_columns);

    template <typename KeyGetter>
    void ALWAYS_INLINE initPrefetchStates(JoinProbeContext & context)
    {
        if (!context.prefetch_states)
        {
            context.prefetch_states = decltype(context.prefetch_states)(
                static_cast<void *>(new ProbePrefetchState<KeyGetter>[settings.probe_prefetch_step]),
                [](void * ptr) { delete[] static_cast<ProbePrefetchState<KeyGetter> *>(ptr); });
        }
    }

    template <typename KeyGetterType, typename KeyType, typename HashValueType>
    bool ALWAYS_INLINE joinKeyIsEqual(
        KeyGetterType & key_getter,
        const KeyType & key1,
        const KeyType & key2,
        HashValueType hash1,
        RowPtr row_ptr) const
    {
        if constexpr (KeyGetterType::joinKeyCompareHashFirst())
        {
            auto hash2 = unalignedLoad<HashValueType>(row_ptr + sizeof(RowPtr));
            if (hash1 != hash2)
                return false;
        }
        return key_getter.joinKeyIsEqual(key1, key2);
    }

    template <bool late_materialization>
    void ALWAYS_INLINE insertRowToBatch(JoinProbeWorkerData & wd, MutableColumns & added_columns, RowPtr row_ptr) const
    {
        wd.insert_batch.push_back(row_ptr);
        flushBatchIfNecessary<late_materialization, false>(wd, added_columns);
    }

    template <bool late_materialization, bool force>
    void ALWAYS_INLINE flushBatchIfNecessary(JoinProbeWorkerData & wd, MutableColumns & added_columns) const
    {
        if constexpr (!force)
        {
            if likely (wd.insert_batch.size() < settings.probe_insert_batch_size)
                return;
        }
        if constexpr (late_materialization)
        {
            size_t idx = 0;
            for (auto [_, is_nullable] : row_layout.raw_key_column_indexes)
            {
                IColumn * column = added_columns[idx].get();
                if (is_nullable)
                    column = &static_cast<ColumnNullable &>(*added_columns[idx]).getNestedColumn();
                column->deserializeAndInsertFromPos(wd.insert_batch, true);
                ++idx;
            }
            for (size_t i = 0; i < row_layout.other_column_count_for_other_condition; ++i)
                added_columns[idx++]->deserializeAndInsertFromPos(wd.insert_batch, true);

            wd.row_ptrs_for_lm.insert(wd.insert_batch.begin(), wd.insert_batch.end());
        }
        else
        {
            for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
            {
                IColumn * column = added_columns[column_index].get();
                if (is_nullable)
                    column = &static_cast<ColumnNullable &>(*added_columns[column_index]).getNestedColumn();
                column->deserializeAndInsertFromPos(wd.insert_batch, true);
            }
            for (auto [column_index, _] : row_layout.other_column_indexes)
                added_columns[column_index]->deserializeAndInsertFromPos(wd.insert_batch, true);
        }

        if constexpr (force)
        {
            if constexpr (late_materialization)
            {
                size_t idx = 0;
                for (auto [_, is_nullable] : row_layout.raw_key_column_indexes)
                {
                    IColumn * column = added_columns[idx].get();
                    if (is_nullable)
                        column = &static_cast<ColumnNullable &>(*added_columns[idx]).getNestedColumn();
                    column->flushNTAlignBuffer();
                    ++idx;
                }
                for (size_t i = 0; i < row_layout.other_column_count_for_other_condition; ++i)
                    added_columns[idx++]->flushNTAlignBuffer();
            }
            else
            {
                for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
                {
                    IColumn * column = added_columns[column_index].get();
                    if (is_nullable)
                        column = &static_cast<ColumnNullable &>(*added_columns[column_index]).getNestedColumn();
                    column->flushNTAlignBuffer();
                }
                for (auto [column_index, _] : row_layout.other_column_indexes)
                    added_columns[column_index]->flushNTAlignBuffer();
            }
        }

        wd.insert_batch.clear();
    }

    template <bool late_materialization>
    void ALWAYS_INLINE fillNullMapWithZero(MutableColumns & added_columns) const
    {
        size_t idx = 0;
        for (auto [column_index, is_nullable] : row_layout.raw_key_column_indexes)
        {
            if (is_nullable)
            {
                size_t index;
                if constexpr (late_materialization)
                    index = idx;
                else
                    index = column_index;
                auto & nullable_column = static_cast<ColumnNullable &>(*added_columns[index]);
                size_t data_size = nullable_column.getNestedColumn().size();
                size_t nullmap_size = nullable_column.getNullMapColumn().size();
                RUNTIME_CHECK(nullmap_size <= data_size);
                nullable_column.getNullMapColumn().getData().resize_fill_zero(data_size);
            }
            ++idx;
        }
    }

    Block handleOtherConditions(
        JoinProbeContext & context,
        JoinProbeWorkerData & wd,
        ASTTableJoin::Kind kind,
        bool late_materialization);

    Block fillNotMatchedRowsForLeftOuter(JoinProbeContext & context, JoinProbeWorkerData & wd);

private:
    template <ASTTableJoin::Kind kind, bool has_other_condition, bool late_materialization>
    friend struct ProbeAdder;

    using FuncType = Block (JoinProbeBlockHelper::*)(JoinProbeContext &, JoinProbeWorkerData &);
    FuncType func_ptr_has_null = nullptr;
    FuncType func_ptr_no_null = nullptr;
    const HashJoin * join;
    const HashJoinSettings & settings;
    const HashJoinPointerTable & pointer_table;
    const HashJoinRowLayout & row_layout;
};

} // namespace DB
