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
#include <Interpreters/JoinV2/SemiJoinProbeList.h>
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
    size_t current_row_idx = 0;
    RowPtr current_build_row_ptr = nullptr;
    /// For left outer/(left outer) (anti) semi join without other conditions.
    bool current_row_is_matched = false;
    /// For left outer with other conditions.
    IColumn::Filter rows_not_matched;
    /// < 0 means not_matched_offsets is not initialized.
    ssize_t not_matched_offsets_idx = -1;
    IColumn::Offsets not_matched_offsets;
    /// For left outer (anti) semi join.
    PaddedPODArray<Int8> left_semi_match_res;
    /// For left outer (anti) semi join with other-eq-from-in conditions.
    PaddedPODArray<UInt8> left_semi_match_null_res;
    /// For (anti) semi join with other conditions.
    IColumn::Offsets semi_selective_offsets;
    /// For (left outer) (anti) semi join with other conditions.
    std::unique_ptr<SemiJoinProbeListBase> semi_join_probe_list;

    size_t prefetch_active_states = 0;
    size_t prefetch_iter = 0;
    std::unique_ptr<void, std::function<void(void *)>> prefetch_states;

    bool is_prepared = false;
    Columns materialized_key_columns;
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
        bool has_other_eq_from_in_condition,
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
    /// For left outer join without other conditions
    IColumn::Offsets not_matched_selective_offsets;

    RowPtrs insert_batch;

    /// For other condition
    ColumnVector<UInt8>::Container filter;
    IColumn::Offsets block_filter_offsets;
    IColumn::Offsets result_block_filter_offsets;
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

class JoinProbeHelperUtil
{
protected:
    JoinProbeHelperUtil(const HashJoinSettings & settings, const HashJoinRowLayout & row_layout)
        : settings(settings)
        , row_layout(row_layout)
    {}

    template <typename KeyGetterType, typename KeyType, typename HashValueType>
    static bool ALWAYS_INLINE joinKeyIsEqual(
        KeyGetterType & key_getter,
        const KeyType & key1,
        const KeyType & key2,
        HashValueType hash1,
        RowPtr row_ptr)
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
        if unlikely (wd.insert_batch.size() >= settings.probe_insert_batch_size)
            flushInsertBatch<late_materialization, false>(wd, added_columns);
    }

    template <bool late_materialization, bool last_flush>
    void flushInsertBatch(JoinProbeWorkerData & wd, MutableColumns & added_columns) const;

    template <bool late_materialization>
    void fillNullMapWithZero(MutableColumns & added_columns) const;

protected:
    const HashJoinSettings & settings;
    const HashJoinRowLayout & row_layout;
};

template <ASTTableJoin::Kind kind, bool has_other_condition, bool late_materialization>
struct JoinProbeAdder;

#define JOIN_PROBE_HELPER_TEMPLATE \
    template <                     \
        typename KeyGetter,        \
        ASTTableJoin::Kind kind,   \
        bool has_null_map,         \
        bool has_other_condition,  \
        bool late_materialization, \
        bool tagged_pointer>

class HashJoin;
class JoinProbeHelper : public JoinProbeHelperUtil
{
public:
    JoinProbeHelper(const HashJoin * join, bool late_materialization);

    Block probe(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

private:
    JOIN_PROBE_HELPER_TEMPLATE
    Block probeImpl(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

    JOIN_PROBE_HELPER_TEMPLATE
    void NO_INLINE probeFillColumns(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);
    JOIN_PROBE_HELPER_TEMPLATE
    void NO_INLINE
    probeFillColumnsPrefetch(JoinProbeContext & ctx, JoinProbeWorkerData & wd, MutableColumns & added_columns);

    Block handleOtherConditions(
        JoinProbeContext & ctx,
        JoinProbeWorkerData & wd,
        ASTTableJoin::Kind kind,
        bool late_materialization);

    Block fillNotMatchedRowsForLeftOuter(JoinProbeContext & ctx, JoinProbeWorkerData & wd);

    Block genResultBlockForLeftOuterSemi(JoinProbeContext & ctx);

private:
    template <ASTTableJoin::Kind kind, bool has_other_condition, bool late_materialization>
    friend struct JoinProbeAdder;

    using FuncType = Block (JoinProbeHelper::*)(JoinProbeContext &, JoinProbeWorkerData &);
    FuncType func_ptr_has_null = nullptr;
    FuncType func_ptr_no_null = nullptr;
    const HashJoin * join;
    const HashJoinPointerTable & pointer_table;
};

} // namespace DB
