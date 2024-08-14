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
#include <Core/Block.h>
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
    size_t rows;
    size_t start_row_idx = 0;
    size_t prefetch_active_states = 0;
    RowPtr current_probe_row_ptr = nullptr;

    bool is_prepared = false;
    Columns materialized_columns;
    ColumnRawPtrs key_columns;
    ColumnPtr null_map_holder = nullptr;
    ConstNullMapPtr null_map = nullptr;
    std::unique_ptr<void, std::function<void(void *)>> key_getter;

    bool current_row_is_matched = false;

    bool isCurrentProbeFinished() const;
    void resetBlock(Block & block_);

    void prepareForHashProbe(
        HashJoinKeyMethod method,
        ASTTableJoin::Kind kind,
        const Names & key_names,
        const String & filter_column,
        const NameSet & probe_output_name_set,
        const TiDB::TiDBCollators & collators);
};

struct alignas(ABSL_CACHELINE_SIZE) JoinProbeWorkerData
{
    size_t prefetch_iter = 0;
    std::unique_ptr<void, std::function<void(void *)>> prefetch_states;

    IColumn::Offsets selective_offsets;
    IColumn::Offsets offsets_to_replicate;

    RowPtrs insert_batch;
    RowPtrs insert_batch_other;

    size_t probe_handle_rows = 0;
    size_t probe_time = 0;
    size_t probe_hash_table_time = 0;
    size_t replicate_time = 0;
    size_t other_condition_time = 0;
    size_t collision = 0;

    ColumnUInt8::MutablePtr filter_column = ColumnUInt8::create();

    std::vector<AlignBufferAVX2> align_buffer;
};

void joinProbeBlock(
    JoinProbeContext & context,
    JoinProbeWorkerData & wd,
    HashJoinKeyMethod method,
    ASTTableJoin::Kind kind,
    const JoinNonEqualConditions & non_equal_conditions,
    const HashJoinSettings & settings,
    const HashJoinPointerTable & pointer_table,
    const HashJoinRowLayout & row_layout,
    MutableColumns & added_columns);


} // namespace DB
