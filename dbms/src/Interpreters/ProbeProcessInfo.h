// Copyright 2023 PingCAP, Inc.
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
#include <Parsers/ASTTablesInSelectQuery.h>

#include <memory>

namespace DB
{
enum class CrossProbeMode
{
    /// DEEP_COPY_RIGHT_BLOCK construct the probe block using CrossJoinAdder, the right rows will be added to probe block row by row, used when right side has few rows
    DEEP_COPY_RIGHT_BLOCK,
    /// SHALLOW_COPY_RIGHT_BLOCK construct the probe block without copy right block, the left row is appended to the right block, used when right side has many rows
    /// Note for the rows that is filtered by left condition, it still use CrossJoinAdder to construct the probe block
    SHALLOW_COPY_RIGHT_BLOCK,
};

struct HashJoinProbeProcessData
{
    Columns materialized_columns;
    ColumnRawPtrs key_columns;
    /// TODO: consider adding a virtual column in Sender side to avoid computing cost and potential inconsistency by heterogeneous envs(AMD64, ARM64)
    /// Note: 1. Not sure, if inconsistency will do happen in heterogeneous envs
    ///       2. Virtual column would take up a little more network bandwidth, might lead to poor performance if network was bottleneck
    /// Currently, the computation cost is tolerable, since it's a very simple crc32 hash algorithm, and heterogeneous envs support is not considered
    std::unique_ptr<WeakHash32> hash_data; /// to reproduce hash values according to build stage
    void reset()
    {
        key_columns.clear();
        materialized_columns.clear();
        hash_data = nullptr;
    }
};
struct CrossJoinProbeProcessData
{
    Block result_block_schema;
    std::vector<size_t> right_column_index_in_right_block;
    std::vector<size_t> left_column_index_in_left_block;
    size_t right_rows_to_be_added_when_matched = 0;
    CrossProbeMode cross_probe_mode = CrossProbeMode::DEEP_COPY_RIGHT_BLOCK;
    /// the following fields are used for NO_COPY_RIGHT_BLOCK probe
    size_t right_block_size = 0;
    /// the rows that is filtered by left condition
    size_t row_num_filtered_by_left_condition = 0;
    size_t next_right_block_index = 0;
    /// used for outer/semi/anti/left outer semi/left outer anti join
    bool has_row_matched = false;
    /// used for left outer semi/left outer anti join
    bool has_row_null = false;
    void reset()
    {
        right_rows_to_be_added_when_matched = 0;
        cross_probe_mode = CrossProbeMode::DEEP_COPY_RIGHT_BLOCK;
        right_block_size = 0;
        next_right_block_index = 0;
        row_num_filtered_by_left_condition = 0;
        has_row_matched = false;
        has_row_null = false;
    }
};
struct NullAwareJoinProbeProcessData
{
    Columns materialized_columns;
    ColumnRawPtrs key_columns;
    ColumnPtr filter_map_holder = nullptr;
    ConstNullMapPtr filter_map = nullptr;
    ColumnPtr all_key_null_map_holder = nullptr;
    ConstNullMapPtr all_key_null_map = nullptr;
    void reset()
    {
        key_columns.clear();
        materialized_columns.clear();
        filter_map_holder = nullptr;
        filter_map = nullptr;
        all_key_null_map_holder = nullptr;
        all_key_null_map = nullptr;
    }
};

struct ProbeProcessInfo
{
    Block block;
    size_t partition_index;
    UInt64 max_block_size;
    UInt64 min_result_block_size;
    size_t start_row;
    size_t end_row;
    bool all_rows_joined_finish;
    UInt64 cache_columns_threshold;

    /// these should be inited before probe each block
    bool prepare_for_probe_done = false;
    ColumnPtr null_map_holder = nullptr;
    ConstNullMapPtr null_map = nullptr;
    /// Used with ANY INNER ANTI JOIN
    std::unique_ptr<IColumn::Filter> filter = nullptr;
    /// Used with ALL ... JOIN
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate = nullptr;

    /// for hash probe
    std::unique_ptr<HashJoinProbeProcessData> hash_join_data;

    /// for cross probe
    std::unique_ptr<CrossJoinProbeProcessData> cross_join_data;

    /// for null-aware join
    std::unique_ptr<NullAwareJoinProbeProcessData> null_aware_join_data;

    // for semi join
    std::unique_ptr<void, std::function<void(void *)>> semi_family_helper = nullptr; /// type erasure

    ProbeProcessInfo(UInt64 max_block_size_, UInt64 cache_columns_threshold_)
        : partition_index(0)
        , max_block_size(max_block_size_)
        , min_result_block_size((max_block_size + 1) / 2)
        , start_row(0)
        , end_row(0)
        , all_rows_joined_finish(true)
        , cache_columns_threshold(cache_columns_threshold_){};

    void resetBlock(Block && block_, size_t partition_index_ = 0);
    template <bool is_shallow_cross_probe_mode>
    void updateStartRow()
    {
        if constexpr (is_shallow_cross_probe_mode)
        {
            if (cross_join_data->next_right_block_index < cross_join_data->right_block_size)
                return;
            cross_join_data->next_right_block_index = 0;
            cross_join_data->has_row_matched = false;
            cross_join_data->has_row_null = false;
        }
        assert(start_row <= end_row);
        start_row = end_row;
        if (filter != nullptr)
            filter->resize(block.rows());
        if (offsets_to_replicate != nullptr)
            offsets_to_replicate->resize(block.rows());
    }

    template <bool is_shallow_cross_probe_mode>
    void updateEndRow(size_t next_row_to_probe)
    {
        end_row = next_row_to_probe;
        if constexpr (is_shallow_cross_probe_mode)
        {
            if (cross_join_data->next_right_block_index < cross_join_data->right_block_size)
            {
                /// current probe is not finished, just return
                return;
            }
            all_rows_joined_finish
                = cross_join_data->row_num_filtered_by_left_condition == 0 && end_row == block.rows();
        }
        else
        {
            all_rows_joined_finish = end_row == block.rows();
        }
    }

    void prepareForHashProbe(
        const Names & key_names,
        const String & filter_column,
        ASTTableJoin::Kind kind,
        ASTTableJoin::Strictness strictness,
        bool need_compute_hash,
        const TiDB::TiDBCollators & collators,
        size_t restore_round);
    void prepareForCrossProbe(
        const String & filter_column,
        ASTTableJoin::Kind kind,
        ASTTableJoin::Strictness strictness,
        const Block & right_sample_block,
        const NameSet & output_column_names_set,
        size_t right_rows_to_be_added_when_matched,
        CrossProbeMode cross_probe_mode,
        size_t right_block_size);
    void prepareForNullAware(const Names & key_names, const String & filter_column);

    void cutFilterAndOffsetVector(size_t start, size_t end) const;
    bool isCurrentProbeRowFinished() const;
    void finishCurrentProbeRow() const;
};
} // namespace DB
