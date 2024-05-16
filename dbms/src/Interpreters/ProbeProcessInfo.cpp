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

#include <Columns/ColumnUtils.h>
#include <Columns/ColumnsCommon.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/ProbeProcessInfo.h>

namespace DB
{
void ProbeProcessInfo::resetBlock(Block && block_, size_t partition_index_)
{
    block = std::move(block_);
    partition_index = partition_index_;
    start_row = 0;
    end_row = 0;
    all_rows_joined_finish = false;
    // If the probe block size is greater than max_block_size, we will set max_block_size to the probe block size to avoid some unnecessary split.
    max_block_size = std::max(max_block_size, block.rows());
    // min_result_block_size is used to avoid generating too many small block, use 50% of the block size as the default value
    min_result_block_size = std::max(1, (std::min(block.rows(), max_block_size) + 1) / 2);
    prepare_for_probe_done = false;
    null_map = nullptr;
    null_map_holder = nullptr;
    filter.reset();
    offsets_to_replicate.reset();
    if (hash_join_data)
        hash_join_data->reset();
    if (cross_join_data)
        cross_join_data->reset();
    if (null_aware_join_data)
        null_aware_join_data->reset();
}

void ProbeProcessInfo::prepareForHashProbe(
    const Names & key_names,
    const String & filter_column,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    bool need_compute_hash,
    const TiDB::TiDBCollators & collators,
    size_t restore_round)
{
    if (prepare_for_probe_done)
        return;
    if (unlikely(hash_join_data == nullptr))
        hash_join_data = std::make_unique<HashJoinProbeProcessData>();
    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    hash_join_data->key_columns
        = extractAndMaterializeKeyColumns(block, hash_join_data->materialized_columns, key_names);
    /// Keys with NULL value in any column won't join to anything.
    extractNestedColumnsAndNullMap(hash_join_data->key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter won't join to anything
    recordFilteredRows(block, filter_column, null_map_holder, null_map);
    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (getFullness(kind))
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto & col = block.getByPosition(i).column;
            if (ColumnPtr converted = col->convertToFullColumnIfConst())
                col = converted;
            convertColumnToNullable(block.getByPosition(i));
        }
    }

    if (!isSemiFamily(kind) && !isLeftOuterSemiFamily(kind) && strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(block.rows());

    hash_join_data->hash_data = std::make_unique<WeakHash32>(0);
    if (need_compute_hash)
    {
        std::vector<std::string> sort_key_containers;
        sort_key_containers.resize(hash_join_data->key_columns.size());
        computeDispatchHash(
            block.rows(),
            hash_join_data->key_columns,
            collators,
            sort_key_containers,
            restore_round,
            *hash_join_data->hash_data);
    }
    prepare_for_probe_done = true;
}

void ProbeProcessInfo::prepareForCrossProbe(
    const String & filter_column,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    const Block & right_sample_block,
    const NameSet & output_column_names_set,
    size_t right_rows_to_be_added_when_matched_,
    CrossProbeMode cross_probe_mode_,
    size_t right_block_size_)
{
    if (prepare_for_probe_done)
        return;
    if (unlikely(cross_join_data == nullptr))
        cross_join_data = std::make_unique<CrossJoinProbeProcessData>();

    cross_join_data->right_rows_to_be_added_when_matched = right_rows_to_be_added_when_matched_;
    cross_join_data->cross_probe_mode = cross_probe_mode_;
    cross_join_data->right_block_size = right_block_size_;

    recordFilteredRows(block, filter_column, null_map_holder, null_map);
    if (kind == ASTTableJoin::Kind::Cross_Anti && strictness == ASTTableJoin::Strictness::All)
        /// `CrossJoinAdder<Cross_Anti, Any>` will skip the matched rows directly, so filter is not needed
        filter = std::make_unique<IColumn::Filter>(block.rows());
    if (strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(block.rows());

    /// Should convert all the columns in block to nullable if it is cross right join, here we don't need
    /// to do so because cross_right join is converted to cross left join during compile
    if unlikely (cross_join_data->result_block_schema.columns() == 0)
    {
        /// these information only need to be init once
        for (size_t i = 0; i < block.columns(); ++i)
        {
            auto & column = block.getByPosition(i);
            if (output_column_names_set.contains(column.name))
            {
                cross_join_data->result_block_schema.insert(column.cloneEmpty());
                cross_join_data->left_column_index_in_left_block.push_back(i);
            }
        }
        for (size_t i = 0; i < right_sample_block.columns(); ++i)
        {
            const ColumnWithTypeAndName & src_column = right_sample_block.getByPosition(i);
            if (output_column_names_set.contains(src_column.name))
            {
                RUNTIME_CHECK_MSG(
                    !cross_join_data->result_block_schema.has(src_column.name),
                    "block from probe side has a column with the same name: {} as a column in "
                    "right_sample_block",
                    src_column.name);
                cross_join_data->result_block_schema.insert(src_column);
                cross_join_data->right_column_index_in_right_block.push_back(i);
            }
        }
    }
    if (cross_join_data->cross_probe_mode == CrossProbeMode::SHALLOW_COPY_RIGHT_BLOCK && null_map != nullptr)
        cross_join_data->row_num_filtered_by_left_condition = countBytesInFilter(*null_map);
    prepare_for_probe_done = true;
}

void ProbeProcessInfo::prepareForNullAware(const Names & key_names, const String & filter_column)
{
    assert(prepare_for_probe_done == false);
    if unlikely (null_aware_join_data == nullptr)
        null_aware_join_data = std::make_unique<NullAwareJoinProbeProcessData>();
    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    null_aware_join_data->key_columns
        = extractAndMaterializeKeyColumns(block, null_aware_join_data->materialized_columns, key_names);

    /// Note that `extractAllKeyNullMap` must be done before `extractNestedColumnsAndNullMap`
    /// because `extractNestedColumnsAndNullMap` will change the nullable column to its nested column.
    extractAllKeyNullMap(
        null_aware_join_data->key_columns,
        null_aware_join_data->all_key_null_map_holder,
        null_aware_join_data->all_key_null_map);

    extractNestedColumnsAndNullMap(null_aware_join_data->key_columns, null_map_holder, null_map);

    recordFilteredRows(block, filter_column, null_aware_join_data->filter_map_holder, null_aware_join_data->filter_map);
    prepare_for_probe_done = true;
}

void ProbeProcessInfo::cutFilterAndOffsetVector(size_t start, size_t end) const
{
    if (filter != nullptr)
        filter->assignFromSelf(start, end);
    if (offsets_to_replicate != nullptr)
        offsets_to_replicate->assignFromSelf(start, end);
}

bool ProbeProcessInfo::isCurrentProbeRowFinished() const
{
    /// only used in cross join of shallow copy cross probe mode
    return cross_join_data->next_right_block_index == cross_join_data->right_block_size;
}

void ProbeProcessInfo::finishCurrentProbeRow() const
{
    /// only used in cross join of shallow copy cross probe mode
    cross_join_data->next_right_block_index = cross_join_data->right_block_size;
}

} // namespace DB
