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
    key_columns.clear();
    materialized_columns.clear();
    result_block_schema.clear();
    right_column_index.clear();
    right_rows_to_be_added_when_matched = 0;
    cross_probe_mode = CrossProbeMode::DEEP_COPY_RIGHT_BLOCK;
    right_block_size = 0;
    next_right_block_index = 0;
    row_num_filtered_by_left_condition = 0;
    has_row_matched = false;
    has_row_null = false;
}

void ProbeProcessInfo::prepareForHashProbe(
    const Names & key_names,
    const String & filter_column,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness)
{
    if (prepare_for_probe_done)
        return;
    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);
    /// Keys with NULL value in any column won't join to anything.
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
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

            /// convert left columns (except keys) to Nullable
            if (std::end(key_names) == std::find(key_names.begin(), key_names.end(), block.getByPosition(i).name))
                convertColumnToNullable(block.getByPosition(i));
        }
    }
    if (((kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::RightOuter)
         && strictness == ASTTableJoin::Strictness::Any)
        || kind == ASTTableJoin::Kind::Anti)
        filter = std::make_unique<IColumn::Filter>(block.rows());
    if (strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(block.rows());
    prepare_for_probe_done = true;
}

void ProbeProcessInfo::prepareForCrossProbe(
    const String & filter_column,
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    const Block & sample_block_with_columns_to_add,
    size_t right_rows_to_be_added_when_matched_,
    CrossProbeMode cross_probe_mode_,
    size_t right_block_size_)
{
    if (prepare_for_probe_done)
        return;

    right_rows_to_be_added_when_matched = right_rows_to_be_added_when_matched_;
    cross_probe_mode = cross_probe_mode_;
    right_block_size = right_block_size_;

    recordFilteredRows(block, filter_column, null_map_holder, null_map);
    if (kind == ASTTableJoin::Kind::Cross_Anti && strictness == ASTTableJoin::Strictness::All)
        /// `CrossJoinAdder<Cross_Anti, Any>` will skip the matched rows directly, so filter is not needed
        filter = std::make_unique<IColumn::Filter>(block.rows());
    if (strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(block.rows());

    /// Should convert all the columns in block to nullable if it is cross right join, here we don't need
    /// to do so because cross_right join is converted to cross left join during compile
    result_block_schema = block.cloneEmpty();
    for (size_t i = 0; i < sample_block_with_columns_to_add.columns(); ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(
            !result_block_schema.has(src_column.name),
            "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add",
            src_column.name);
        result_block_schema.insert(src_column);
    }
    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
    for (size_t i = 0; i < num_columns_to_add; ++i)
        right_column_index.push_back(num_existing_columns + i);

    if (cross_probe_mode == CrossProbeMode::SHALLOW_COPY_RIGHT_BLOCK && null_map != nullptr)
        row_num_filtered_by_left_condition = countBytesInFilter(*null_map);
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
    return next_right_block_index == right_block_size;
}

void ProbeProcessInfo::finishCurrentProbeRow()
{
    /// only used in cross join of shallow copy cross probe mode
    next_right_block_index = right_block_size;
}

} // namespace DB
