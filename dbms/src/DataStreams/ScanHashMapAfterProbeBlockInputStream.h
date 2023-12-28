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

#include <Core/ColumnNumbers.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Join.h>

namespace DB
{
/// Stream from scanning the hash table after probe
class ScanHashMapAfterProbeBlockInputStream : public IProfilingBlockInputStream
{
public:
    ScanHashMapAfterProbeBlockInputStream(
        const Join & parent_,
        const Block & left_sample_block,
        size_t index_,
        size_t step_,
        size_t max_block_size_);

    String getName() const override { return "ScanHashMapAfterProbe"; }

    Block getHeader() const override { return projected_sample_block; };

    size_t getIndex() const { return index; }


protected:
    Block readImpl() override;

private:
    const Join & parent;
    size_t index;
    size_t step;
    size_t max_block_size;
    size_t current_partition_index;

    bool not_mapped_row_pos_inited = false;
    /// point to the next row that to be added for rows that is not added to the map during build stage
    RowRefList * not_mapped_row_pos = nullptr;

    bool pos_in_hashmap_inited = false;
    /// point to the next row that to be checked in the hash map
    std::unique_ptr<void, std::function<void(void *)>> pos_in_hashmap = nullptr; /// type erasure
    const void * next_element_in_row_list = nullptr;


    Block result_sample_block;
    Block projected_sample_block; /// same schema with join's final schema
    /// Indices of columns in left sample block
    ColumnNumbers column_indices_left;
    /// Indices of columns in right sample block
    /// Order is significant: it is the same as the order of columns in the blocks of the right-side table that are saved in parent.blocks.
    ColumnNumbers column_indices_right;
    /// Columns of the current output block corresponding to column_indices_left.
    MutableColumns columns_left;
    /// Columns of the current output block corresponding to column_indices_right.
    MutableColumns columns_right;

    void advancedToNextPartition()
    {
        current_partition_index += step;
        pos_in_hashmap_inited = false;
        pos_in_hashmap = nullptr;
        not_mapped_row_pos_inited = false;
        not_mapped_row_pos = nullptr;
    }

    template <ASTTableJoin::Strictness STRICTNESS, bool row_flagged, bool output_joined_rows, typename Map>
    void fillColumns(
        const Map & map,
        MutableColumns & mutable_columns_left,
        MutableColumns & mutable_columns_right,
        IColumn * row_counter_column);

    template <bool row_flagged, bool output_joined_rows>
    void fillColumnsUsingCurrentPartition(
        MutableColumns & mutable_columns_left,
        MutableColumns & mutable_columns_right,
        IColumn * row_counter_column);
};
} // namespace DB