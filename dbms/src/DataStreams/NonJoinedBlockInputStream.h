// Copyright 2022 PingCAP, Ltd.
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
/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : public IProfilingBlockInputStream
{
public:
    NonJoinedBlockInputStream(const Join & parent_, const Block & left_sample_block, size_t index_, size_t step_, size_t max_block_size_);

    String getName() const override { return "NonJoined"; }

    Block getHeader() const override { return result_sample_block; };

    size_t getNonJoinedIndex() const { return index; }


protected:
    Block readImpl() override;

private:
    const Join & parent;
    size_t index;
    size_t step;
    size_t max_block_size;
    bool add_not_mapped_rows;
    size_t next_index;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that come from the left-side table (except key columns).
    ColumnNumbers column_indices_left;
    /// Indices of columns that come from the right-side table.
    /// Order is significant: it is the same as the order of columns in the blocks of the right-side table that are saved in parent.blocks.
    ColumnNumbers column_indices_right;
    /// Columns of the current output block corresponding to column_indices_left.
    MutableColumns columns_left;
    /// Columns of the current output block corresponding to column_indices_right.
    MutableColumns columns_right;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure
    const void * next_element_in_row_list = nullptr;
    size_t current_segment = 0;
    Join::RowRefList * current_not_mapped_row = nullptr;

    void setNextCurrentNotMappedRow();

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Block createBlock(const Maps & maps);


    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map,
                       size_t num_columns_left,
                       MutableColumns & mutable_columns_left,
                       size_t num_columns_right,
                       MutableColumns & mutable_columns_right);
};
} // namespace DB