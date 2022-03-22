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

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Merges several sorted streams into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps row with max `version` value.
  */
class ReplacingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingSortedBlockInputStream(
        const BlockInputStreams & inputs_, const SortDescription & description_,
        const String & version_column, size_t max_block_size_, WriteBuffer * out_row_sources_buf_ = nullptr)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_, 0, out_row_sources_buf_)
    {
        if (!version_column.empty())
            version_column_number = header.getPositionByName(version_column);
    }

    String getName() const override { return "ReplacingSorted"; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    ssize_t version_column_number = -1;

    Poco::Logger * log = &Poco::Logger::get("ReplacingSortedBlockInputStream");

    /// All data has been read.
    bool finished = false;

    /// Primary key of current row.
    RowRef current_key;
    /// Primary key of next row.
    RowRef next_key;
    /// Last row with maximum version for current primary key.
    RowRef selected_row;
    /// The position (into current_row_sources) of the row with the highest version.
    size_t max_pos = 0;

    /// Sources of rows with the current primary key.
    PODArray<RowSourcePart> current_row_sources;

    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /// Output into result the rows for current primary key.
    void insertRow(MutableColumns & merged_columns, size_t & merged_rows);
};

}
