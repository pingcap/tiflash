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

#include <common/logger_useful.h>

#include <Core/Row.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>


namespace DB
{

/** Merges several sorted streams to one.
  * During this for each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  * merges them into one row. When merging, the data is pre-aggregated - merge of states of aggregate functions,
  * corresponding to a one value of the primary key. For columns that are not part of the primary key and which do not have the AggregateFunction type,
  * when merged, the first value is selected.
  */
class AggregatingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    AggregatingSortedBlockInputStream(
        const BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_);

    String getName() const override { return "AggregatingSorted"; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    Poco::Logger * log = &Poco::Logger::get("AggregatingSortedBlockInputStream");

    /// Read finished.
    bool finished = false;

    /// Columns with which numbers should be aggregated.
    ColumnNumbers column_numbers_to_aggregate;
    ColumnNumbers column_numbers_not_to_aggregate;
    std::vector<ColumnAggregateFunction *> columns_to_aggregate;

    RowRef current_key;        /// The current primary key.
    RowRef next_key;           /// The primary key of the next row.

    /** We support two different cursors - with Collation and without.
     *  Templates are used instead of polymorphic SortCursor and calls to virtual functions.
     */
    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /** Extract all states of aggregate functions and merge them with the current group.
      */
    void addRow(SortCursor & cursor);
};

}
