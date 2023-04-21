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

#include <Core/SortDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class PartialSortingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "PartialSorting";

public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingBlockInputStream(
        const BlockInputStreamPtr & input_,
        const SortDescription & description_,
        const String & req_id,
        size_t limit_ = 0)
        : description(description_)
        , limit(limit_)
        , log(Logger::get(req_id))
    {
        children.push_back(input_);

        assert(!description_.empty());
        for (const auto & column_sort_desc : description)
            description_with_positions.emplace_back(column_sort_desc, children.at(0)->getHeader().getPositionByName(column_sort_desc.column_name));
    }

    String getName() const override { return NAME; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;
    void appendInfo(FmtBuffer & buffer) const override;

private:
    SortDescription description;
    SortDescriptionWithPositions description_with_positions;
    size_t limit;
    LoggerPtr log;

    /// This are just buffers which reserve memory to reduce the number of allocations.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    IColumn::Filter filter;

    Columns sort_description_threshold_columns;

    static constexpr size_t min_limit_for_partial_sort_optimization = 1500; // 1500
};

} // namespace DB
