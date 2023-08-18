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

#include <DataStreams/FilterTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
using FilterPtr = IColumn::Filter *;

/** Implements WHERE, HAVING operations.
  * A stream of blocks and an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions, are passed as input.
  * The expression is evaluated and a stream of blocks is returned, which contains only the filtered rows.
  */
class FilterBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Filter";

private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    FilterBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & expression_,
        const String & filter_column_name_,
        const String & req_id);

    String getName() const override { return NAME; }
    Block getHeader() const override;

protected:
    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    // Note: When return_filter is true, res_filter will be point to the filter column of the returned block.
    // If res_filter is nullptr, it means the filter conditions are always true.
    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

private:
    FilterTransformAction filter_transform_action;

    const LoggerPtr log;
};

} // namespace DB
