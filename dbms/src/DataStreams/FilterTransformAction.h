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

#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

using FilterPtr = IColumn::Filter *;

struct FilterTransformAction
{
public:
    FilterTransformAction(
        const Block & header_,
        const ExpressionActionsPtr & expression_,
        const String & filter_column_name_);

    bool alwaysFalse() const;
    // return false if all filter out.
    // When return_filter is true, res_filter will be set to the filter column.
    // Always return true, and when filter conditions are always true, set res_filter = nullptr.
    bool transform(Block & block, FilterPtr & res_filter, bool return_filter);
    Block getHeader() const;
    ExpressionActionsPtr getExperssion() const;

private:
    Block header;
    ExpressionActionsPtr expression;
    size_t filter_column;

    ConstantFilterDescription constant_filter_description;
    IColumn::Filter * filter = nullptr;
    ColumnPtr filter_holder;
};

} // namespace DB
