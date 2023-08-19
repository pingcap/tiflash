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

#include <Core/NamesAndTypes.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQuerySource.h>

#include <unordered_map>

namespace DB
{
// DAGQueryInfo contains filter information in dag request, it will
// be used to extracted key conditions by storage engine
struct DAGQueryInfo
{
    DAGQueryInfo(
        const std::vector<const tipb::Expr *> & filters_,
        DAGPreparedSets dag_sets_,
        const NamesAndTypes & source_columns_,
        const TimezoneInfo & timezone_info_)
        : filters(filters_)
        , dag_sets(std::move(dag_sets_))
        , source_columns(source_columns_)
        , timezone_info(timezone_info_){};
    // filters in dag request
    const std::vector<const tipb::Expr *> & filters;
    // Prepared sets extracted from dag request, which are used for indices
    // by storage engine.
    DAGPreparedSets dag_sets;
    const NamesAndTypes & source_columns;

    const TimezoneInfo & timezone_info;
};
} // namespace DB
