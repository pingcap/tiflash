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

#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{
using MockGroupingNameVec = std::vector<ASTPtr>;
using MockVecGroupingNameVec = std::vector<MockGroupingNameVec>;
using MockVVecGroupingNameVec = std::vector<MockVecGroupingNameVec>;

class ExpandBinder : public ExecutorBinder
{
public:
    ExpandBinder(size_t & index_, const DAGSchema & output_schema_, MockVVecGroupingNameVec gss)
        : ExecutorBinder(index_, "expand_" + std::to_string(index_), output_schema_)
        , grouping_sets_columns(gss)
    {}

    bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context) override;

    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }

private:
    // for now, every grouping set is base columns list, modify structure to be one more nested if grouping set merge is enabled.
    MockVVecGroupingNameVec grouping_sets_columns;
};

ExecutorBinderPtr compileExpand(
    ExecutorBinderPtr input,
    size_t & executor_index,
    MockVVecGroupingNameVec grouping_set_columns,
    std::set<String> set);
} // namespace DB::mock
