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
#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{

class ProjectBinder : public ExecutorBinder
{
public:
    ProjectBinder(size_t & index_, const DAGSchema & output_schema_, ASTs && exprs_)
        : ExecutorBinder(index_, "project_" + std::to_string(index_), output_schema_)
        , exprs(std::move(exprs_))
    {}

    bool toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;

    void columnPrune(std::unordered_set<String> & used_columns) override;

private:
    std::vector<ASTPtr> exprs;
};

ExecutorBinderPtr compileProject(ExecutorBinderPtr input, size_t & executor_index, ASTPtr select_list);
} // namespace DB::mock
