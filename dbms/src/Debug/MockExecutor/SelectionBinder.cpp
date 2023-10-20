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
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/SelectionBinder.h>

namespace DB::mock
{
bool SelectionBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeSelection);
    tipb_executor->set_executor_id(name);
    auto * sel = tipb_executor->mutable_selection();
    for (auto & expr : conditions)
    {
        tipb::Expr * cond = sel->add_conditions();
        astToPB(children[0]->output_schema, expr, cond, collator_id, context);
    }
    auto * child_executor = sel->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

void SelectionBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    for (auto & expr : conditions)
        collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

ExecutorBinderPtr compileSelection(ExecutorBinderPtr input, size_t & executor_index, ASTPtr filter)
{
    std::vector<ASTPtr> conditions;
    compileFilter(input->output_schema, filter, conditions);
    auto selection
        = std::make_shared<mock::SelectionBinder>(executor_index, input->output_schema, std::move(conditions));
    selection->children.push_back(input);
    return selection;
}
} // namespace DB::mock
