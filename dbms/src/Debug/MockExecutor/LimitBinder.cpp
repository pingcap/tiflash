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

#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/LimitBinder.h>
#include <Parsers/ASTLiteral.h>

namespace DB::mock
{
bool LimitBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeLimit);
    tipb_executor->set_executor_id(name);
    tipb::Limit * lt = tipb_executor->mutable_limit();
    lt->set_limit(limit);
    auto * child_executor = lt->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

void LimitBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

ExecutorBinderPtr compileLimit(ExecutorBinderPtr input, size_t & executor_index, ASTPtr limit_expr)
{
    auto limit_length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto limit = std::make_shared<mock::LimitBinder>(executor_index, input->output_schema, limit_length);
    limit->children.push_back(input);
    return limit;
}
} // namespace DB::mock
