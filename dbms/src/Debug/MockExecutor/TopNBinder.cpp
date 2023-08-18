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
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/TopNBinder.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>

namespace DB::mock
{
bool TopNBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeTopN);
    tipb_executor->set_executor_id(name);
    tipb::TopN * topn = tipb_executor->mutable_topn();
    for (const auto & child : order_columns)
    {
        auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = topn->add_order_by();
        by->set_desc(elem->direction < 0);
        tipb::Expr * expr = by->mutable_expr();
        astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
    }
    topn->set_limit(limit);
    auto * child_executor = topn->mutable_child();
    return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
}

void TopNBinder::columnPrune(std::unordered_set<String> & used_columns)
{
    for (auto & expr : order_columns)
        collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
    children[0]->columnPrune(used_columns);
    /// update output schema after column prune
    output_schema = children[0]->output_schema;
}

ExecutorBinderPtr compileTopN(ExecutorBinderPtr input, size_t & executor_index, ASTPtr order_exprs, ASTPtr limit_expr)
{
    std::vector<ASTPtr> order_columns;
    for (const auto & child : order_exprs->children)
    {
        auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        order_columns.push_back(child);
        compileExpr(input->output_schema, elem->children[0]);
    }
    auto limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto top_n
        = std::make_shared<mock::TopNBinder>(executor_index, input->output_schema, std::move(order_columns), limit);
    top_n->children.push_back(input);
    return top_n;
}
} // namespace DB::mock
