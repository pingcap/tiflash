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
#include <Debug/MockExecutor/SortBinder.h>
#include <Parsers/ASTOrderByElement.h>

namespace DB::mock
{
bool SortBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeSort);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);
    tipb::Sort * sort = tipb_executor->mutable_sort();
    sort->set_ispartialsort(is_partial_sort);

    for (const auto & child : by_exprs)
    {
        auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = sort->add_byitems();
        by->set_desc(elem->direction < 0);
        tipb::Expr * expr = by->mutable_expr();
        astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
    }

    auto * children_executor = sort->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}

ExecutorBinderPtr compileSort(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr order_by_expr_list,
    bool is_partial_sort,
    uint64_t fine_grained_shuffle_stream_count)
{
    std::vector<ASTPtr> order_columns;
    if (order_by_expr_list != nullptr)
    {
        for (const auto & child : order_by_expr_list->children)
        {
            auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
            if (!elem)
                throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
            order_columns.push_back(child);
            compileExpr(input->output_schema, elem->children[0]);
        }
    }
    ExecutorBinderPtr sort = std::make_shared<mock::SortBinder>(
        executor_index,
        input->output_schema,
        std::move(order_columns),
        is_partial_sort,
        fine_grained_shuffle_stream_count);
    sort->children.push_back(input);
    return sort;
}
} // namespace DB::mock
