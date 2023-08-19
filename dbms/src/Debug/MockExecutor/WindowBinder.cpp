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
#include <Debug/MockExecutor/FuncSigMap.h>
#include <Debug/MockExecutor/WindowBinder.h>
#include <Parsers/ASTFunction.h>

namespace DB::mock
{
using ASTPartitionByElement = ASTOrderByElement;

bool WindowBinder::toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeWindow);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);
    tipb::Window * window = tipb_executor->mutable_window();
    auto & input_schema = children[0]->output_schema;
    for (const auto & expr : func_descs)
    {
        tipb::Expr * window_expr = window->add_func_desc();
        const auto * window_func = typeid_cast<const ASTFunction *>(expr.get());
        for (const auto & arg : window_func->arguments->children)
        {
            tipb::Expr * func = window_expr->add_children();
            astToPB(input_schema, arg, func, collator_id, context);
        }
        auto window_sig_it = tests::window_func_name_to_sig.find(window_func->name);
        if (window_sig_it == tests::window_func_name_to_sig.end())
            throw Exception(fmt::format("Unsupported window function {}", window_func->name), ErrorCodes::LOGICAL_ERROR);
        auto window_sig = window_sig_it->second;
        window_expr->set_tp(window_sig);
        auto * ft = window_expr->mutable_field_type();
        switch (window_sig)
        {
        case tipb::ExprType::Lead:
        case tipb::ExprType::Lag:
        {
            // TODO handling complex situations
            // like lead(col, offset, NULL), lead(data_type1, offset, data_type2)
            assert(window_expr->children_size() >= 1 && window_expr->children_size() <= 3);
            const auto first_arg_type = window_expr->children(0).field_type();
            ft->set_tp(first_arg_type.tp());
            if (window_expr->children_size() < 3)
            {
                auto field_type = TiDB::fieldTypeToColumnInfo(first_arg_type);
                field_type.clearNotNullFlag();
                ft->set_flag(field_type.flag);
            }
            else
            {
                const auto third_arg_type = window_expr->children(2).field_type();
                assert(first_arg_type.tp() == third_arg_type.tp());
                ft->set_flag(TiDB::fieldTypeToColumnInfo(first_arg_type).hasNotNullFlag()
                                 ? third_arg_type.flag()
                                 : first_arg_type.flag());
            }
            ft->set_collate(first_arg_type.collate());
            ft->set_flen(first_arg_type.flen());
            ft->set_decimal(first_arg_type.decimal());
            break;
        }
        default:
            ft->set_tp(TiDB::TypeLongLong);
            ft->set_flag(TiDB::ColumnFlagBinary);
            ft->set_collate(collator_id);
            ft->set_flen(21);
            ft->set_decimal(-1);
        }
    }

    for (const auto & child : order_by_exprs)
    {
        auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = window->add_order_by();
        by->set_desc(elem->direction < 0);
        tipb::Expr * expr = by->mutable_expr();
        astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
    }

    for (const auto & child : partition_by_exprs)
    {
        auto * elem = typeid_cast<ASTPartitionByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid partition by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = window->add_partition_by();
        by->set_desc(elem->direction < 0);
        tipb::Expr * expr = by->mutable_expr();
        astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
    }

    if (frame.type.has_value())
    {
        tipb::WindowFrame * mut_frame = window->mutable_frame();
        mut_frame->set_type(frame.type.value());
        if (frame.start.has_value())
        {
            auto * start = mut_frame->mutable_start();
            start->set_offset(std::get<2>(frame.start.value()));
            start->set_unbounded(std::get<1>(frame.start.value()));
            start->set_type(std::get<0>(frame.start.value()));
        }

        if (frame.end.has_value())
        {
            auto * end = mut_frame->mutable_end();
            end->set_offset(std::get<2>(frame.end.value()));
            end->set_unbounded(std::get<1>(frame.end.value()));
            end->set_type(std::get<0>(frame.end.value()));
        }
    }

    auto * children_executor = window->mutable_child();
    return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
}

ExecutorBinderPtr compileWindow(ExecutorBinderPtr input, size_t & executor_index, ASTPtr func_desc_list, ASTPtr partition_by_expr_list, ASTPtr order_by_expr_list, mock::MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count)
{
    std::vector<ASTPtr> partition_columns;
    if (partition_by_expr_list != nullptr)
    {
        for (const auto & child : partition_by_expr_list->children)
        {
            auto * elem = typeid_cast<ASTPartitionByElement *>(child.get());
            if (!elem)
                throw Exception("Invalid partition by element", ErrorCodes::LOGICAL_ERROR);
            partition_columns.push_back(child);
            compileExpr(input->output_schema, elem->children[0]);
        }
    }

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

    DAGSchema output_schema;
    output_schema.insert(output_schema.end(), input->output_schema.begin(), input->output_schema.end());

    std::vector<ASTPtr> window_exprs;
    if (func_desc_list != nullptr)
    {
        for (const auto & expr : func_desc_list->children)
        {
            const auto * func = typeid_cast<const ASTFunction *>(expr.get());
            window_exprs.push_back(expr);
            std::vector<TiDB::ColumnInfo> children_ci;
            for (const auto & arg : func->arguments->children)
            {
                children_ci.push_back(compileExpr(input->output_schema, arg));
            }
            // TODO: add more window functions
            TiDB::ColumnInfo ci;
            switch (tests::window_func_name_to_sig[func->name])
            {
            case tipb::ExprType::RowNumber:
            case tipb::ExprType::Rank:
            case tipb::ExprType::DenseRank:
            {
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagBinary;
                break;
            }
            case tipb::ExprType::Lead:
            case tipb::ExprType::Lag:
            {
                // TODO handling complex situations
                // like lead(col, offset, NULL), lead(data_type1, offset, data_type2)
                assert(!children_ci.empty() && children_ci.size() <= 3);
                if (children_ci.size() < 3)
                {
                    ci = children_ci[0];
                    ci.clearNotNullFlag();
                }
                else
                {
                    assert(children_ci[0].tp == children_ci[2].tp);
                    ci = children_ci[0].hasNotNullFlag() ? children_ci[2] : children_ci[0];
                }
                break;
            }
            default:
                throw Exception(fmt::format("Unsupported window function {}", func->name), ErrorCodes::LOGICAL_ERROR);
            }
            output_schema.emplace_back(std::make_pair(func->getColumnName(), ci));
        }
    }

    ExecutorBinderPtr window = std::make_shared<WindowBinder>(
        executor_index,
        output_schema,
        std::move(window_exprs),
        std::move(partition_columns),
        std::move(order_columns),
        frame,
        fine_grained_shuffle_stream_count);
    window->children.push_back(input);
    return window;
}
} // namespace DB::mock
