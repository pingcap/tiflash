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
#include <tipb/expression.pb.h>


namespace DB::mock
{
namespace
{
// The following functions must return not nullable column:
//   Window: row_number, rank, dense_rank, cume_dist, percent_rank,
//           lead and lag(When receiving 3 parameters and the first and the third parameters
//                        are both not related with nullable)
//   Aggregation: count, count distinct, bit_and, bit_or, bit_xor
//
// Other window or aggregation functions always return nullable column and we need to
// remove the not null flag for them.
void setFieldTypeForWindowFunc(tipb::Expr * window_expr, const tipb::ExprType window_sig, const int32_t collator_id)
{
    window_expr->set_tp(window_sig);
    auto * window_field_type = window_expr->mutable_field_type();
    switch (window_sig)
    {
    case tipb::ExprType::Lead:
    case tipb::ExprType::Lag:
    {
        assert(window_expr->children_size() >= 1 && window_expr->children_size() <= 3);
        const auto first_arg_type = window_expr->children(0).field_type();
        window_field_type->set_tp(first_arg_type.tp());
        if (window_expr->children_size() < 3)
        {
            auto field_type = TiDB::fieldTypeToColumnInfo(first_arg_type);
            field_type.clearNotNullFlag();
            window_field_type->set_flag(field_type.flag);
        }
        else
        {
            const auto third_arg_type = window_expr->children(2).field_type();
            assert(first_arg_type.tp() == third_arg_type.tp());
            window_field_type->set_flag(
                TiDB::fieldTypeToColumnInfo(first_arg_type).hasNotNullFlag() ? third_arg_type.flag()
                                                                             : first_arg_type.flag());
        }
        window_field_type->set_collate(first_arg_type.collate());
        window_field_type->set_flen(first_arg_type.flen());
        window_field_type->set_decimal(first_arg_type.decimal());
        break;
    }
    case tipb::ExprType::FirstValue:
    case tipb::ExprType::LastValue:
    {
        assert(window_expr->children_size() == 1);
        const auto arg_type = window_expr->children(0).field_type();
        (*window_field_type) = arg_type;

        auto field_type = TiDB::fieldTypeToColumnInfo(arg_type);
        field_type.clearNotNullFlag();
        window_field_type->set_flag(field_type.flag);
        break;
    }
    default:
        window_field_type->set_tp(TiDB::TypeLongLong);
        window_field_type->set_flag(TiDB::ColumnFlagBinary);
        window_field_type->set_collate(collator_id);
        window_field_type->set_flen(21);
        window_field_type->set_decimal(-1);
    }
}

void setFieldTypeForAggFunc(
    const DB::ASTFunction * func,
    tipb::Expr * expr,
    const tipb::ExprType agg_sig,
    int32_t collator_id)
{
    expr->set_tp(agg_sig);
    if (agg_sig == tipb::ExprType::Count || agg_sig == tipb::ExprType::Sum)
    {
        auto * ft = expr->mutable_field_type();
        ft->set_tp(TiDB::TypeLongLong);
        ft->set_flag(TiDB::ColumnFlagNotNull);
    }
    else if (agg_sig == tipb::ExprType::Min || agg_sig == tipb::ExprType::Max)
    {
        if (expr->children_size() != 1)
            throw Exception(fmt::format("Agg function({}) only accept 1 argument", func->name));

        auto * ft = expr->mutable_field_type();
        ft->set_tp(expr->children(0).field_type().tp());
        ft->set_decimal(expr->children(0).field_type().decimal());
        ft->set_flag(expr->children(0).field_type().flag() & (~TiDB::ColumnFlagNotNull));
        ft->set_collate(collator_id);
    }
    else
    {
        throw Exception("Window does not support this agg function");
    }

    expr->set_aggfuncmode(tipb::AggFunctionMode::FinalMode);
}

void setFieldType(const DB::ASTFunction * func, tipb::Expr * expr, int32_t collator_id)
{
    auto window_sig_it = tests::window_func_name_to_sig.find(func->name);
    if (window_sig_it != tests::window_func_name_to_sig.end())
    {
        setFieldTypeForWindowFunc(expr, window_sig_it->second, collator_id);
        return;
    }

    auto agg_sig_it = tests::agg_func_name_to_sig.find(func->name);
    if (agg_sig_it == tests::agg_func_name_to_sig.end())
        throw Exception("Unsupported agg function: " + func->name, ErrorCodes::LOGICAL_ERROR);

    auto agg_sig = agg_sig_it->second;
    setFieldTypeForAggFunc(func, expr, agg_sig, collator_id);
}

void setWindowFrame(MockWindowFrame & frame, tipb::Window * window)
{
    if (frame.type.has_value())
    {
        tipb::WindowFrame * mut_frame = window->mutable_frame();
        mut_frame->set_type(frame.type.value());
        if (frame.start.has_value())
        {
            auto * start = mut_frame->mutable_start();
            start->set_offset(frame.start->getOffset());
            start->set_unbounded(frame.start->isUnbounded());
            start->set_type(frame.start->getBoundType());
            if (frame.start->isRangeFrame())
            {
                start->set_allocated_frame_range(frame.start->robRangeFrame());
                start->set_cmp_data_type(frame.start->getCmpDataType());
            }
        }

        if (frame.end.has_value())
        {
            auto * end = mut_frame->mutable_end();
            end->set_offset(frame.end->getOffset());
            end->set_unbounded(frame.end->isUnbounded());
            end->set_type(frame.end->getBoundType());
            if (frame.end->isRangeFrame())
            {
                end->set_allocated_frame_range(frame.end->robRangeFrame());
                end->set_cmp_data_type(frame.end->getCmpDataType());
            }
        }
    }
}
} // namespace

using ASTPartitionByElement = ASTOrderByElement;

bool WindowBinder::toTiPBExecutor(
    tipb::Executor * tipb_executor,
    int32_t collator_id,
    const MPPInfo & mpp_info,
    const Context & context)
{
    tipb_executor->set_tp(tipb::ExecType::TypeWindow);
    tipb_executor->set_executor_id(name);
    tipb_executor->set_fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count);

    tipb::Window * window = tipb_executor->mutable_window();
    const auto & input_schema = children[0]->output_schema;
    for (const auto & expr : func_descs)
    {
        tipb::Expr * window_expr = window->add_func_desc();
        const auto * window_func = typeid_cast<const ASTFunction *>(expr.get());
        for (const auto & arg : window_func->arguments->children)
        {
            astToPB(input_schema, arg, window_expr->add_children(), collator_id, context);
        }

        setFieldType(window_func, window_expr, collator_id);
    }

    for (const auto & child : order_by_exprs)
    {
        auto * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = window->add_order_by();
        by->set_desc(elem->direction < 0);
        astToPB(children[0]->output_schema, elem->children[0], by->mutable_expr(), collator_id, context);
    }

    for (const auto & child : partition_by_exprs)
    {
        auto * elem = typeid_cast<ASTPartitionByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid partition by element", ErrorCodes::LOGICAL_ERROR);
        tipb::ByItem * by = window->add_partition_by();
        by->set_desc(elem->direction < 0);
        astToPB(children[0]->output_schema, elem->children[0], by->mutable_expr(), collator_id, context);
    }

    setWindowFrame(frame, window);

    return children[0]->toTiPBExecutor(window->mutable_child(), collator_id, mpp_info, context);
}

void setColumnInfoForAgg(
    TiDB::ColumnInfo & ci,
    const DB::ASTFunction * func,
    const std::vector<TiDB::ColumnInfo> & children_ci)
{
    // TODO: Other agg func.
    if (func->name == "count")
    {
        ci.tp = TiDB::TypeLongLong;
        ci.flag = TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull;
    }
    else if (func->name == "max" || func->name == "min" || func->name == "sum")
    {
        ci = children_ci[0];
        ci.flag &= ~TiDB::ColumnFlagNotNull;
    }
    else
    {
        throw Exception("Unsupported agg function: " + func->name, ErrorCodes::LOGICAL_ERROR);
    }
}

void setColumnInfoForWindowFunc(
    TiDB::ColumnInfo & ci,
    const DB::ASTFunction * func,
    const std::vector<TiDB::ColumnInfo> & children_ci,
    tipb::ExprType expr_type)
{
    // TODO: add more window functions
    switch (expr_type)
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
    case tipb::ExprType::FirstValue:
    case tipb::ExprType::LastValue:
    {
        ci = children_ci[0];
        break;
    }
    default:
        throw Exception(fmt::format("Unsupported window function {}", func->name), ErrorCodes::LOGICAL_ERROR);
    }
}

TiDB::ColumnInfo createColumnInfo(const DB::ASTFunction * func, const std::vector<TiDB::ColumnInfo> & children_ci)
{
    TiDB::ColumnInfo ci;
    auto iter = tests::window_func_name_to_sig.find(func->name);
    if (iter != tests::window_func_name_to_sig.end())
    {
        setColumnInfoForWindowFunc(ci, func, children_ci, iter->second);
        return ci;
    }

    setColumnInfoForAgg(ci, func, children_ci);
    return ci;
}

ExecutorBinderPtr compileWindow(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr func_desc_list,
    ASTPtr partition_by_expr_list,
    ASTPtr order_by_expr_list,
    mock::MockWindowFrame frame,
    uint64_t fine_grained_shuffle_stream_count)
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

    // Build range frame's auxiliary function
    if (frame.start.has_value() && frame.start->isRangeFrame())
        frame.start->buildRangeFrameAuxFunction(input->output_schema);

    // Build range frame's auxiliary function
    if (frame.end.has_value() && frame.end->isRangeFrame())
        frame.end->buildRangeFrameAuxFunction(input->output_schema);

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
            TiDB::ColumnInfo ci = createColumnInfo(func, children_ci);
            output_schema.emplace_back(std::make_pair(func->getColumnName(), ci));
        }
    }

    ExecutorBinderPtr window = std::make_shared<WindowBinder>(
        executor_index,
        output_schema,
        std::move(window_exprs),
        std::move(partition_columns),
        std::move(order_columns),
        std::move(frame),
        fine_grained_shuffle_stream_count);
    window->children.push_back(input);
    return window;
}
} // namespace DB::mock
