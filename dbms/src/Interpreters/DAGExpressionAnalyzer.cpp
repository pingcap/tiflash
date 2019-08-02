
#include <DataTypes/FieldToDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/CoprocessorBuilderUtils.h>
#include <Interpreters/DAGExpressionAnalyzer.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
DAGExpressionAnalyzer::DAGExpressionAnalyzer(const NamesAndTypesList & source_columns_, const Context & context_)
    : source_columns(source_columns_), context(context_)
{
    settings = context.getSettings();
}

bool DAGExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, const tipb::Selection & sel, String & filter_column_name)
{
    if (sel.conditions_size() == 0)
    {
        return false;
    }
    tipb::Expr final_condition;
    if (sel.conditions_size() > 1)
    {
        final_condition.set_tp(tipb::ExprType::ScalarFunc);
        final_condition.set_sig(tipb::ScalarFuncSig::LogicalAnd);

        for (auto & condition : sel.conditions())
        {
            auto c = final_condition.add_children();
            c->ParseFromString(condition.SerializeAsString());
        }
    }

    const tipb::Expr & filter = sel.conditions_size() > 1 ? final_condition : sel.conditions(0);
    initChain(chain, source_columns);
    filter_column_name = getActions(filter, chain.steps.back().actions);
    chain.steps.back().required_output.push_back(filter_column_name);
    return true;
}

bool DAGExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, Strings & order_column_names)
{
    if (topN.order_by_size() == 0)
    {
        return false;
    }
    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();
    for (const tipb::ByItem & byItem : topN.order_by())
    {
        String name = getActions(byItem.expr(), step.actions);
        step.required_output.push_back(name);
        order_column_names.push_back(name);
    }
    return true;
}

const NamesAndTypesList & DAGExpressionAnalyzer::getCurrentInputColumns() { return source_columns; }

String DAGExpressionAnalyzer::getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions)
{
    String expr_name = getName(expr, getCurrentInputColumns());
    if ((isLiteralExpr(expr) || isFunctionExpr(expr)) && actions->getSampleBlock().has(expr_name))
    {
        return expr_name;
    }
    if (isLiteralExpr(expr))
    {
        Field value = decodeLiteral(expr);
        DataTypePtr type = expr.has_field_type() ? getDataTypeByFieldType(expr.field_type()) : applyVisitor(FieldToDataType(), value);

        ColumnWithTypeAndName column;
        column.column = type->createColumnConst(1, convertFieldToType(value, *type));
        column.name = expr_name;
        column.type = type;

        actions->add(ExpressionAction::addColumn(column));
        return column.name;
    }
    else if (isColumnExpr(expr))
    {
        ColumnID columnId = getColumnID(expr);
        if (columnId < 1 || columnId > (ColumnID)getCurrentInputColumns().size())
        {
            throw Exception("column id out of bound");
        }
        //todo check if the column type need to be cast to field type
        return expr_name;
    }
    else if (isFunctionExpr(expr))
    {
        if (isAggFunctionExpr(expr))
        {
            throw Exception("agg function is not supported yet");
        }
        const String & func_name = getFunctionName(expr);
        if (func_name == "in" || func_name == "notIn" || func_name == "globalIn" || func_name == "globalNotIn")
        {
            // todo support in
            throw Exception(func_name + " is not supported yet");
        }

        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
        Names argument_names;
        DataTypes argument_types;
        for (auto & child : expr.children())
        {
            String name = getActions(child, actions);
            if (actions->getSampleBlock().has(name))
            {
                argument_names.push_back(name);
                argument_types.push_back(actions->getSampleBlock().getByName(name).type);
            }
            else
            {
                throw Exception("Unknown expr: " + child.DebugString());
            }
        }

        const ExpressionAction & applyFunction = ExpressionAction::applyFunction(function_builder, argument_names, expr_name);
        actions->add(applyFunction);
        // add cast if needed
        if (expr.has_field_type())
        {
            DataTypePtr expected_type = getDataTypeByFieldType(expr.field_type());
            DataTypePtr actual_type = applyFunction.result_type;
            //todo maybe use a more decent compare method
            if (expected_type->getName() != actual_type->getName())
            {
                // need to add cast function
                // first construct the second argument
                tipb::Expr type_expr;
                type_expr.set_tp(tipb::ExprType::String);
                std::stringstream ss;
                EncodeCompactBytes(expected_type->getName(), ss);
                type_expr.set_val(ss.str());
                auto type_field_type = type_expr.field_type();
                type_field_type.set_tp(0xfe);
                type_field_type.set_flag(1);
                String name = getActions(type_expr, actions);
                String cast_name = "cast";
                const FunctionBuilderPtr & cast_func_builder = FunctionFactory::instance().get(cast_name, context);
                String cast_expr_name = cast_name + "_" + expr_name + "_" + getName(type_expr, getCurrentInputColumns());
                Names cast_argument_names;
                cast_argument_names.push_back(expr_name);
                cast_argument_names.push_back(getName(type_expr, getCurrentInputColumns()));
                const ExpressionAction & apply_cast_function
                    = ExpressionAction::applyFunction(cast_func_builder, argument_names, cast_expr_name);
                actions->add(apply_cast_function);
                return cast_expr_name;
            }
            else
            {
                return expr_name;
            }
        }
        else
        {
            return expr_name;
        }
    }
    else
    {
        throw Exception("Unsupported expr type: " + getTypeName(expr));
    }
}
} // namespace DB
