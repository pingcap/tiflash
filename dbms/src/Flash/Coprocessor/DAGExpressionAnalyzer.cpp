#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
extern const int UNSUPPORTED_METHOD;
} // namespace ErrorCodes

static String genCastString(const String & org_name, const String & target_type_name)
{
    return "cast(" + org_name + ", " + target_type_name + ") ";
}

static String genFuncString(const String & func_name, const Names & argument_names)
{
    std::stringstream ss;
    ss << func_name << "(";
    bool first = true;
    for (const String & argument_name : argument_names)
    {
        if (first)
        {
            first = false;
        }
        else
        {
            ss << ", ";
        }
        ss << argument_name;
    }
    ss << ") ";
    return ss.str();
}

DAGExpressionAnalyzer::DAGExpressionAnalyzer(const NamesAndTypesList & source_columns_, const Context & context_)
    : source_columns(source_columns_), context(context_)
{
    settings = context.getSettings();
    after_agg = false;
}

void DAGExpressionAnalyzer::appendAggregation(
    ExpressionActionsChain & chain, const tipb::Aggregation & agg, Names & aggregation_keys, AggregateDescriptions & aggregate_descriptions)
{
    if (agg.group_by_size() == 0 && agg.agg_func_size() == 0)
    {
        //should not reach here
        throw Exception("Aggregation executor without group by/agg exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    Names agg_argument_names;
    for (const tipb::Expr & expr : agg.agg_func())
    {
        const String & agg_func_name = getAggFunctionName(expr);
        AggregateDescription aggregate;
        DataTypes types(expr.children_size());
        aggregate.argument_names.resize(expr.children_size());
        for (Int32 i = 0; i < expr.children_size(); i++)
        {
            String arg_name = getActions(expr.children(i), step.actions);
            agg_argument_names.push_back(arg_name);
            types[i] = step.actions->getSampleBlock().getByName(arg_name).type;
            aggregate.argument_names[i] = arg_name;
        }
        String func_string = genFuncString(agg_func_name, agg_argument_names);
        aggregate.column_name = func_string;
        //todo de-duplicate aggregation column
        aggregate.parameters = Array();
        aggregate.function = AggregateFunctionFactory::instance().get(agg_func_name, types);
        aggregate_descriptions.push_back(aggregate);
        DataTypePtr result_type = aggregate.function->getReturnType();
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(func_string, result_type);
    }

    std::move(agg_argument_names.begin(), agg_argument_names.end(), std::back_inserter(step.required_output));

    for (const tipb::Expr & expr : agg.group_by())
    {
        String name = getActions(expr, step.actions);
        step.required_output.push_back(name);
        // this is a temp result since implicit cast maybe added on these aggregated_columns
        aggregated_columns.emplace_back(name, step.actions->getSampleBlock().getByName(name).type);
        aggregation_keys.push_back(name);
    }
    after_agg = true;
}

void DAGExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, const tipb::Selection & sel, String & filter_column_name)
{
    if (sel.conditions_size() == 0)
    {
        throw Exception("Selection executor without condition exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
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
    initChain(chain, getCurrentInputColumns());
    filter_column_name = getActions(filter, chain.steps.back().actions);
    chain.steps.back().required_output.push_back(filter_column_name);
}

void DAGExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, Strings & order_column_names)
{
    if (topN.order_by_size() == 0)
    {
        throw Exception("TopN executor without order by exprs", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    initChain(chain, getCurrentInputColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    for (const tipb::ByItem & byItem : topN.order_by())
    {
        String name = getActions(byItem.expr(), step.actions);
        step.required_output.push_back(name);
        order_column_names.push_back(name);
    }
}

const NamesAndTypesList & DAGExpressionAnalyzer::getCurrentInputColumns() { return after_agg ? aggregated_columns : source_columns; }

void DAGExpressionAnalyzer::appendFinalProject(ExpressionActionsChain & chain, const NamesWithAliases & final_project)
{
    initChain(chain, getCurrentInputColumns());
    for (auto name : final_project)
    {
        chain.steps.back().required_output.push_back(name.first);
    }
}

void DAGExpressionAnalyzer::appendAggSelect(ExpressionActionsChain & chain, const tipb::Aggregation & aggregation)
{
    initChain(chain, getCurrentInputColumns());
    bool need_update_aggregated_columns = false;
    NamesAndTypesList updated_aggregated_columns;
    ExpressionActionsChain::Step step = chain.steps.back();
    auto agg_col_names = aggregated_columns.getNames();
    for (Int32 i = 0; i < aggregation.agg_func_size(); i++)
    {
        String & name = agg_col_names[i];
        String updated_name = appendCastIfNeeded(aggregation.agg_func(i), step.actions, name);
        if (name != updated_name)
        {
            need_update_aggregated_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_aggregated_columns.emplace_back(name, aggregated_columns.getTypes()[i]);
            step.required_output.push_back(name);
        }
    }
    for (Int32 i = 0; i < aggregation.group_by_size(); i++)
    {
        String & name = agg_col_names[i + aggregation.agg_func_size()];
        String updated_name = appendCastIfNeeded(aggregation.group_by(i), step.actions, name);
        if (name != updated_name)
        {
            need_update_aggregated_columns = true;
            DataTypePtr type = step.actions->getSampleBlock().getByName(updated_name).type;
            updated_aggregated_columns.emplace_back(updated_name, type);
            step.required_output.push_back(updated_name);
        }
        else
        {
            updated_aggregated_columns.emplace_back(name, aggregated_columns.getTypes()[i]);
            step.required_output.push_back(name);
        }
    }

    if (need_update_aggregated_columns)
    {
        auto updated_agg_col_names = updated_aggregated_columns.getNames();
        auto updated_agg_col_types = updated_aggregated_columns.getTypes();
        aggregated_columns.clear();
        for (size_t i = 0; i < updated_aggregated_columns.size(); i++)
        {
            aggregated_columns.emplace_back(updated_agg_col_names[i], updated_agg_col_types[i]);
        }
    }
}

String DAGExpressionAnalyzer::appendCastIfNeeded(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String & expr_name)
{
    if (!expr.has_field_type() && context.getSettingsRef().dag_expr_field_type_strict_check)
    {
        throw Exception("Expression without field type", ErrorCodes::COP_BAD_DAG_REQUEST);
    }
    if (expr.has_field_type() && isFunctionExpr(expr))
    {
        DataTypePtr expected_type = getDataTypeByFieldType(expr.field_type());
        DataTypePtr actual_type = actions->getSampleBlock().getByName(expr_name).type;
        //todo maybe use a more decent compare method
        // todo ignore nullable info??
        if (expected_type->getName() != actual_type->getName())
        {
            // need to add cast function
            // first construct the second argument
            tipb::Expr type_expr;
            type_expr.set_tp(tipb::ExprType::String);
            std::stringstream ss;
            type_expr.set_val(expected_type->getName());
            auto type_field_type = type_expr.field_type();
            type_field_type.set_tp(0xfe);
            type_field_type.set_flag(1);
            String name = getActions(type_expr, actions);
            String cast_name = "CAST";
            const FunctionBuilderPtr & cast_func_builder = FunctionFactory::instance().get(cast_name, context);
            String cast_expr_name = genCastString(expr_name, getName(type_expr, getCurrentInputColumns()));

            Names cast_argument_names;
            cast_argument_names.push_back(expr_name);
            cast_argument_names.push_back(getName(type_expr, getCurrentInputColumns()));
            const ExpressionAction & apply_cast_function
                = ExpressionAction::applyFunction(cast_func_builder, cast_argument_names, cast_expr_name);
            actions->add(apply_cast_function);
            return cast_expr_name;
        }
        else
        {
            return expr_name;
        }
    }
    return expr_name;
}

void DAGExpressionAnalyzer::makeExplicitSet(
    const tipb::Expr & expr, const Block & sample_block, bool create_ordered_set, const String & left_arg_name)
{
    if (prepared_sets.count(&expr))
    {
        return;
    }
    DataTypes set_element_types;
    // todo support tuple in, i.e. (a,b) in ((1,2), (3,4)), currently TiDB convert tuple in into a series of or/and/eq exprs
    // which means tuple in is never be pushed to coprocessor, but it is quite in-efficient
    set_element_types.push_back(sample_block.getByName(left_arg_name).type);

    // todo if this is a single value in, then convert it to equal expr
    SetPtr set = std::make_shared<Set>(SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode));
    set->createFromDAGExpr(set_element_types, expr, create_ordered_set);
    prepared_sets[&expr] = std::move(set);
}

static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}

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
        ColumnID column_id = getColumnID(expr);
        if (column_id < 0 || column_id >= (ColumnID)getCurrentInputColumns().size())
        {
            throw Exception("column id out of bound", ErrorCodes::COP_BAD_DAG_REQUEST);
        }
        //todo check if the column type need to be cast to field type
        return expr_name;
    }
    else if (isFunctionExpr(expr))
    {
        if (isAggFunctionExpr(expr))
        {
            throw Exception("agg function is not supported yet", ErrorCodes::UNSUPPORTED_METHOD);
        }
        const String & func_name = getFunctionName(expr);
        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get(func_name, context);
        Names argument_names;
        DataTypes argument_types;

        if (isInOrGlobalInOperator(func_name))
        {
            String name = getActions(expr.children(0), actions);
            argument_names.push_back(name);
            argument_types.push_back(actions->getSampleBlock().getByName(name).type);
            makeExplicitSet(expr, actions->getSampleBlock(), false, name);
            ColumnWithTypeAndName column;
            column.type = std::make_shared<DataTypeSet>();

            const SetPtr & set = prepared_sets[&expr];

            column.name = getUniqueName(actions->getSampleBlock(), "___set");
            column.column = ColumnSet::create(1, set);
            actions->add(ExpressionAction::addColumn(column));
            argument_names.push_back(column.name);
            argument_types.push_back(column.type);
        }
        else
        {
            for (auto & child : expr.children())
            {
                String name = getActions(child, actions);
                argument_names.push_back(name);
                argument_types.push_back(actions->getSampleBlock().getByName(name).type);
            }
        }

        // re-construct expr_name, because expr_name generated previously is based on expr tree,
        // but for function call, it's argument name may be changed as an implicit cast func maybe
        // inserted(refer to the logic below), so we need to update the expr_name
        // for example, for a expr and(arg1, arg2), the expr_name is and(arg1_name,arg2_name), but
        // if the arg1 need to be casted to the type passed by dag request, then the expr_name
        // should be updated to and(casted_arg1_name, arg2_name)
        expr_name = genFuncString(func_name, argument_names);

        const ExpressionAction & apply_function = ExpressionAction::applyFunction(function_builder, argument_names, expr_name);
        actions->add(apply_function);
        // add cast if needed
        expr_name = appendCastIfNeeded(expr, actions, expr_name);
        return expr_name;
    }
    else
    {
        throw Exception("Unsupported expr type: " + getTypeName(expr), ErrorCodes::UNSUPPORTED_METHOD);
    }
}
} // namespace DB
