#include <Storages/MergeTree/RPNBuilder.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/queryToString.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_TYPE_OF_FIELD;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes

const tipb::Expr & getChild(const tipb::Expr & node, int index) { return node.children(index); }

const ASTPtr & getChild(const ASTPtr & node, int index)
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        return func->arguments->children[index];
    }
    else
    {
        return node->children[index];
    }
}

int getChildCount(const tipb::Expr & node) { return node.children_size(); }

int getChildCount(const ASTPtr & node)
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        return func->arguments->children.size();
    }
    else
    {
        return node->children.size();
    }
}

const String getFuncName(const tipb::Expr & node) { return getFunctionName(node); }

const String getFuncName(const ASTPtr & node)
{
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        return func->name;
    }
    return "";
}

const String getColumnName(const tipb::Expr & node, const std::vector<NameAndTypePair> & source_columns)
{
    if (isColumnExpr(node))
        return getColumnNameForColumnExpr(node, source_columns);
    return "";
}

const String getColumnName(const ASTPtr & node, const std::vector<NameAndTypePair> &) { return node->getColumnName(); }

bool isFuncNode(const ASTPtr & node) { return typeid_cast<const ASTFunction *>(node.get()); }

bool isFuncNode(const tipb::Expr & node) { return node.tp() == tipb::ExprType::ScalarFunc; }

/** Computes value of constant expression and it data type.
  * Returns false, if expression isn't constant.
  */
bool getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    String column_name = expr->getColumnName();

    if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(expr.get()))
    {
        /// By default block_with_constants has only one column named "_dummy".
        /// If block contains only constants it's may not be preprocessed by
        //  ExpressionAnalyzer, so try to look up in the default column.
        if (!block_with_constants.has(column_name))
            column_name = "_dummy";

        /// Simple literal
        out_value = lit->value;
        out_type = block_with_constants.getByName(column_name).type;
        return true;
    }
    else if (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isColumnConst())
    {
        /// An expression which is dependent on constants only
        const auto & expr_info = block_with_constants.getByName(column_name);
        out_value = (*expr_info.column)[0];
        out_type = expr_info.type;
        return true;
    }
    else
        return false;
}

/** Computes value of constant expression and it data type.
  * Returns false, if expression isn't constant.
  */
bool getConstant(const tipb::Expr & expr, Block &, Field & out_value, DataTypePtr & out_type)
{

    if (isLiteralExpr(expr))
    {
        out_value = decodeLiteral(expr);
        //todo check if need any extra cast
        out_type = exprHasValidFieldType(expr) ? getDataTypeByFieldType(expr.field_type()) : applyVisitor(FieldToDataType(), out_value);
        return true;
    }

    return false;
}

void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const String & node)
{
    if (desired_type->equals(*src_type))
        return;

    try
    {
        /// NOTE: We don't need accurate info about src_type at this moment
        src_value = convertFieldToType(src_value, *desired_type);
    }
    catch (...)
    {
        throw Exception("Key expression contains comparison between inconvertible types: " + desired_type->getName() + " and "
                + src_type->getName() + " inside " + node,
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}

void applyFunction(
    const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value, DataTypePtr & res_type, Field & res_value)
{
    res_type = func->getReturnType();

    Block block{{arg_type->createColumnConst(1, arg_value), arg_type, "x"}, {nullptr, res_type, "y"}};

    func->execute(block, {0}, 1);

    block.safeGetByPosition(1).column->get(0, res_value);
}

bool setContains(const tipb::Expr & expr, DAGPreparedSets & sets)
{
    // todo support remaining exprs
    return sets.count(&expr) && sets[&expr]->remaining_exprs.empty();
}

bool setContains(const ASTPtr & expr, PreparedSets & sets) { return sets.count(getChild(expr, 1).get()); }

SetPtr & lookByExpr(const tipb::Expr & expr, DAGPreparedSets & sets) { return sets[&expr]->constant_set; }

SetPtr & lookByExpr(const ASTPtr & expr, PreparedSets & sets) { return sets[getChild(expr, 1).get()]; }

String nodeToString(const tipb::Expr & node) { return node.DebugString(); }

String nodeToString(const ASTPtr & node) { return queryToString(node); }

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const NodeT & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, std::vector<String> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
          * Therefore, use the full name of the expression for search.
          */
    const auto & sample_block = key_expr->getSampleBlock();
    String name = getColumnName(node, source_columns);

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(it->first).type;
        return true;
    }

    if (isFuncNode(node))
    {
        if (getChildCount(node) != 1)
            return false;

        out_functions_chain.push_back(getFuncName(node));

        if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(getChild(node, 0), out_key_column_num, out_key_column_type, out_functions_chain))
            return false;

        return true;
    }

    return false;
}

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::isKeyPossiblyWrappedByMonotonicFunctions(const NodeT & node,
    const Context & context,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    RPNElement::MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<String> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_key_column_num, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto func_builder = FunctionFactory::instance().tryGet(*it, context);
        ColumnsWithTypeAndName arguments{{nullptr, key_column_type, ""}};
        auto func = func_builder->build(arguments);

        if (!func || !func->hasInformationAboutMonotonicity())
            return false;

        key_column_type = func->getReturnType();
        out_functions_chain.push_back(func);
    }

    out_key_res_column_type = key_column_type;

    return true;
}

template <typename NodeT, typename PreparedSetsT>
void RPNBuilder<NodeT, PreparedSetsT>::getKeyTuplePositionMapping(const NodeT & node,
    const Context & context,
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> & indexes_mapping,
    const size_t tuple_index,
    size_t & out_key_column_num)
{
    MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
    index_mapping.tuple_index = tuple_index;
    DataTypePtr data_type;
    if (isKeyPossiblyWrappedByMonotonicFunctions(node, context, index_mapping.key_index, data_type, index_mapping.functions))
    {
        indexes_mapping.push_back(index_mapping);
        if (out_key_column_num < index_mapping.key_index)
        {
            out_key_column_num = index_mapping.key_index;
        }
    }
}

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::isTupleIndexable(
    const NodeT & node, const Context & context, RPNElement & out, const SetPtr & prepared_set, size_t & out_key_column_num)
{
    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;

    size_t num_key_columns = prepared_set->getDataTypes().size();

    bool is_func = isFuncNode(node);
    if (is_func && getFuncName(node) == "tuple")
    {
        if (num_key_columns != (size_t)getChildCount(node))
        {
            std::stringstream message;
            message << "Number of columns in section IN doesn't match. " << getChildCount(node) << " at left, " << num_key_columns
                    << " at right.";
            throw Exception(message.str(), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
        }

        size_t current_tuple_index = 0;
        for (int i = 0; i < getChildCount(node); i++)
        {
            const auto & arg = getChild(node, i);
            getKeyTuplePositionMapping(arg, context, indexes_mapping, current_tuple_index, out_key_column_num);
            ++current_tuple_index;
        }
    }
    else
    {
        getKeyTuplePositionMapping(node, context, indexes_mapping, 0, out_key_column_num);
    }

    if (indexes_mapping.empty())
        return false;

    out.set_index = std::make_shared<MergeTreeSetIndex>(prepared_set->getSetElements(), std::move(indexes_mapping));

    return true;
}

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::canConstantBeWrappedByMonotonicFunctions(
    const NodeT & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type)
{
    String expr_name = getColumnName(node, source_columns);
    const auto & sample_block = key_expr->getSampleBlock();
    if (!sample_block.has(expr_name))
        return false;

    bool found_transformation = false;
    for (const ExpressionAction & a : key_expr->getActions())
    {
        /** The key functional expression constraint may be inferred from a plain column in the expression.
              * For example, if the key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
              * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
              * condition also holds, so the index may be used to select only parts satisfying this condition.
              *
              * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
              * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
              * Instead, we can qualify only functions that do not transform the range (for example rounding),
              * which while not strictly monotonic, are monotonic everywhere on the input range.
              */
        const auto & action = a.argument_names;
        if (a.type == ExpressionAction::Type::APPLY_FUNCTION && action.size() == 1 && a.argument_names[0] == expr_name)
        {
            if (!a.function->hasInformationAboutMonotonicity())
                return false;

            // Range is irrelevant in this case
            IFunction::Monotonicity monotonicity = a.function->getMonotonicityForRange(*out_type, Field(), Field());
            if (!monotonicity.is_always_monotonic)
                return false;

            // Apply the next transformation step
            DataTypePtr new_type;
            applyFunction(a.function, out_type, out_value, new_type, out_value);
            if (!new_type)
                return false;

            out_type.swap(new_type);
            expr_name = a.result_name;

            // Transformation results in a key expression, accept
            auto it = key_columns.find(expr_name);
            if (key_columns.end() != it)
            {
                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                found_transformation = true;
                break;
            }
        }
    }

    return found_transformation;
}

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::operatorFromNodeTree(const NodeT & node, RPNElement & out)
{
    /// Functions AND, OR, NOT.
    /** Also a special function `indexHint` - works as if instead of calling a function there are just parentheses
          * (or, the same thing - calling the function `and` from one argument).
          */
    if (!isFuncNode(node))
        return false;
    String name = getFuncName(node);

    if (name == "not")
    {
        if (getChildCount(node) != 1)
            return false;

        out.function = RPNElement::FUNCTION_NOT;
    }
    else
    {
        if (name == "and" || name == "indexHint")
            out.function = RPNElement::FUNCTION_AND;
        else if (name == "or")
            out.function = RPNElement::FUNCTION_OR;
        else
            return false;
    }

    return true;
}

template <typename NodeT, typename PreparedSetsT>
bool RPNBuilder<NodeT, PreparedSetsT>::atomFromNodeTree(
    const NodeT & node, const Context & context, Block & block_with_constants, PreparedSetsT & sets, RPNElement & out)
{
    /** Functions < > = != <= >= in `notIn`, where one argument is a constant, and the other is one of columns of key,
          *  or itself, wrapped in a chain of possibly-monotonic functions,
          *  or constant expression - number.
          */
    Field const_value;
    DataTypePtr const_type;
    if (isFuncNode(node))
    {
        if (getChildCount(node) != 2)
            return false;

        DataTypePtr key_expr_type; /// Type of expression containing key column
        size_t key_arg_pos;        /// Position of argument with key column (non-const argument)
        size_t key_column_num;     /// Number of a key column (inside sort_descr array)
        RPNElement::MonotonicFunctionsChain chain;
        bool is_set_const = false;
        bool is_constant_transformed = false;
        const NodeT & child0 = getChild(node, 0);
        const NodeT & child1 = getChild(node, 1);

        if (setContains(node, sets) && isTupleIndexable(child0, context, out, lookByExpr(node, sets), key_column_num))
        {
            key_arg_pos = 0;
            is_set_const = true;
        }
        else if (getConstant(child1, block_with_constants, const_value, const_type)
            && isKeyPossiblyWrappedByMonotonicFunctions(child0, context, key_column_num, key_expr_type, chain))
        {
            key_arg_pos = 0;
        }
        else if (getConstant(child1, block_with_constants, const_value, const_type)
            && canConstantBeWrappedByMonotonicFunctions(child0, key_column_num, key_expr_type, const_value, const_type))
        {
            key_arg_pos = 0;
            is_constant_transformed = true;
        }
        else if (getConstant(child0, block_with_constants, const_value, const_type)
            && isKeyPossiblyWrappedByMonotonicFunctions(child1, context, key_column_num, key_expr_type, chain))
        {
            key_arg_pos = 1;
        }
        else if (getConstant(child0, block_with_constants, const_value, const_type)
            && canConstantBeWrappedByMonotonicFunctions(child1, key_column_num, key_expr_type, const_value, const_type))
        {
            key_arg_pos = 1;
            is_constant_transformed = true;
        }
        else
            return false;

        std::string func_name = getFuncName(node);

        // make sure that RPNElement of FUNCTION_IN_SET/FUNCTION_NOT_IN_SET
        // has valid set in PreparedSets
        if (func_name == "in" || func_name == "notIn")
            if (!is_set_const)
                return false;

        /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
        if (is_constant_transformed)
        {
            if (func_name == "less")
                func_name = "lessOrEquals";
            else if (func_name == "greater")
                func_name = "greaterOrEquals";
        }

        /// Replace <const> <sign> <data> on to <data> <-sign> <const>
        if (key_arg_pos == 1)
        {
            if (func_name == "less")
                func_name = "greater";
            else if (func_name == "greater")
                func_name = "less";
            else if (func_name == "greaterOrEquals")
                func_name = "lessOrEquals";
            else if (func_name == "lessOrEquals")
                func_name = "greaterOrEquals";
            else if (func_name == "in" || func_name == "notIn" || func_name == "like")
            {
                /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                return false;
            }
        }

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        const auto atom_it = KeyCondition::atom_map.find(func_name);
        if (atom_it == std::end(KeyCondition::atom_map))
            return false;

        bool cast_not_needed = is_set_const                           /// Set args are already casted inside Set::createFromAST
            || (key_expr_type->isNumber() && const_type->isNumber()); /// Numbers are accurately compared without cast.

        if (!cast_not_needed)
            castValueToType(key_expr_type, const_value, const_type, nodeToString(node));

        return atom_it->second(out, const_value);
    }
    else if (getConstant(
                 node, block_with_constants, const_value, const_type)) /// For cases where it says, for example, `WHERE 0 AND something`
    {
        if (const_value.getType() == Field::Types::UInt64 || const_value.getType() == Field::Types::Int64
            || const_value.getType() == Field::Types::Float64)
        {
            /// Zero in all types is represented in memory the same way as in UInt64.
            out.function = const_value.get<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;

            return true;
        }
    }

    return false;
}

template <typename NodeT, typename PreparedSetsT>
void RPNBuilder<NodeT, PreparedSetsT>::traverseNodeTree(
    const NodeT & node, const Context & context, Block & block_with_constants, PreparedSetsT & sets, RPN & rpn)
{
    RPNElement element;

    if (isFuncNode(node))
    {
        if (operatorFromNodeTree(node, element))
        {
            for (size_t i = 0, size = getChildCount(node); i < size; ++i)
            {
                traverseNodeTree(getChild(node, i), context, block_with_constants, sets, rpn);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                      * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                      */
                if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
                    rpn.push_back(element);
            }

            return;
        }
    }

    if (!atomFromNodeTree(node, context, block_with_constants, sets, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}

template class RPNBuilder<ASTPtr, PreparedSets>;
template class RPNBuilder<tipb::Expr, DAGPreparedSets>;

} // namespace DB
