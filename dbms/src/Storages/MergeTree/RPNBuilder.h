#pragma once

#include <Common/typeid_cast.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/queryToString.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
using RPN = std::vector<RPNElement>;

void applyFunction(
    const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value, DataTypePtr & res_type, Field & res_value);

template <typename NodeT, typename PreparedSetsT>
class RPNBuilder
{
public:
    RPNBuilder(const ExpressionActionsPtr & key_expr_, ColumnIndices & key_columns_, const std::vector<NameAndTypePair> & source_columns_)
        : key_expr(key_expr_), key_columns(key_columns_), source_columns(source_columns_)
    {}

    bool isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const NodeT & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, std::vector<String> & out_functions_chain);

    /** Is node the key column
      *  or expression in which column of key is wrapped by chain of functions,
      *  that can be monotomic on certain ranges?
      * If these conditions are true, then returns number of column in key, type of resulting expression
      *  and fills chain of possibly-monotonic functions.
      */
    bool isKeyPossiblyWrappedByMonotonicFunctions(const NodeT & node,
        const Context & context,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        RPNElement::MonotonicFunctionsChain & out_functions_chain);

    void getKeyTuplePositionMapping(const NodeT & node,
        const Context & context,
        std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> & indexes_mapping,
        const size_t tuple_index,
        size_t & out_key_column_num);
    /// Try to prepare KeyTuplePositionMapping for tuples from IN expression.
    bool isTupleIndexable(
        const NodeT & node, const Context & context, RPNElement & out, const SetPtr & prepared_set, size_t & out_key_column_num);

    bool canConstantBeWrappedByMonotonicFunctions(
        const NodeT & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type);

    bool operatorFromNodeTree(const NodeT & node, RPNElement & out);

    bool atomFromNodeTree(
        const NodeT & node, const Context & context, Block & block_with_constants, PreparedSetsT & sets, RPNElement & out);

    void traverseNodeTree(const NodeT & node, const Context & context, Block & block_with_constants, PreparedSetsT & sets, RPN & rpn);

protected:
    const ExpressionActionsPtr & key_expr;
    ColumnIndices & key_columns;
    const std::vector<NameAndTypePair> & source_columns;
};
} // namespace DB
