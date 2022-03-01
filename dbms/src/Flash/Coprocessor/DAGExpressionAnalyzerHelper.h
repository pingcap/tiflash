#pragma once

#include <Interpreters/ExpressionActions.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

namespace DB
{
class DAGExpressionAnalyzer;

class DAGExpressionAnalyzerHelper
{
public:
    static String buildInFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildBitwiseFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildMultiIfFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildIfNullFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildLogicalFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildLeftUTF8Function(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildCastFunctionInternal(
        DAGExpressionAnalyzer * analyzer,
        const Names & argument_names,
        bool in_union,
        const tipb::FieldType & field_type,
        const ExpressionActionsPtr & actions);

    static String buildCastFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    template <typename Impl>
    static String buildDateAddOrSubFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildRoundFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildRegexpFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String genFuncString(
        const String & func_name,
        const Names & argument_names,
        const TiDB::TiDBCollators & collators);

    using FunctionBuilder = std::function<String(DAGExpressionAnalyzer *, const tipb::Expr &, const ExpressionActionsPtr &)>;
    using FunctionBuilderMap = std::unordered_map<String, FunctionBuilder>;

    static FunctionBuilderMap function_builder_map;
};
} // namespace DB
