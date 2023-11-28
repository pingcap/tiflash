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
    static String buildFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);
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

    static String buildSingleParamJsonRelatedFunctions(
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

    static String buildGroupingFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    static String buildDefaultFunction(
        DAGExpressionAnalyzer * analyzer,
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    using FunctionBuilder
        = std::function<String(DAGExpressionAnalyzer *, const tipb::Expr &, const ExpressionActionsPtr &)>;
    using FunctionBuilderMap = std::unordered_map<String, FunctionBuilder>;

    static FunctionBuilderMap function_builder_map;
};
} // namespace DB
