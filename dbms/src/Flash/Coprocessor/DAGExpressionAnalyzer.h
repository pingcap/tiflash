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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGSet.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/WindowDescription.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{
class Set;
using DAGSetPtr = std::shared_ptr<DAGSet>;
using DAGPreparedSets = std::unordered_map<const tipb::Expr *, DAGSetPtr>;

enum class ExtraCastAfterTSMode
{
    None,
    AppendTimeZoneCast,
    AppendDurationCast
};

class DAGExpressionAnalyzerHelper;
/** Transforms an expression from DAG expression into a sequence of actions to execute it.
  */
class DAGExpressionAnalyzer : private boost::noncopyable
{
public:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

    // source_columns_ is intended to be passed by value to adapt both to left and right references.
    DAGExpressionAnalyzer(std::vector<NameAndTypePair> source_columns_, const Context & context_);

    const Context & getContext() const { return context; }

    const std::vector<NameAndTypePair> & getCurrentInputColumns() const;

    DAGPreparedSets & getPreparedSets() { return prepared_sets; }

    String appendWhere(
        ExpressionActionsChain & chain,
        const std::vector<const tipb::Expr *> & conditions);

    NamesAndTypes buildWindowOrderColumns(const tipb::Sort & window_sort) const;

    std::vector<NameAndTypePair> appendOrderBy(
        ExpressionActionsChain & chain,
        const tipb::TopN & topN);

    /// <aggregation_keys, collators, aggregate_descriptions, before_agg>
    /// May change the source columns.
    std::tuple<Names, TiDB::TiDBCollators, AggregateDescriptions, ExpressionActionsPtr> appendAggregation(
        ExpressionActionsChain & chain,
        const tipb::Aggregation & agg,
        bool group_by_collation_sensitive);

    std::tuple<WindowDescription, NamesAndTypes> appendWindowColumns(const tipb::Window & window, ExpressionActionsChain::Step & step);

    WindowDescription buildWindowDescription(const tipb::Window & window);

    SortDescription getWindowSortDescription(
        const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & by_items) const;

    void initChain(
        ExpressionActionsChain & chain,
        const std::vector<NameAndTypePair> & columns) const;

    ExpressionActionsChain::Step & initAndGetLastStep(ExpressionActionsChain & chain) const;

    void appendJoin(
        ExpressionActionsChain & chain,
        SubqueryForSet & join_query,
        const NamesAndTypesList & columns_added_by_join) const;

    // Generate a project action for non-root DAGQueryBlock,
    // to keep the schema of Block and tidb-schema the same, and
    // guarantee that left/right block of join don't have duplicated column names.
    NamesWithAliases appendFinalProjectForNonRootQueryBlock(
        ExpressionActionsChain & chain,
        const String & column_prefix) const;

    // Generate a project action for root DAGQueryBlock,
    // to keep the schema of Block and tidb-schema the same.
    NamesWithAliases appendFinalProjectForRootQueryBlock(
        ExpressionActionsChain & chain,
        const std::vector<tipb::FieldType> & schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info);

    String getActions(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions,
        bool output_as_uint8_type = false);

    // appendExtraCastsAfterTS will append extra casts after tablescan if needed.
    // 1) add timezone cast after table scan, this is used for session level timezone support
    // the basic idea of supporting session level timezone is that:
    // 1. for every timestamp column used in the dag request, after reading it from table scan,
    //    we add cast function to convert its timezone to the timezone specified in DAG request
    // 2. based on the dag encode type, the return column will be with session level timezone(Arrow encode)
    //    or UTC timezone(Default encode), if UTC timezone is needed, another cast function is used to
    //    convert the session level timezone to UTC timezone.
    // Note in the worst case(e.g select ts_col from table with Default encode), this will introduce two
    // useless casts to all the timestamp columns, however, since TiDB now use chunk encode as the default
    // encoding scheme, the worst case should happen rarely.
    // 2) add duration cast after table scan, this is ued for calculation of duration in TiFlash.
    // TiFlash stores duration type in the form of Int64 in storage layer, and need the extra cast which convert
    // Int64 to duration.
    bool appendExtraCastsAfterTS(
        ExpressionActionsChain & chain,
        const std::vector<ExtraCastAfterTSMode> & need_cast_column,
        const TiDBTableScan & table_scan);

    /// return true if some actions is needed
    bool appendJoinKeyAndJoinFilters(
        ExpressionActionsChain & chain,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types,
        Names & key_names,
        bool left,
        bool is_right_out_join,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters,
        String & filter_column_name);

    void appendSourceColumnsToRequireOutput(ExpressionActionsChain::Step & step) const;

    void appendCastAfterWindow(
        const ExpressionActionsPtr & actions,
        const tipb::Window & window,
        const size_t window_columns_start_index);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    NamesAndTypes buildOrderColumns(
        const ExpressionActionsPtr & actions,
        const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & order_by);

    void appendCastAfterAgg(
        const ExpressionActionsPtr & actions,
        const tipb::Aggregation & agg);

    String buildTupleFunctionForGroupConcat(
        const tipb::Expr & expr,
        SortDescription & sort_desc,
        NamesAndTypes & names_and_types,
        TiDB::TiDBCollators & collators,
        const ExpressionActionsPtr & actions);

    void buildGroupConcat(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions,
        const String & agg_func_name,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns,
        bool result_is_nullable);

    void buildCommonAggFunc(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions,
        const String & agg_func_name,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns,
        bool empty_input_as_null);

    void buildAggFuncs(
        const tipb::Aggregation & aggregation,
        const ExpressionActionsPtr & actions,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns);

    void buildAggGroupBy(
        const google::protobuf::RepeatedPtrField<tipb::Expr> & group_by,
        const ExpressionActionsPtr & actions,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns,
        Names & aggregation_keys,
        std::unordered_set<String> & agg_key_set,
        bool group_by_collation_sensitive,
        TiDB::TiDBCollators & collators);

    void fillArgumentDetail(
        const ExpressionActionsPtr & actions,
        const tipb::Expr & arg,
        Names & arg_names,
        DataTypes & arg_types,
        TiDB::TiDBCollators & arg_collators);

    void makeExplicitSet(
        const tipb::Expr & expr,
        const Block & sample_block,
        bool create_ordered_set,
        const String & left_arg_name);

    String appendCast(
        const DataTypePtr & target_type,
        const ExpressionActionsPtr & actions,
        const String & expr_name);

    String appendCastIfNeeded(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions,
        const String & expr_name);

    /**
     * when force_uint8 is false, alignReturnType align the data type in tiflash with the data type in dag request, otherwise
     * always convert the return type to uint8 or nullable(uint8)
     * @param expr
     * @param actions
     * @param expr_name
     * @param force_uint8
     * @return
     */
    String alignReturnType(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions,
        const String & expr_name,
        bool force_uint8);

    bool buildExtraCastsAfterTS(
        const ExpressionActionsPtr & actions,
        const std::vector<ExtraCastAfterTSMode> & need_cast_column,
        const ::google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & table_scan_columns);

    std::pair<bool, Names> buildJoinKey(
        const ExpressionActionsPtr & actions,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types,
        bool left,
        bool is_right_out_join);

    String applyFunction(
        const String & func_name,
        const Names & arg_names,
        const ExpressionActionsPtr & actions,
        const TiDB::TiDBCollatorPtr & collator);

    String appendTimeZoneCast(
        const String & tz_col,
        const String & ts_col,
        const String & func_name,
        const ExpressionActionsPtr & actions);

    String appendDurationCast(
        const String & fsp_expr,
        const String & dur_expr,
        const String & func_name,
        const ExpressionActionsPtr & actions);

    String convertToUInt8(
        const ExpressionActionsPtr & actions,
        const String & column_name);

    String buildFunction(
        const tipb::Expr & expr,
        const ExpressionActionsPtr & actions);

    String buildFilterColumn(
        const ExpressionActionsPtr & actions,
        const std::vector<const tipb::Expr *> & conditions);

    NamesWithAliases genNonRootFinalProjectAliases(const String & column_prefix) const;

    NamesWithAliases genRootFinalProjectAliases(
        const String & column_prefix,
        const std::vector<Int32> & output_offsets) const;

    // May change the source columns.
    void appendCastForRootFinalProjection(
        const ExpressionActionsPtr & actions,
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        bool need_append_timezone_cast,
        const BoolVec & need_append_type_cast_vec);

    // return {need_append_type_cast, need_append_type_cast_vec}
    // need_append_type_cast_vec: BoolVec of which one should append type cast.
    // And need_append_type_cast_vec.size() == output_offsets.size().
    std::pair<bool, BoolVec> isCastRequiredForRootFinalProjection(
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets) const;

    // all columns from table scan
    NamesAndTypes source_columns;
    DAGPreparedSets prepared_sets;
    const Context & context;
    Settings settings;

    friend class DAGExpressionAnalyzerHelper;
};

} // namespace DB
