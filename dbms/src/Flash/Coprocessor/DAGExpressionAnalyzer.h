#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGSet.h>
#include <Flash/Coprocessor/DAGUtils.h>
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

    std::vector<NameAndTypePair> appendWindowOrderBy(
        ExpressionActionsChain & chain,
        const tipb::Sort & window_sort);

    std::vector<NameAndTypePair> appendOrderBy(
        ExpressionActionsChain & chain,
        const tipb::TopN & topN);

    /// <aggregation_keys, collators, aggregate_descriptions, before_agg>
    std::tuple<Names, TiDB::TiDBCollators, AggregateDescriptions, ExpressionActionsPtr> appendAggregation(
        ExpressionActionsChain & chain,
        const tipb::Aggregation & agg,
        bool group_by_collation_sensitive);

    WindowDescription appendWindow(
        ExpressionActionsChain & chain,
        const tipb::Window & window);

    SortDescription getWindowSortDescription(
        const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & byItems,
        ExpressionActionsChain::Step & step);

    void initChain(
        ExpressionActionsChain & chain,
        const std::vector<NameAndTypePair> & columns) const;

    void appendJoin(
        ExpressionActionsChain & chain,
        SubqueryForSet & join_query,
        const NamesAndTypesList & columns_added_by_join) const;

    NamesWithAliases appendFinalProjectForNonRootQueryBlock(
        ExpressionActionsChain & chain,
        const String & column_prefix) const;

    NamesWithAliases appendFinalProjectForRootQueryBlock(
        ExpressionActionsChain & chain,
        const std::vector<tipb::FieldType> & schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info);

    String getActions(
        const tipb::Expr & expr,
        ExpressionActionsPtr & actions,
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
        const tipb::TableScan & table_scan);

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

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    NamesAndTypes buildOrderColumns(
        ExpressionActionsPtr & actions,
        const ::google::protobuf::RepeatedPtrField<tipb::ByItem> & order_by);

    void appendCastAfterAgg(
        ExpressionActionsChain & chain,
        const tipb::Aggregation & agg);

    void appendWindowSelect(
        ExpressionActionsChain & chain,
        const tipb::Window & window,
        const NamesAndTypes window_columns);

    String buildTupleFunctionForGroupConcat(
        const tipb::Expr & expr,
        SortDescription & sort_desc,
        NamesAndTypes & names_and_types,
        TiDB::TiDBCollators & collators,
        ExpressionActionsPtr & actions);

    void buildGroupConcat(
        const tipb::Expr & expr,
        ExpressionActionsChain::Step & step,
        const String & agg_func_name,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns,
        bool result_is_nullable);

    void buildCommonAggFunc(
        const tipb::Expr & expr,
        ExpressionActionsChain::Step & step,
        const String & agg_func_name,
        AggregateDescriptions & aggregate_descriptions,
        NamesAndTypes & aggregated_columns,
        bool empty_input_as_null);

    void makeExplicitSet(
        const tipb::Expr & expr,
        const Block & sample_block,
        bool create_ordered_set,
        const String & left_arg_name);

    String appendCast(
        const DataTypePtr & target_type,
        ExpressionActionsPtr & actions,
        const String & expr_name);

    String appendCastIfNeeded(
        const tipb::Expr & expr,
        ExpressionActionsPtr & actions,
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
        ExpressionActionsPtr & actions,
        const String & expr_name,
        bool force_uint8);

    String applyFunction(
        const String & func_name,
        const Names & arg_names,
        ExpressionActionsPtr & actions,
        const TiDB::TiDBCollatorPtr & collator);

    String appendTimeZoneCast(
        const String & tz_col,
        const String & ts_col,
        const String & func_name,
        ExpressionActionsPtr & actions);

    String appendDurationCast(
        const String & fsp_expr,
        const String & dur_expr,
        const String & func_name,
        ExpressionActionsPtr & actions);

    String convertToUInt8(
        ExpressionActionsPtr & actions,
        const String & column_name);

    String buildFunction(
        const tipb::Expr & expr,
        ExpressionActionsPtr & actions);

    String buildFilterColumn(
        ExpressionActionsPtr & actions,
        const std::vector<const tipb::Expr *> & conditions);

    void appendSourceColumnsToRequireOutput(ExpressionActionsChain::Step & step);

    // all columns from table scan
    NamesAndTypes source_columns;
    DAGPreparedSets prepared_sets;
    const Context & context;
    Settings settings;

    friend class DAGExpressionAnalyzerHelper;
};

} // namespace DB
