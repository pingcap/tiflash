#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGSet.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Set;
using DAGSetPtr = std::shared_ptr<DAGSet>;
using DAGPreparedSets = std::unordered_map<const tipb::Expr *, DAGSetPtr>;

/** Transforms an expression from DAG expression into a sequence of actions to execute it.
  *
  */
class DAGExpressionAnalyzer : private boost::noncopyable
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    // all columns from table scan
    std::vector<NameAndTypePair> source_columns;
    // all columns after aggregation
    std::vector<NameAndTypePair> aggregated_columns;
    DAGPreparedSets prepared_sets;
    Settings settings;
    const Context & context;
    bool after_agg;
    Int32 implicit_cast_count;

public:
    DAGExpressionAnalyzer(std::vector<NameAndTypePair> && source_columns_, const Context & context_);
    DAGExpressionAnalyzer(std::vector<NameAndTypePair> & source_columns_, const Context & context_);
    void appendWhere(ExpressionActionsChain & chain, const std::vector<const tipb::Expr *> & conditions, String & filter_column_name);
    void appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, std::vector<NameAndTypePair> & order_columns);
    void appendAggregation(ExpressionActionsChain & chain, const tipb::Aggregation & agg, Names & aggregate_keys,
        TiDB::TiDBCollators & collators, AggregateDescriptions & aggregate_descriptions, bool group_by_collation_sensitive);
    void appendAggSelect(ExpressionActionsChain & chain, const tipb::Aggregation & agg);
    void generateFinalProject(ExpressionActionsChain & chain, const std::vector<tipb::FieldType> & schema,
        const std::vector<Int32> & output_offsets, const String & column_prefix, bool keep_session_timezone_info,
        NamesWithAliases & final_project);
    String appendCastIfNeeded(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String & expr_name, bool explicit_cast);
    String appendCast(const DataTypePtr & target_type, ExpressionActionsPtr & actions, const String & expr_name);
    String alignReturnType(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String & expr_name, bool force_uint8);
    void initChain(ExpressionActionsChain & chain, const std::vector<NameAndTypePair> & columns) const
    {
        if (chain.steps.empty())
        {
            chain.settings = settings;
            NamesAndTypesList column_list;
            std::unordered_set<String> column_name_set;
            for (const auto & col : columns)
            {
                if (column_name_set.find(col.name) == column_name_set.end())
                {
                    column_list.emplace_back(col.name, col.type);
                    column_name_set.emplace(col.name);
                }
            }
            chain.steps.emplace_back(std::make_shared<ExpressionActions>(column_list, settings));
        }
    }
    void appendJoin(ExpressionActionsChain & chain, SubqueryForSet & join_query, const NamesAndTypesList & columns_added_by_join);
    void appendFinalProject(ExpressionActionsChain & chain, const NamesWithAliases & final_project);
    String getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions, bool output_as_uint8_type = false);
    const std::vector<NameAndTypePair> & getCurrentInputColumns();
    void makeExplicitSet(const tipb::Expr & expr, const Block & sample_block, bool create_ordered_set, const String & left_arg_name);
    String applyFunction(
        const String & func_name, const Names & arg_names, ExpressionActionsPtr & actions, std::shared_ptr<TiDB::ITiDBCollator> collator);
    Int32 getImplicitCastCount() { return implicit_cast_count; };
    bool appendTimeZoneCastsAfterTS(ExpressionActionsChain & chain, std::vector<bool> is_ts_column);
    bool appendJoinKeyAndJoinFilters(ExpressionActionsChain & chain, const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
        const DataTypes & key_types, Names & key_names, bool left, bool is_right_out_join,
        const google::protobuf::RepeatedPtrField<tipb::Expr> & filters, String & filter_column_name);
    String appendTimeZoneCast(const String & tz_col, const String & ts_col, const String & func_name, ExpressionActionsPtr & actions);
    DAGPreparedSets & getPreparedSets() { return prepared_sets; }
    String convertToUInt8(ExpressionActionsPtr & actions, const String & column_name);
    const Context & getContext() const { return context; }
};

} // namespace DB
