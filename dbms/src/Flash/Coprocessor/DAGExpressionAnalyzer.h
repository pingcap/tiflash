#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Transaction/TMTStorages.h>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;
using DAGPreparedSets = std::unordered_map<const tipb::Expr *, SetPtr>;

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
    Poco::Logger * log;

public:
    DAGExpressionAnalyzer(std::vector<NameAndTypePair> && source_columns_, const Context & context_);
    void appendWhere(ExpressionActionsChain & chain, const tipb::Selection & sel, String & filter_column_name);
    void appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, Strings & order_column_names);
    void appendAggregation(ExpressionActionsChain & chain, const tipb::Aggregation & agg, Names & aggregate_keys,
        AggregateDescriptions & aggregate_descriptions);
    void appendAggSelect(ExpressionActionsChain & chain, const tipb::Aggregation & agg, const tipb::DAGRequest & rqst);
    String appendCastIfNeeded(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String & expr_name);
    void initChain(ExpressionActionsChain & chain, const std::vector<NameAndTypePair> & columns) const
    {
        if (chain.steps.empty())
        {
            chain.settings = settings;
            NamesAndTypesList column_list;
            for (const auto & col : columns)
            {
                column_list.emplace_back(col.name, col.type);
            }
            chain.steps.emplace_back(std::make_shared<ExpressionActions>(column_list, settings));
        }
    }
    void appendFinalProject(ExpressionActionsChain & chain, const NamesWithAliases & final_project);
    String getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions);
    const std::vector<NameAndTypePair> & getCurrentInputColumns();
    void makeExplicitSet(const tipb::Expr & expr, const Block & sample_block, bool create_ordered_set, const String & left_arg_name);
    void makeExplicitSetForIndex(const tipb::Expr & expr, const ManageableStoragePtr & storage);
    String applyFunction(const String & func_name, Names & arg_names, ExpressionActionsPtr & actions);
    Int32 getImplicitCastCount() { return implicit_cast_count; };
    bool appendTimeZoneCastsAfterTS(ExpressionActionsChain & chain, std::vector<bool> is_ts_column, const tipb::DAGRequest & rqst);
    String appendTimeZoneCast(const String & tz_col, const String & ts_col, const String & func_name, ExpressionActionsPtr & actions);
    DAGPreparedSets getPreparedSets() { return prepared_sets; }
};

} // namespace DB
