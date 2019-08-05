#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/DAGUtils.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

/** Transforms an expression from DAG expression into a sequence of actions to execute it.
  *
  */
class DAGExpressionAnalyzer : private boost::noncopyable
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    // all columns from table scan
    NamesAndTypesList source_columns;
    // all columns after aggregation
    NamesAndTypesList aggregated_columns;
    Settings settings;
    const Context & context;
    bool after_agg;

public:
    DAGExpressionAnalyzer(const NamesAndTypesList & source_columns_, const Context & context_);
    bool appendWhere(ExpressionActionsChain & chain, const tipb::Selection & sel, String & filter_column_name);
    bool appendOrderBy(ExpressionActionsChain & chain, const tipb::TopN & topN, Strings & order_column_names);
    bool appendAggregation(ExpressionActionsChain & chain, const tipb::Aggregation & agg, Names & aggregate_keys,
        AggregateDescriptions & aggregate_descriptions);
    bool appendAggSelect(ExpressionActionsChain & chain, const tipb::Aggregation & agg);
    String appendCastIfNeeded(const tipb::Expr & expr, ExpressionActionsPtr & actions, const String expr_name);
    void initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const
    {
        if (chain.steps.empty())
        {
            chain.settings = settings;
            chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns, settings));
        }
    }
    String getActions(const tipb::Expr & expr, ExpressionActionsPtr & actions);
    const NamesAndTypesList & getCurrentInputColumns();
};

} // namespace DB
