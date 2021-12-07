#pragma once

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
/** build ch plan from dag request: dag executors -> ch plan
  */
class DAGInterpreterBase
{
public:
    virtual ~DAGInterpreterBase() = default;

    DAGPipelinePtr execute();

protected:
    DAGInterpreterBase(
        Context & context_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const LogWithPrefixPtr & log_);

    virtual void executeImpl(DAGPipelinePtr & pipeline) = 0;

    void executeNonSourceExecutors(DAGPipeline & pipeline);

    void executeWhere(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & expressionActionsPtr,
        const String & filter_column);

    void executeExpression(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & expressionActionsPtr);

    void executeOrder(
        DAGPipeline & pipeline,
        const std::vector<NameAndTypePair> & order_columns);

    void executeLimit(DAGPipeline & pipeline);

    void executeAggregation(
        DAGPipeline & pipeline,
        const ExpressionActionsPtr & expression_actions_ptr,
        Names & key_names,
        TiDB::TiDBCollators & collators,
        AggregateDescriptions & aggregate_descriptions);

    void executeProject(
        DAGPipeline & pipeline,
        const NamesWithAliases & project_cols);

    void recordProfileStreams(const DAGPipeline & pipeline, const String & key);
    void recordProfileStream(const BlockInputStreamPtr & stream, const String & key);

    DAGContext & dagContext() const { return *context.getDAGContext(); }

    Context & context;
    const DAGQueryBlock & query_block;
    const bool keep_session_timezone_info;
    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
    /// How many streams before aggregation
    size_t before_agg_streams = 1;
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
    std::vector<const tipb::Expr *> conditions;
    const LogWithPrefixPtr log;
};
} // namespace DB
