#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/ProjectionInterpreter.h>

namespace DB
{
ProjectionInterpreter::ProjectionInterpreter(
    Context & context_,
    const DAGPipelinePtr & input_pipeline_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    const DAGQuerySource & dag_,
    const LogWithPrefixPtr & log_)
    : DAGInterpreterBase(
        context_,
        query_block_,
        max_streams_,
        keep_session_timezone_info_,
        dag_,
        log_)
    , input_pipeline(input_pipeline_)
{
    assert(query_block.source->tp() == tipb::ExecType::TypeProjection);
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void ProjectionInterpreter::executeImpl(DAGPipelinePtr & pipeline)
{
    pipeline = input_pipeline;

    std::vector<NameAndTypePair> input_columns;
    for (auto const & p : pipeline->firstStream()->getHeader().getNamesAndTypesList())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(input_columns), context);
    DAGExpressionActionsChain chain; // Do not use pipeline->chain
    dag_analyzer.initChain(chain, dag_analyzer.getCurrentInputColumns());
    DAGExpressionActionsChain::Step & last_step = chain.steps.back();
    std::vector<NameAndTypePair> output_columns;
    NamesWithAliases project_cols;
    UniqueNameGenerator unique_name_generator;
    for (const auto & expr : query_block.source->projection().exprs())
    {
        auto expr_name = dag_analyzer.getActions(expr, last_step.actions);
        last_step.required_output.emplace_back(expr_name);
        const auto & col = last_step.actions->getSampleBlock().getByName(expr_name);
        String alias = unique_name_generator.toUniqueName(col.name);
        output_columns.emplace_back(alias, col.type);
        project_cols.emplace_back(col.name, alias);
    }
    pipeline->transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log); });
    executeProject(*pipeline, project_cols);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
    recordProfileStreams(dag.getDAGContext(), *pipeline, query_block.source_name, query_block.id);
}
} // namespace DB
