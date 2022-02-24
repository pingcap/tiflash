#include <Common/LogWithPrefix.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalJoin.h>
#include <Interpreters/Context.h>
#include <Common/FailPoint.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

void PhysicalJoin::recordJoinExecuteInfo(const JoinPtr & join_ptr, DAGContext & dag_context)
{
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_child->execId();
    join_execute_info.join_ptr = join_ptr;
    assert(join_execute_info.join_ptr);
    dag_context.getJoinExecuteInfoMap()[execId()] = std::move(join_execute_info);
}

void PhysicalJoin::transform(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    DAGPipeline & probe_pipeline = pipeline;
    DAGPipeline build_pipeline;
    probe_child->transform(probe_pipeline, context, max_streams);
    build_child->transform(build_pipeline, context, max_streams);
    assert(probe_pipeline.streams_with_non_joined_data.empty());
    assert(build_pipeline.streams_with_non_joined_data.empty());

    DAGContext & dag_context = *context.getDAGContext();
    const LogWithPrefixPtr & logger = dag_context.log;

    if (probe_prepare_expr)
        probe_pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, probe_prepare_expr, logger); });
    if (build_prepare_expr)
        build_pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, build_prepare_expr, logger); });

    const Settings & settings = context.getSettingsRef();
    size_t join_build_concurrency = settings.join_concurrent_build ? std::min(max_streams, build_pipeline.streams.size()) : 1;
    size_t max_block_size_for_cross_join = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });

    JoinPtr join_ptr = std::make_shared<Join>(
        probe_key_names,
        build_key_names,
        true,
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
        kind,
        strictness,
        join_build_concurrency,
        join_key_collators,
        probe_filter_column_name,
        build_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    recordJoinExecuteInfo(join_ptr, dag_context);

    SubqueryForSet build_query;
    size_t stream_index = 0;
    build_pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, ++stream_index, logger); });
    executeUnion(build_pipeline, max_streams, logger, /*ignore_block=*/true);
    assert(build_pipeline.streams.size() == 1);
    build_query.source = build_pipeline.firstStream();
    build_query.join = join_ptr;
    build_query.join->setSampleBlock(build_query.source->getHeader());

    const auto & before_probe_header = probe_pipeline.firstStream()->getHeader();
    if (kind == ASTTableJoin::Kind::Right)
    {
        auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
        for (size_t i = 0; i < join_build_concurrency; ++i)
        {
            auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(before_probe_header, i, join_build_concurrency, settings.max_block_size);
            pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    ExpressionActionsPtr probe_actions = std::make_shared<ExpressionActions>(before_probe_header.getColumnsWithTypeAndName(), context.getSettingsRef());
    probe_actions->add(ExpressionAction::ordinaryJoin(join_ptr, columns_added_by_join));
    for (auto & stream : probe_pipeline.streams)
        stream = std::make_shared<ExpressionBlockInputStream>(stream, probe_actions, logger);
}

bool PhysicalJoin::finalize(const Names & parent_require)
{
    checkSchemaContainsParentRequire(schema, parent_require);
    return false;
}

const Block & PhysicalJoin::getSampleBlock() const
{
    return probe_child->getSampleBlock();
}
} // namespace DB