#include <Common/Logger.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalJoin.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
void recordJoinExecuteInfo(
    DAGContext & dag_context,
    const JoinPtr & join_ptr,
    const PhysicalPlanPtr & build_plan,
    const String & join_executor_id)
{
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_plan->execId();
    join_execute_info.join_ptr = join_ptr;
    assert(join_execute_info.join_ptr);
    dag_context.getJoinExecuteInfoMap()[join_executor_id] = std::move(join_execute_info);
}
} // namespace

PhysicalPlanPtr PhysicalJoin::build(
    const Context & context,
    const String & executor_id,
    const tipb::Join & join,
    PhysicalPlanPtr left,
    PhysicalPlanPtr right)
{
    assert(left);
    assert(right);

    auto [kind, build_side_index] = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(join);
    assert(build_side_index == 1 || build_side_index == 0);

    auto [schema, join_ptr, columns_added_by_join, prepare_build_actions, prepare_probe_actions] = JoinInterpreterHelper::handleJoin(context, join, build_side_index, kind, {left->getSampleBlock(), right->getSampleBlock()});

    const auto & build_plan = build_side_index == 0 ? left : right;
    const auto & probe_plan = build_side_index == 0 ? right : left;

    recordJoinExecuteInfo(*context.getDAGContext(), join_ptr, build_plan, executor_id);
    auto physical_join = std::make_shared<PhysicalJoin>(executor_id, schema, join_ptr, columns_added_by_join, prepare_build_actions, prepare_probe_actions);
    physical_join->appendChild(build_plan);
    physical_join->appendChild(probe_plan);
    return physical_join;
}

PhysicalJoin::PhysicalJoin(
    const String & executor_id_,
    const NamesAndTypes & schema_,
    const JoinPtr & join_ptr_,
    const NamesAndTypesList & columns_added_by_join_,
    const ExpressionActionsPtr & prepare_build_actions_,
    const ExpressionActionsPtr & prepare_probe_actions_)
    : PhysicalBinary(executor_id_, PlanType::PhysicalJoinType, schema_)
    , join_ptr(join_ptr_)
    , columns_added_by_join(columns_added_by_join_)
    , prepare_build_actions(prepare_build_actions_)
    , prepare_probe_actions(prepare_probe_actions_)
    , sample_block(PhysicalPlanHelper::constructBlockFromSchema(schema_))
{}

// recordJoinExecuteInfo
void PhysicalJoin::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    DAGPipeline build_pipeline;
    build()->transform(build_pipeline, context, max_streams);

    DAGPipeline & probe_pipeline = pipeline;
    probe()->transform(probe_pipeline, context, max_streams);

    const auto & logger = context.getDAGContext()->log;
    const Settings & settings = context.getSettingsRef();

    /// build build side streams
    build_pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, prepare_build_actions, logger->identifier()); });
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    size_t stream_index = 0;
    build_pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, stream_index++, logger->identifier()); });
    executeUnion(build_pipeline, max_streams, logger, /*ignore_block=*/true);

    SubqueryForSet build_query;
    build_query.source = build_pipeline.firstStream();
    build_query.join = join_ptr;
    build_query.join->setSampleBlock(build_query.source->getHeader());

    /// build probe side streams
    probe_pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, prepare_probe_actions, logger->identifier()); });
    ExpressionActionsPtr join_probe_actions = PhysicalPlanHelper::newActions(probe_pipeline.firstStream()->getHeader(), context);
    join_probe_actions->add(ExpressionAction::ordinaryJoin(join_ptr, columns_added_by_join));
    if (JoinInterpreterHelper::isTiflashRightJoin(join_ptr->getKind()))
    {
        size_t join_build_concurrency = join_ptr->getBuildConcurrency();
        auto & join_execute_info = context.getDAGContext()->getJoinExecuteInfoMap()[executor_id];
        for (size_t i = 0; i < join_build_concurrency; ++i)
        {
            auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(
                pipeline.firstStream()->getHeader(),
                i,
                join_build_concurrency,
                settings.max_block_size);
            probe_pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    for (auto & stream : probe_pipeline.streams)
        stream = std::make_shared<HashJoinProbeBlockInputStream>(stream, join_probe_actions, logger->identifier());

    /// add a project to remove all the useless column
    /// do not need to care about duplicated column names because
    /// it is guaranteed by its children query block
    PhysicalPlanHelper::executeSchemaProjectAction(context, probe_pipeline, schema);
}

void PhysicalJoin::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalJoin::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB