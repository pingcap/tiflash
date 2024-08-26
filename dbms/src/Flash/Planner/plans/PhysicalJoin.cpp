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

#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/TiFlashException.h>
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
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
namespace FailPoints
{
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

namespace
{
void recordJoinExecuteInfo(
    DAGContext & dag_context,
    const String & executor_id,
    const String & build_side_executor_id,
    const JoinPtr & join_ptr)
{
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_side_executor_id;
    join_execute_info.join_ptr = join_ptr;
    assert(join_execute_info.join_ptr);
    dag_context.getJoinExecuteInfoMap()[executor_id] = std::move(join_execute_info);
}

void executeUnionForPreviousNonJoinedData(DAGPipeline & probe_pipeline, Context & context, size_t max_streams, const LoggerPtr & log)
{
    // If there is non-joined-streams here, we need call `executeUnion`
    // to ensure that non-joined-streams is executed after joined-streams.
    if (!probe_pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(probe_pipeline, max_streams, log, false, "final union for non_joined_data");
        restoreConcurrency(probe_pipeline, context.getDAGContext()->final_concurrency, log);
    }
}

} // namespace

PhysicalPlanNodePtr PhysicalJoin::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Join & join,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & left,
    const PhysicalPlanNodePtr & right)
{
    assert(left);
    assert(right);

    left->finalize();
    right->finalize();

    const Block & left_input_header = left->getSampleBlock();
    const Block & right_input_header = right->getSampleBlock();

    JoinInterpreterHelper::TiFlashJoin tiflash_join{join};

    const auto & probe_plan = tiflash_join.build_side_index == 0 ? right : left;
    const auto & build_plan = tiflash_join.build_side_index == 0 ? left : right;

    const Block & probe_side_header = probe_plan->getSampleBlock();
    const Block & build_side_header = build_plan->getSampleBlock();

    String match_helper_name = tiflash_join.genMatchHelperName(left_input_header, right_input_header);
    NamesAndTypesList columns_added_by_join = tiflash_join.genColumnsAddedByJoin(build_side_header, match_helper_name);
    NamesAndTypes join_output_schema = tiflash_join.genJoinOutputColumns(left_input_header, right_input_header, match_helper_name);

    auto & dag_context = *context.getDAGContext();

    /// add necessary transformation if the join key is an expression

    bool is_tiflash_right_join = tiflash_join.isTiFlashRightJoin();

    // prepare probe side
    auto [probe_side_prepare_actions, probe_key_names, probe_filter_column_name] = JoinInterpreterHelper::prepareJoin(
        context,
        probe_side_header,
        tiflash_join.getProbeJoinKeys(),
        tiflash_join.join_key_types,
        /*left=*/true,
        is_tiflash_right_join,
        tiflash_join.getProbeConditions());
    RUNTIME_ASSERT(probe_side_prepare_actions, log, "probe_side_prepare_actions cannot be nullptr");

    // prepare build side
    auto [build_side_prepare_actions, build_key_names, build_filter_column_name] = JoinInterpreterHelper::prepareJoin(
        context,
        build_side_header,
        tiflash_join.getBuildJoinKeys(),
        tiflash_join.join_key_types,
        /*left=*/false,
        is_tiflash_right_join,
        tiflash_join.getBuildConditions());
    RUNTIME_ASSERT(build_side_prepare_actions, log, "build_side_prepare_actions cannot be nullptr");

    auto [other_condition_expr, other_filter_column_name, other_eq_filter_from_in_column_name]
        = tiflash_join.genJoinOtherConditionAction(context, left_input_header, right_input_header, probe_side_prepare_actions);

    const Settings & settings = context.getSettingsRef();
    size_t max_block_size_for_cross_join = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });

    JoinPtr join_ptr = std::make_shared<Join>(
        probe_key_names,
        build_key_names,
        true,
        tiflash_join.kind,
        tiflash_join.strictness,
        log->identifier(),
        fine_grained_shuffle.enable(),
        fine_grained_shuffle.stream_count,
        tiflash_join.join_key_collators,
        probe_filter_column_name,
        build_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    recordJoinExecuteInfo(dag_context, executor_id, build_plan->execId(), join_ptr);

    auto physical_join = std::make_shared<PhysicalJoin>(
        executor_id,
        join_output_schema,
        log->identifier(),
        probe_plan,
        build_plan,
        join_ptr,
        columns_added_by_join,
        probe_side_prepare_actions,
        build_side_prepare_actions,
        is_tiflash_right_join,
        Block(join_output_schema),
        fine_grained_shuffle);
    return physical_join;
}

void PhysicalJoin::probeSideTransform(DAGPipeline & probe_pipeline, Context & context, size_t max_streams)
{
    const auto & settings = context.getSettingsRef();
    auto & dag_context = *context.getDAGContext();

    // TODO we can call `executeUnionForPreviousNonJoinedData` only when has_non_joined == true.
    executeUnionForPreviousNonJoinedData(probe_pipeline, context, max_streams, log);

    /// probe side streams
    assert(probe_pipeline.streams_with_non_joined_data.empty());
    executeExpression(probe_pipeline, probe_side_prepare_actions, log, "append join key and join filters for probe side");
    auto join_probe_actions = PhysicalPlanHelper::newActions(probe_pipeline.firstStream()->getHeader(), context);
    join_probe_actions->add(ExpressionAction::ordinaryJoin(join_ptr, columns_added_by_join));
    /// add join input stream
    if (has_non_joined)
    {
        auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
        size_t not_joined_concurrency = join_ptr->getNotJoinedStreamConcurrency();
        const auto & input_header = probe_pipeline.firstStream()->getHeader();
        for (size_t i = 0; i < not_joined_concurrency; ++i)
        {
            auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(join_ptr, input_header, i, not_joined_concurrency, settings.max_block_size);
            non_joined_stream->setExtraInfo("add stream with non_joined_data if full_or_right_join");
            probe_pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    String join_probe_extra_info = fmt::format("join probe, join_executor_id = {}", execId());
    for (auto & stream : probe_pipeline.streams)
    {
        stream = std::make_shared<HashJoinProbeBlockInputStream>(stream, join_probe_actions, log->identifier());
        stream->setExtraInfo(join_probe_extra_info);
    }
}

void PhysicalJoin::buildSideTransform(DAGPipeline & build_pipeline, Context & context, size_t max_streams)
{
    auto & dag_context = *context.getDAGContext();
    size_t join_build_concurrency = std::max(build_pipeline.streams.size(), build_pipeline.streams_with_non_joined_data.size());

    /// build side streams
    executeExpression(build_pipeline, build_side_prepare_actions, log, "append join key and join filters for build side");
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    String join_build_extra_info = fmt::format("join build, build_side_root_executor_id = {}", build()->execId());
    if (fine_grained_shuffle.enable())
        join_build_extra_info = fmt::format("{} {}", join_build_extra_info, String(enableFineGrainedShuffleExtraInfo));
    auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
    auto build_streams = [&](BlockInputStreams & streams) {
        size_t build_index = 0;
        for (auto & stream : streams)
        {
            stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, build_index++, log->identifier());
            stream->setExtraInfo(join_build_extra_info);
            join_execute_info.join_build_streams.push_back(stream);
        }
    };
    build_streams(build_pipeline.streams);
    build_streams(build_pipeline.streams_with_non_joined_data);
    // for test, join executor need the return blocks to output.
    executeUnion(build_pipeline, max_streams, log, /*ignore_block=*/!context.isTest(), "for join");

    SubqueryForSet build_query;
    build_query.source = build_pipeline.firstStream();
    build_query.join = join_ptr;
    join_ptr->init(build_query.source->getHeader(), join_build_concurrency);
    dag_context.addSubquery(execId(), std::move(build_query));
}

void PhysicalJoin::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    /// The build side needs to be transformed first.
    {
        DAGPipeline build_pipeline;
        build()->transform(build_pipeline, context, max_streams);
        buildSideTransform(build_pipeline, context, max_streams);
    }

    {
        DAGPipeline & probe_pipeline = pipeline;
        probe()->transform(probe_pipeline, context, max_streams);
        probeSideTransform(probe_pipeline, context, max_streams);
    }

    doSchemaProject(pipeline, context);
}

void PhysicalJoin::doSchemaProject(DAGPipeline & pipeline, Context & context)
{
    /// add a project to remove all the useless column
    NamesWithAliases schema_project_cols;
    for (auto & c : schema)
    {
        /// do not need to care about duplicated column names because
        /// it is guaranteed by its children physical plan nodes
        schema_project_cols.emplace_back(c.name, c.name);
    }
    assert(!schema_project_cols.empty());
    ExpressionActionsPtr schema_project = generateProjectExpressionActions(pipeline.firstStream(), context, schema_project_cols);
    assert(schema_project && !schema_project->getActions().empty());
    executeExpression(pipeline, schema_project, log, "remove useless column after join");
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
