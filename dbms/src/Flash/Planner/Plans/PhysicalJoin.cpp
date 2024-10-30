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
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalJoin.h>
#include <Flash/Planner/Plans/PhysicalJoinBuild.h>
#include <Flash/Planner/Plans/PhysicalJoinProbe.h>
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
    join_execute_info.join_profile_info = join_ptr->profile_info;
    RUNTIME_CHECK(join_execute_info.join_profile_info);
    dag_context.getJoinExecuteInfoMap()[executor_id] = std::move(join_execute_info);
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
    RUNTIME_CHECK(left);
    RUNTIME_CHECK(right);

    const Block & left_input_header = left->getSampleBlock();
    const Block & right_input_header = right->getSampleBlock();

    JoinInterpreterHelper::TiFlashJoin tiflash_join(join, context.isTest());

    const auto & probe_plan = tiflash_join.build_side_index == 0 ? right : left;
    const auto & build_plan = tiflash_join.build_side_index == 0 ? left : right;
    const auto probe_source_columns = tiflash_join.build_side_index == 0
        ? JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(right_input_header, right->getSchema())
        : JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(left_input_header, left->getSchema());
    const auto & build_source_columns = tiflash_join.build_side_index == 0
        ? JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(left_input_header, left->getSchema())
        : JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(right_input_header, right->getSchema());

    String match_helper_name = tiflash_join.genMatchHelperName(left_input_header, right_input_header);
    NamesAndTypes join_output_schema
        = tiflash_join.genJoinOutputColumns(left->getSchema(), right->getSchema(), match_helper_name);
    auto & dag_context = *context.getDAGContext();

    /// add necessary transformation if the join key is an expression

    JoinNonEqualConditions join_non_equal_conditions;
    // prepare probe side
    auto [probe_side_prepare_actions, probe_key_names, original_probe_key_names, probe_filter_column_name]
        = JoinInterpreterHelper::prepareJoin(
            context,
            probe_source_columns,
            tiflash_join.getProbeJoinKeys(),
            tiflash_join.join_key_types,
            tiflash_join.getProbeConditions());
    RUNTIME_ASSERT(probe_side_prepare_actions, log, "probe_side_prepare_actions cannot be nullptr");
    /// in TiFlash, left side is always the probe side
    join_non_equal_conditions.left_filter_column = std::move(probe_filter_column_name);

    // prepare build side
    auto [build_side_prepare_actions, build_key_names, original_build_key_names, build_filter_column_name]
        = JoinInterpreterHelper::prepareJoin(
            context,
            build_source_columns,
            tiflash_join.getBuildJoinKeys(),
            tiflash_join.join_key_types,
            tiflash_join.getBuildConditions());
    RUNTIME_ASSERT(build_side_prepare_actions, log, "build_side_prepare_actions cannot be nullptr");
    /// in TiFlash, right side is always the build side
    join_non_equal_conditions.right_filter_column = std::move(build_filter_column_name);

    tiflash_join.fillJoinOtherConditionsAction(
        context,
        left->getSchema(),
        right->getSchema(),
        probe_side_prepare_actions,
        original_probe_key_names,
        original_build_key_names,
        join_non_equal_conditions);

    const Settings & settings = context.getSettingsRef();
    size_t max_bytes_before_external_join = settings.max_bytes_before_external_join;
    auto join_req_id = fmt::format("{}_{}", log->identifier(), executor_id);
    SpillConfig build_spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_0_build", join_req_id),
        settings.max_cached_data_bytes_in_spiller,
        settings.max_spilled_rows_per_file,
        settings.max_spilled_bytes_per_file,
        context.getFileProvider(),
        settings.max_threads,
        settings.max_block_size);
    SpillConfig probe_spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_0_probe", join_req_id),
        settings.max_cached_data_bytes_in_spiller,
        settings.max_spilled_rows_per_file,
        settings.max_spilled_bytes_per_file,
        context.getFileProvider(),
        settings.max_threads,
        settings.max_block_size);
    size_t max_block_size = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size = 1; });

    String flag_mapped_entry_helper_name = tiflash_join.genFlagMappedEntryHelperName(
        left_input_header,
        right_input_header,
        join_non_equal_conditions.other_cond_expr != nullptr);

    assert(build_key_names.size() == original_build_key_names.size());
    std::unordered_map<String, String> build_key_names_map;
    for (size_t i = 0; i < original_build_key_names.size(); ++i)
    {
        build_key_names_map[original_build_key_names[i]] = build_key_names[i];
    }
    auto runtime_filter_list
        = tiflash_join.genRuntimeFilterList(context, build_source_columns, build_key_names_map, log);
    LOG_DEBUG(log, "before register runtime filter list, list size:{}", runtime_filter_list.size());
    context.getDAGContext()->runtime_filter_mgr.registerRuntimeFilterList(runtime_filter_list);

    JoinPtr join_ptr = std::make_shared<Join>(
        probe_key_names,
        build_key_names,
        tiflash_join.kind,
        join_req_id,
        fine_grained_shuffle.stream_count,
        max_bytes_before_external_join,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{settings.join_restore_concurrency, 0, 0},
        join_output_schema,
        [&](const OperatorSpillContextPtr & operator_spill_context) {
            if (context.getDAGContext() != nullptr)
            {
                context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
            }
        },
        context.getDAGContext() != nullptr ? context.getDAGContext()->getAutoSpillTrigger() : nullptr,
        tiflash_join.join_key_collators,
        join_non_equal_conditions,
        max_block_size,
        settings.shallow_copy_cross_probe_threshold,
        match_helper_name,
        flag_mapped_entry_helper_name,
        settings.join_probe_cache_columns_threshold,
        context.isTest(),
        runtime_filter_list);

    recordJoinExecuteInfo(dag_context, executor_id, build_plan->execId(), join_ptr);

    auto physical_join = std::make_shared<PhysicalJoin>(
        executor_id,
        join_output_schema,
        fine_grained_shuffle,
        log->identifier(),
        probe_plan,
        build_plan,
        join_ptr,
        probe_side_prepare_actions,
        build_side_prepare_actions);
    return physical_join;
}

void PhysicalJoin::probeSideTransform(DAGPipeline & probe_pipeline, Context & context)
{
    const auto & settings = context.getSettingsRef();
    /// probe side streams
    executeExpression(
        probe_pipeline,
        probe_side_prepare_actions,
        log,
        "append join key and join filters for probe side");
    /// add join input stream
    String join_probe_extra_info = fmt::format(
        "join probe, join_executor_id = {}, scan_hash_map_after_probe = {}",
        execId(),
        needScanHashMapAfterProbe(join_ptr->getKind()));
    join_ptr->initProbe(probe_pipeline.firstStream()->getHeader(), probe_pipeline.streams.size());
    size_t probe_index = 0;
    for (auto & stream : probe_pipeline.streams)
    {
        stream = std::make_shared<HashJoinProbeBlockInputStream>(
            stream,
            join_ptr,
            probe_index++,
            log->identifier(),
            settings.max_block_size);
        stream->setExtraInfo(join_probe_extra_info);
    }
    join_ptr->setCancellationHook([&] { return context.isCancelled(); });
}

void PhysicalJoin::buildSideTransform(DAGPipeline & build_pipeline, Context & context, size_t max_streams)
{
    auto & dag_context = *context.getDAGContext();
    size_t join_build_concurrency = build_pipeline.streams.size();

    /// build side streams
    executeExpression(
        build_pipeline,
        build_side_prepare_actions,
        log,
        "append join key and join filters for build side");
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    String join_build_extra_info = fmt::format("join build, build_side_root_executor_id = {}", build()->execId());
    if (fine_grained_shuffle.enabled())
        join_build_extra_info = fmt::format("{} {}", join_build_extra_info, String(enableFineGrainedShuffleExtraInfo));
    auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
    auto build_streams = [&](BlockInputStreams & streams) {
        size_t build_index = 0;
        for (auto & stream : streams)
        {
            stream
                = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, build_index++, log->identifier());
            stream->setExtraInfo(join_build_extra_info);
            join_execute_info.join_build_streams.push_back(stream);
        }
    };
    build_streams(build_pipeline.streams);
    // for test, join executor need the return blocks to output.
    executeUnion(
        build_pipeline,
        max_streams,
        context.getSettingsRef().max_buffered_bytes_in_executor,
        log,
        /*ignore_block=*/!context.isTest(),
        "for join");

    SubqueryForSet build_query;
    build_query.source = build_pipeline.firstStream();
    build_query.join = join_ptr;
    join_ptr->initBuild(build_query.source->getHeader(), join_build_concurrency);
    dag_context.addSubquery(execId(), std::move(build_query));
}

void PhysicalJoin::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    /// The build side needs to be transformed first.
    {
        DAGPipeline build_pipeline;
        build()->buildBlockInputStream(build_pipeline, context, max_streams);
        buildSideTransform(build_pipeline, context, max_streams);
    }

    {
        DAGPipeline & probe_pipeline = pipeline;
        probe()->buildBlockInputStream(probe_pipeline, context, max_streams);
        probeSideTransform(probe_pipeline, context);
    }
}

void PhysicalJoin::buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context)
{
    // Break the pipeline for join build.
    auto join_build = std::make_shared<PhysicalJoinBuild>(
        executor_id,
        build()->getSchema(),
        fine_grained_shuffle,
        req_id,
        build(),
        join_ptr,
        build_side_prepare_actions);
    auto join_build_builder = builder.breakPipeline(join_build);
    // Join build pipeline.
    build()->buildPipeline(join_build_builder, context, exec_context);
    join_build_builder.build();

    // Join probe pipeline.
    builder.setHasPipelineBreakerWaitTime(true);
    probe()->buildPipeline(builder, context, exec_context);
    auto join_probe = std::make_shared<PhysicalJoinProbe>(
        executor_id,
        schema,
        req_id,
        probe(),
        join_ptr,
        probe_side_prepare_actions);
    builder.addPlanNode(join_probe);
}

void PhysicalJoin::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    join_ptr->finalize(parent_require);
    auto required_input_columns = join_ptr->getRequiredColumns();

    Names build_required;
    Names probe_required;
    const auto & build_sample_block = build_side_prepare_actions->getSampleBlock();
    for (const auto & name : required_input_columns)
    {
        if (build_sample_block.has(name))
            build_required.push_back(name);
        else
            /// if name not exists in probe side, it will throw error when call `probe_size_prepare_actions->finalize(probe_required)`
            probe_required.push_back(name);
    }

    build_side_prepare_actions->finalize(build_required);
    build()->finalize(build_side_prepare_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(build_side_prepare_actions, build()->getSampleBlock().columns());

    probe_side_prepare_actions->finalize(probe_required);
    probe()->finalize(probe_side_prepare_actions->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(probe_side_prepare_actions, probe()->getSampleBlock().columns());
}

const Block & PhysicalJoin::getSampleBlock() const
{
    return join_ptr->getOutputBlock();
}
} // namespace DB
