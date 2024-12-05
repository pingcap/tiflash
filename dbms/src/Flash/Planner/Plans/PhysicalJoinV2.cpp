// Copyright 2024 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Pipeline/PipelineBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalJoinV2.h>
#include <Flash/Planner/Plans/PhysicalJoinV2Build.h>
#include <Flash/Planner/Plans/PhysicalJoinV2Probe.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{

namespace
{
void recordJoinExecuteInfo(
    DAGContext & dag_context,
    const String & executor_id,
    const String & build_side_executor_id,
    const HashJoinPtr & join_ptr)
{
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_side_executor_id;
    join_execute_info.join_profile_info = join_ptr->getProfileInfo();
    RUNTIME_CHECK(join_execute_info.join_profile_info);
    dag_context.getJoinExecuteInfoMap()[executor_id] = std::move(join_execute_info);
}
} // namespace

PhysicalPlanNodePtr PhysicalJoinV2::build(
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

    auto join_req_id = fmt::format("{}_{}", log->identifier(), executor_id);

    HashJoinPtr join_ptr = std::make_shared<HashJoin>(
        probe_key_names,
        build_key_names,
        tiflash_join.kind,
        join_req_id,
        join_output_schema,
        tiflash_join.join_key_collators,
        join_non_equal_conditions,
        context.getSettingsRef(),
        match_helper_name);

    recordJoinExecuteInfo(dag_context, executor_id, build_plan->execId(), join_ptr);

    auto physical_join = std::make_shared<PhysicalJoinV2>(
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

void PhysicalJoinV2::buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context)
{
    // Break the pipeline for join build.
    auto join_build = std::make_shared<PhysicalJoinV2Build>(
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
    probe()->buildPipeline(builder, context, exec_context);
    auto join_probe = std::make_shared<PhysicalJoinV2Probe>(
        executor_id,
        schema,
        req_id,
        probe(),
        join_ptr,
        probe_side_prepare_actions);
    builder.addPlanNode(join_probe);
}

void PhysicalJoinV2::finalizeImpl(const Names & parent_require)
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

const Block & PhysicalJoinV2::getSampleBlock() const
{
    return join_ptr->getOutputBlock();
}

bool PhysicalJoinV2::isSupported(const tipb::Join & join)
{
    JoinInterpreterHelper::TiFlashJoin tiflash_join(join, false);
    using enum ASTTableJoin::Kind;
    switch (tiflash_join.kind)
    {
    case Inner:
        //case LeftOuter:
        //case Semi:
        //case Anti:
        //case RightOuter:
        //case RightSemi:
        //case RightAnti:
        {
            if (!tiflash_join.getBuildJoinKeys().empty())
                return true;
        }
    default:
    }
    return false;
}


} // namespace DB
