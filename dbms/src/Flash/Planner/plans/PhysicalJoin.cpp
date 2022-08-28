// Copyright 2022 PingCAP, Ltd.
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

// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Planner/plans/PhysicalJoinBuild.h>
#include <Flash/Planner/plans/PhysicalJoinProbe.h>
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
} // namespace

PhysicalPlanNodePtr PhysicalJoin::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Join & join,
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
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
        tiflash_join.kind,
        tiflash_join.strictness,
        log->identifier(),
        tiflash_join.join_key_collators,
        probe_filter_column_name,
        build_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    recordJoinExecuteInfo(dag_context, executor_id, build_plan->execId(), join_ptr);

    auto physical_join_build = std::make_shared<PhysicalJoinBuild>(
        executor_id,
        build_side_prepare_actions->getSampleBlock().getNamesAndTypes(),
        log->identifier(),
        build_plan,
        join_ptr,
        build_side_prepare_actions);
    physical_join_build->notTiDBOperator();
    physical_join_build->disableRestoreConcurrency();

    auto physical_join_probe = std::make_shared<PhysicalJoinProbe>(
        executor_id,
        join_output_schema,
        log->identifier(),
        probe_plan,
        join_ptr,
        columns_added_by_join,
        probe_side_prepare_actions,
        is_tiflash_right_join,
        Block(join_output_schema));

    auto physical_join = std::make_shared<PhysicalJoin>(
        executor_id,
        join_output_schema,
        log->identifier(),
        physical_join_probe,
        physical_join_build);
    physical_join->notTiDBOperator();
    physical_join_build->disableRestoreConcurrency();
    return physical_join;
}

void PhysicalJoin::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    /// The build side needs to be transformed first.
    {
        DAGPipeline build_pipeline;
        build()->transform(build_pipeline, context, max_streams);
    }

    probe()->transform(pipeline, context, max_streams);
}

void PhysicalJoin::finalize(const Names & parent_require)
{
    probe()->finalize(parent_require);
}

const Block & PhysicalJoin::getSampleBlock() const
{
    return probe()->getSampleBlock();
}
} // namespace DB
