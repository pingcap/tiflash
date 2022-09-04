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
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalJoinProbe.h>
#include <Flash/Planner/plans/PhysicalNonJoinProbe.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
namespace
{
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

void PhysicalJoinProbe::probeSideTransform(DAGPipeline & probe_pipeline, Context & context, size_t max_streams)
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
            auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(input_header, i, not_joined_concurrency, settings.max_block_size);
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

void PhysicalJoinProbe::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);
    probeSideTransform(pipeline, context, max_streams);
    doSchemaProject(pipeline, context);
}

void PhysicalJoinProbe::doSchemaProject(DAGPipeline & pipeline, Context & context)
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

void PhysicalJoinProbe::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalJoinProbe::getSampleBlock() const
{
    return sample_block;
}

std::optional<PhysicalPlanNodePtr> PhysicalJoinProbe::splitNonJoinedPlanNode()
{
    if (!has_non_joined)
        return {};

    has_non_joined = false;
    auto non_joined_plan = std::make_shared<PhysicalNonJoinProbe>(
        executor_id,
        schema,
        log->identifier(),
        join_ptr,
        probe_side_prepare_actions->getSampleBlock(),
        sample_block);
    non_joined_plan->notTiDBOperator();
    non_joined_plan->disableRestoreConcurrency();
    return {non_joined_plan};
}
} // namespace DB
