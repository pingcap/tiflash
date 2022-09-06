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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalNonJoinProbe.h>
#include <Interpreters/Context.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/BlockInputStreamSource.h>
#include <Transforms/TransformsPipeline.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
void PhysicalNonJoinProbe::probeSideTransform(DAGPipeline & probe_pipeline, Context & context)
{
    const auto & settings = context.getSettingsRef();
    auto & dag_context = *context.getDAGContext();

    /// probe side streams
    assert(probe_pipeline.streams_with_non_joined_data.empty() && probe_pipeline.streams.empty());
    auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
    size_t not_joined_concurrency = join_ptr->getNotJoinedStreamConcurrency();
    for (size_t i = 0; i < not_joined_concurrency; ++i)
    {
        auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(probe_side_prepare_header, i, not_joined_concurrency, settings.max_block_size);
        non_joined_stream->setExtraInfo("add stream with non_joined_data if full_or_right_join");
        probe_pipeline.streams.push_back(non_joined_stream);
        join_execute_info.non_joined_streams.push_back(non_joined_stream);
    }
}

void PhysicalNonJoinProbe::transform(TransformsPipeline & pipeline, Context & context)
{
    const auto & settings = context.getSettingsRef();
    size_t i = 0;
    size_t not_joined_concurrency = pipeline.concurrency();
    pipeline.transform([&](auto & transforms) {
        auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(probe_side_prepare_header, i++, not_joined_concurrency, settings.max_block_size);
        transforms->setSource(std::make_shared<BlockInputStreamSource>(non_joined_stream));
    });

    // todo
    // NamesWithAliases schema_project_cols;
    // for (auto & c : schema)
    //     schema_project_cols.emplace_back(c.name, c.name);
    // ExpressionActionsPtr schema_project = generateProjectExpressionActions(probe_side_prepare_header, context, schema_project_cols);
    // pipeline.transform([&](auto & transforms) {
    //     transforms->append(std::make_shared<ExpressionTransform>(schema_project));
    // });
}

void PhysicalNonJoinProbe::transformImpl(DAGPipeline & pipeline, Context & context, size_t)
{
    probeSideTransform(pipeline, context);
    doSchemaProject(pipeline, context);
}

void PhysicalNonJoinProbe::doSchemaProject(DAGPipeline & pipeline, Context & context)
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

void PhysicalNonJoinProbe::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalNonJoinProbe::getSampleBlock() const
{
    return sample_block;
}
} // namespace DB
