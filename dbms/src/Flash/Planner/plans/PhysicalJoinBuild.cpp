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
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalJoinBuild.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <fmt/format.h>
#include <Transforms/ExpressionTransform.h>
#include <Transforms/HashJoinBuildSink.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalJoinBuild::buildSideTransform(DAGPipeline & build_pipeline, Context & context, size_t max_streams)
{
    auto & dag_context = *context.getDAGContext();
    const auto & settings = context.getSettingsRef();

    size_t join_build_concurrency = settings.join_concurrent_build ? std::min(max_streams, build_pipeline.streams.size()) : 1;

    /// build side streams
    executeExpression(build_pipeline, build_side_prepare_actions, log, "append join key and join filters for build side");
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    auto get_concurrency_build_index = JoinInterpreterHelper::concurrencyBuildIndexGenerator(join_build_concurrency);
    String join_build_extra_info = fmt::format("join build, build_side_root_executor_id = {}", child->execId());
    auto & join_execute_info = dag_context.getJoinExecuteInfoMap()[execId()];
    build_pipeline.transform([&](auto & stream) {
        stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, get_concurrency_build_index(), log->identifier());
        stream->setExtraInfo(join_build_extra_info);
        join_execute_info.join_build_streams.push_back(stream);
    });
    if (!join_ptr->initialized)
    {
        join_ptr->init(build_pipeline.firstStream()->getHeader(), join_build_concurrency);
    }
}

void PhysicalJoinBuild::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    buildSideTransform(pipeline, context, max_streams);
}

void PhysicalJoinBuild::transform(TransformsPipeline & pipeline, Context & context)
{
    child->transform(pipeline, context);

    pipeline.transform([&](auto & transforms) { 
        transforms->append(std::make_shared<ExpressionTransform>(build_side_prepare_actions));
        transforms->set(std::make_shared<HashJoinBuildSink>(join_ptr)); 
    });
    if (!join_ptr->initialized)
    {
        join_ptr->init(build_side_prepare_actions->getSampleBlock(), pipeline.concurrency());
    }
}

void PhysicalJoinBuild::finalize(const Names & parent_require)
{
    // schema.size() >= parent_require.size()
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalJoinBuild::getSampleBlock() const
{
    return build_side_prepare_actions->getSampleBlock();
}
} // namespace DB
