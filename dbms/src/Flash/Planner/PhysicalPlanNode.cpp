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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNode::PhysicalPlanNode(
    const String & executor_id_,
    const PlanType & type_,
    const NamesAndTypes & schema_,
    const String & req_id)
    : executor_id(executor_id_)
    , type(type_)
    , schema(schema_)
    , log(Logger::get(type_.toString(), req_id))
{}

String PhysicalPlanNode::toString()
{
    auto schema_to_string = [&]() {
        FmtBuffer buffer;
        buffer.joinStr(
            schema.cbegin(),
            schema.cend(),
            [](const auto & item, FmtBuffer & buf) { buf.fmtAppend("<{}, {}>", item.name, item.type->getName()); },
            ", ");
        return buffer.toString();
    };
    return fmt::format(
        "<{}, {}> | is_tidb_operator: {}, schema: {}",
        type.toString(),
        executor_id,
        is_tidb_operator,
        schema_to_string());
}

void PhysicalPlanNode::finalize()
{
    finalize(DB::toNames(schema));
}

void PhysicalPlanNode::recordProfileStreams(DAGPipeline & pipeline, const Context & context)
{
    auto & profile_streams_map = context.getDAGContext()->getProfileStreamsMap();
    /// The profile stream of some operators has been recorded.
    /// For example, `DAGStorageInterpreter` records the profile streams of PhysicalTableScan.
    if (profile_streams_map.find(executor_id) == profile_streams_map.end())
    {
        auto & profile_streams = profile_streams_map[executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void PhysicalPlanNode::transform(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    transformImpl(pipeline, context, max_streams);
    if (is_tidb_operator)
        recordProfileStreams(pipeline, context);
    if (is_restore_concurrency)
    {
        context.getDAGContext()->updateFinalConcurrency(pipeline.streams.size(), max_streams);
        restoreConcurrency(pipeline, context.getDAGContext()->final_concurrency, log);
    }
}
} // namespace DB
