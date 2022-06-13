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

#include <Common/FmtUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlan::PhysicalPlan(
    const String & executor_id_,
    const PlanType & type_,
    const NamesAndTypes & schema_,
    const String & req_id)
    : executor_id(executor_id_)
    , type(type_)
    , schema(schema_)
    , log(Logger::get(type_.toString(), req_id))
{}

String PhysicalPlan::toString()
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
        "type: {}, executor_id: {}, is_record_profile_streams: {}, schema: {}",
        type.toString(),
        executor_id,
        is_record_profile_streams,
        schema_to_string());
}

void PhysicalPlan::finalize()
{
    finalize(PhysicalPlanHelper::schemaToNames(schema));
}

void PhysicalPlan::recordProfileStreams(DAGPipeline & pipeline, const Context & context)
{
    if (is_record_profile_streams)
    {
        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void PhysicalPlan::transform(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    transformImpl(pipeline, context, max_streams);
    recordProfileStreams(pipeline, context);
    context.getDAGContext()->updateFinalConcurrency(pipeline.streams.size(), max_streams);
    restoreConcurrency(pipeline, context.getDAGContext()->final_concurrency, log);
}
} // namespace DB
