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
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/Context.h>

namespace DB
{
String PhysicalPlan::toString()
{
    auto schema_to_string = [&]() {
        FmtBuffer buffer;
        buffer.joinStr(
            schema.cbegin(),
            schema.cend(),
            [](const auto & item, FmtBuffer & buf) { buf.fmtAppend("[{},{}]", item.name, item.type->getName()); },
            ", ");
        return buffer.toString();
    };
    return fmt::format(
        "type: {}, executor_id: {}, is_record_profile_streams: {}, schema: {}",
        DB::toString(type),
        executor_id,
        is_record_profile_streams,
        schema_to_string());
}

void PhysicalPlan::recordProfileStreams(DAGPipeline & pipeline, const Context & context)
{
    if (is_record_profile_streams)
    {
        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void PhysicalPlan::transform(DAGPipeline & pipeline, const Context & context, size_t max_streams)
{
    transformImpl(pipeline, context, max_streams);
    recordProfileStreams(pipeline, context);
}
} // namespace DB