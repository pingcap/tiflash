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

#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <common/logger_useful.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalSource::build(
    const String & executor_id,
    const BlockInputStreams & source_streams,
    const LoggerPtr & log)
{
    RUNTIME_ASSERT(!source_streams.empty(), log, "source streams cannot be empty");
    Block sample_block = source_streams.back()->getHeader();
    NamesAndTypes schema;
    for (const auto & col : sample_block)
        schema.emplace_back(col.name, col.type);
    return std::make_shared<PhysicalSource>(executor_id, schema, log->identifier(), sample_block, source_streams);
}

void PhysicalSource::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/)
{
    pipeline.streams.insert(pipeline.streams.end(), source_streams.begin(), source_streams.end());
}
} // namespace DB
