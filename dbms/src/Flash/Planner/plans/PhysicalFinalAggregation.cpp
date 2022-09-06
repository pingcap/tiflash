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

#include <DataStreams/FinalAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/plans/PhysicalFinalAggregation.h>
#include <Interpreters/Context.h>
#include <Transforms/AggregateSource.h>
#include <Transforms/FinalAggregateReader.h>
#include <Transforms/TransformsPipeline.h>

namespace DB
{
void PhysicalFinalAggregation::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
    expr_after_agg->finalize(DB::toNames(schema));
}

const Block & PhysicalFinalAggregation::getSampleBlock() const
{
    return expr_after_agg->getSampleBlock();
}

void PhysicalFinalAggregation::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t max_streams)
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());
    auto reader = std::make_shared<FinalAggregateReader>(aggregate_store);
    for (size_t i = 0; i < max_streams; ++i)
    {
        pipeline.streams.push_back(std::make_shared<FinalAggregatingBlockInputStream>(reader, log->identifier()));
    }
    executeExpression(pipeline, expr_after_agg, log, "expr after aggregation");
}

void PhysicalFinalAggregation::transform(TransformsPipeline & pipeline, Context & /*context*/)
{
    auto reader = std::make_shared<FinalAggregateReader>(aggregate_store);
    pipeline.transform([&](auto & transforms) {
        transforms->setSource(std::make_shared<AggregateSource>(reader));
    });
}

} // namespace DB
