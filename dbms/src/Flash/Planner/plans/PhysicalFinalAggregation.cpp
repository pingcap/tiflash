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
#include <Flash/Planner/plans/PhysicalFinalAggregation.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>

namespace DB
{
void PhysicalFinalAggregation::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);
}

const Block & PhysicalFinalAggregation::getSampleBlock() const
{
    return expr_after_agg->getSampleBlock();
}

void PhysicalFinalAggregation::transformImpl(DAGPipeline & pipeline, Context & /*context*/, size_t max_streams)
{
    assert(pipeline.streams.empty() && pipeline.streams_with_non_joined_data.empty());
    pipeline.streams.push_back(std::make_shared<FinalAggregatingBlockInputStream>(aggregate_store, log->identifier()));
    restoreConcurrency(pipeline, max_streams, log);

    // we can record for agg after restore concurrency.
    // Because the streams of expr_after_agg will provide the correct ProfileInfo.
    // See #3804.
    assert(expr_after_agg && !expr_after_agg->getActions().empty());
    executeExpression(pipeline, expr_after_agg, log, "expr after aggregation");
}

} // namespace DB
