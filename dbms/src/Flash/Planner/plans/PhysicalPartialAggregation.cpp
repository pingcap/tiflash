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

#include <Flash/Planner/plans/PhysicalPartialAggregation.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <DataStreams/PartialAggregatingBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>

namespace DB
{
void PhysicalPartialAggregation::finalize(const Names & parent_require)
{
    return child->finalize(parent_require);
}

const Block & PhysicalPartialAggregation::getSampleBlock() const
{
    return child->getSampleBlock();
}

void PhysicalPartialAggregation::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    Block before_agg_header = pipeline.firstStream()->getHeader();
    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    size_t max_threads = std::min(max_streams, pipeline.streams.size());
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        max_threads,
        aggregation_keys,
        aggregation_collators,
        aggregate_descriptions,
        is_final_agg);
    aggregate_store->init(max_threads, params);

    size_t index = 0;
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<PartialAggregatingBlockInputStream>(
            stream,
            aggregate_store,
            index % max_threads,
            log->identifier()); 
        ++index;
    });
}
} // namespace DB
