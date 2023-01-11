// Copyright 2023 PingCAP, Ltd.
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

#pragma once

#include <Flash/Pipeline/Pipeline.h>

#include <vector>

namespace DB
{
class PipelineBuilder
{
public:
    PipelineBuilder() : pipeline(std::make_shared<Pipeline>()), pipeline_breaker(std::nullopt)
    {
    }

private:
    struct PipelineBreaker
    {
        PipelinePtr dependent;
        PhysicalPlanNodePtr breaker_node;
    };

    PipelineBuilder(PipelineBreaker && pipeline_breaker_) : pipeline_breaker(std::move(pipeline_breaker_))
    {
    }

public:
    void addPlanNode(const PhysicalPlanNodePtr & node)
    {
        pipeline->addPlanNode(node);
    }

    /// Break the current pipeline and return a new builder for the broke pipeline.
    PipelineBuilder breakPipeline(const PhysicalPlanNodePtr & breaker_node)
    {
        return PipelineBuilder(PipelineBreaker{pipeline, breaker_node});
    }

    PipelinePtr build()
    {
        if (pipeline_breaker)
        {
            // First add the breaker node as the last node in this pipeline.
            pipeline->addPlanNode(pipeline_breaker->breaker_node);
            // Then set this pipeline as a dependency of the dependent pipeline.
            pipeline_breaker->dependent->addDependency(pipeline);
            pipeline_breaker.reset();
        }
        return pipeline;
    }

private:
    PipelinePtr pipeline;
    std::optional<PipelineBreaker> pipeline_breaker;
};
} // namespace DB
