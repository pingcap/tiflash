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

#pragma once

#include <Flash/Pipeline/Pipeline.h>

#include <vector>

namespace DB
{
class PipelineIdGenerator
{
public:
    UInt32 nextID() { return current_id++; }

private:
    UInt32 current_id = 0;
};
using PipelineIdGeneratorPtr = std::shared_ptr<PipelineIdGenerator>;

// PipelineBuilder is used to build pipelines,
// and PipelineBreaker is used to build the tree struct relationship between pipelines.
class PipelineBuilder
{
public:
    explicit PipelineBuilder(const String & req_id)
        : log(Logger::get(req_id))
        , id_generator(std::make_shared<PipelineIdGenerator>())
        , pipeline_breaker(std::nullopt)
    {
        pipeline = std::make_shared<Pipeline>(id_generator->nextID(), req_id);
    }

private:
    struct PipelineBreaker
    {
        PipelineBreaker(const PipelinePtr & pipeline_, const PhysicalPlanNodePtr & breaker_node_)
            : pipeline(pipeline_)
            , breaker_node(breaker_node_)
        {
            RUNTIME_CHECK(pipeline);
            RUNTIME_CHECK(breaker_node);
        }

        // the broken pipeline.
        const PipelinePtr pipeline;
        const PhysicalPlanNodePtr breaker_node;
    };

    PipelineBuilder(
        const PipelineIdGeneratorPtr & id_generator_,
        PipelineBreaker && pipeline_breaker_,
        const String & req_id)
        : log(Logger::get(req_id))
        , id_generator(id_generator_)
        , pipeline_breaker(std::move(pipeline_breaker_))
    {
        pipeline = std::make_shared<Pipeline>(id_generator->nextID(), req_id);
    }

public:
    void addPlanNode(const PhysicalPlanNodePtr & node) { pipeline->addPlanNode(node); }

    /// Break the current pipeline and return a new builder for the broken pipeline.
    PipelineBuilder breakPipeline(const PhysicalPlanNodePtr & breaker_node)
    {
        return PipelineBuilder(id_generator, PipelineBreaker{pipeline, breaker_node}, log->identifier());
    }

    void setHasPipelineBreakerWaitTime(bool value) { pipeline->setHasPipelineBreakerWaitTime(value); }

    PipelinePtr build()
    {
        RUNTIME_CHECK(pipeline);
        if (pipeline_breaker)
        {
            // First add the breaker node as the last node in this pipeline.
            pipeline->addPlanNode(pipeline_breaker->breaker_node);
            // Then set this pipeline as a child of the broken pipeline.
            pipeline_breaker->pipeline->addChild(pipeline);
            pipeline_breaker.reset();
        }
        return pipeline;
    }

private:
    LoggerPtr log;
    PipelineIdGeneratorPtr id_generator;
    PipelinePtr pipeline;
    std::optional<PipelineBreaker> pipeline_breaker;
};
} // namespace DB
