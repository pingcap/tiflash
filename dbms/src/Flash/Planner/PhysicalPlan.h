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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <common/logger_useful.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalPlan
{
public:
    explicit PhysicalPlan(Context & context_, const String & req_id)
        : context(context_)
        , log(Logger::get(req_id))
    {}

    void build(const tipb::DAGRequest * dag_request);

    // after outputAndOptimize, the physical plan node tree is done.
    void outputAndOptimize();

    String toString() const;

    void transform(DAGPipeline & pipeline, Context & context, size_t max_streams);

private:
    void addRootFinalProjectionIfNeed();

    void build(const String & executor_id, const tipb::Executor * executor);

    void buildFinalProjection(const String & column_prefix, bool is_root);

    PhysicalPlanNodePtr popBack();

    void pushBack(const PhysicalPlanNodePtr & plan);

    DAGContext & dagContext() const;

    void buildTableScan(const String & executor_id, const tipb::Executor * executor);

private:
    std::vector<PhysicalPlanNodePtr> cur_plan_nodes{};

    // hold the root node of physical plan node tree after `outputAndOptimize`.
    PhysicalPlanNodePtr root_node;

    Context & context;

    LoggerPtr log;
};
} // namespace DB
