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

#include <Common/Logger.h>
#include <Flash/Planner/PhysicalPlanNode.h>

#include <unordered_set>

namespace DB
{
class Pipeline
{
public:
    Pipeline(
        const PhysicalPlanNodePtr & plan_node_, 
        UInt32 id_,
        const std::unordered_set<UInt32> & parent_ids_,
        const String & req_id)
        : plan_node(plan_node_)
        , id(id_)
        , parent_ids(parent_ids_)
        , log(Logger::get("Pipeline", req_id))
    {}

    void execute(Context & context, size_t max_streams);

    UInt32 getId() const { return id; }
    const std::unordered_set<UInt32> & getParentIds() const { return parent_ids; }

private:
    PhysicalPlanNodePtr plan_node;

    UInt32 id;

    std::unordered_set<UInt32> parent_ids;

    LoggerPtr log;
};

using PipelinePtr = std::shared_ptr<Pipeline>;
}
