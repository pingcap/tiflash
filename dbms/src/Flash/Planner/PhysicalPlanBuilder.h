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
#include <Flash/Planner/PhysicalPlan.h>
#include <common/logger_useful.h>

namespace DB
{
class PhysicalPlanBuilder
{
public:
    explicit PhysicalPlanBuilder(Context & context_, const String & req_id)
        : context(context_)
        , log(Logger::get("PhysicalPlanBuilder", req_id))
    {}

    void buildSource(const Block & sample_block);

    PhysicalPlanPtr getResult() const
    {
        RUNTIME_ASSERT(cur_plans.size() == 1, log, "There can only be one plan output, but here are {}", cur_plans.size());
        return cur_plans.back();
    }

private:
    std::vector<PhysicalPlanPtr> cur_plans;

    [[maybe_unused]] Context & context;

    LoggerPtr log;
};
} // namespace DB
