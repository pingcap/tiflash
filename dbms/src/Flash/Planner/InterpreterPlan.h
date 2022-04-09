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

#include <DataStreams/BlockIO.h>
#include <Flash/Planner/PlanQuerySource.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{
class Context;
class DAGContext;

class InterpreterPlan : public IInterpreter
{
public:
    InterpreterPlan(Context & context_, const PlanQuerySource & plan_source_);

    ~InterpreterPlan() = default;

    BlockIO execute() override;

private:
    DAGContext & dagContext() const { return plan_source.getDAGContext(); }

    Context & context;
    const PlanQuerySource & plan_source;
    /// How many streams we ask for storage to produce, and in how many threads we will do further processing.
    size_t max_streams = 1;
};
} // namespace DB
