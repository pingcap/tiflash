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

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Flash/Planner/PlanType.h>

#include <memory>

namespace DB
{
struct DAGPipeline;
class Context;
class DAGContext;

class PhysicalPlan;
using PhysicalPlanPtr = std::shared_ptr<PhysicalPlan>;

class PhysicalPlan
{
public:
    PhysicalPlan(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_)
        : executor_id(executor_id_)
        , type(type_)
        , schema(schema_)
    {}

    virtual ~PhysicalPlan() = default;

    virtual PhysicalPlanPtr children(size_t /*i*/) const = 0;

    virtual void setChild(size_t /*i*/, const PhysicalPlanPtr & /*new_child*/) = 0;

    const PlanType & tp() const { return type; }

    const String & execId() const { return executor_id; }

    const NamesAndTypes & getSchema() const { return schema; }

    virtual void appendChild(const PhysicalPlanPtr & /*new_child*/) = 0;

    virtual size_t childrenSize() const = 0;

    virtual void transform(DAGPipeline & pipeline, Context & context, size_t max_streams);

    virtual void finalize(const Names & parent_require) = 0;
    void finalize();

    /// Obtain a sample block that contains the names and types of result columns.
    virtual const Block & getSampleBlock() const = 0;

    void disableRecordProfileStreams() { is_record_profile_streams = false; }

    String toString();

protected:
    virtual void transformImpl(DAGPipeline & /*pipeline*/, Context & /*context*/, size_t /*max_streams*/){};

    void recordProfileStreams(DAGPipeline & pipeline, const Context & context);

    String executor_id;
    PlanType type;
    NamesAndTypes schema;
    bool is_record_profile_streams = true;
};
} // namespace DB
