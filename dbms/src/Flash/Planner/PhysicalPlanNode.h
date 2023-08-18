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

#include <Common/Logger.h>
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

class PhysicalPlanNode;
using PhysicalPlanNodePtr = std::shared_ptr<PhysicalPlanNode>;

class PhysicalPlanNode
{
public:
    PhysicalPlanNode(
        const String & executor_id_,
        const PlanType & type_,
        const NamesAndTypes & schema_,
        const String & req_id);

    virtual ~PhysicalPlanNode() = default;

    virtual PhysicalPlanNodePtr children(size_t /*i*/) const = 0;

    virtual void setChild(size_t /*i*/, const PhysicalPlanNodePtr & /*new_child*/) = 0;

    const PlanType & tp() const { return type; }

    const String & execId() const { return executor_id; }

    const NamesAndTypes & getSchema() const { return schema; }

    virtual size_t childrenSize() const = 0;

    virtual void transform(DAGPipeline & pipeline, Context & context, size_t max_streams);

    virtual void finalize(const Names & parent_require) = 0;
    void finalize();

    /// Obtain a sample block that contains the names and types of result columns.
    virtual const Block & getSampleBlock() const = 0;

    bool isTiDBOperator() const { return is_tidb_operator; }

    void notTiDBOperator() { is_tidb_operator = false; }

    void disableRestoreConcurrency() { is_restore_concurrency = false; }

    String toString();

protected:
    virtual void transformImpl(DAGPipeline & /*pipeline*/, Context & /*context*/, size_t /*max_streams*/){};

    void recordProfileStreams(DAGPipeline & pipeline, const Context & context);

    String executor_id;
    PlanType type;
    NamesAndTypes schema;

    bool is_tidb_operator = true;
    bool is_restore_concurrency = true;

    LoggerPtr log;
};
} // namespace DB
