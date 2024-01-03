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
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Planner/PlanType.h>

#include <memory>

namespace DB
{
struct DAGPipeline;
class Context;
class DAGContext;

class PipelineExecutorContext;

class PipelineExecGroupBuilder;

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
class PipelineBuilder;

class Event;
using EventPtr = std::shared_ptr<Event>;

class PhysicalPlanNode;
using PhysicalPlanNodePtr = std::shared_ptr<PhysicalPlanNode>;

class PhysicalPlanNode : public std::enable_shared_from_this<PhysicalPlanNode>
{
public:
    PhysicalPlanNode(
        const String & executor_id_,
        const PlanType & type_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id);

    virtual ~PhysicalPlanNode() = default;

    virtual PhysicalPlanNodePtr children(size_t /*i*/) const = 0;

    const PlanType & tp() const { return type; }

    const String & execId() const { return executor_id; }

    const NamesAndTypes & getSchema() const { return schema; }

    virtual size_t childrenSize() const = 0;

    void buildBlockInputStream(DAGPipeline & pipeline, Context & context, size_t max_streams);

    void buildPipelineExecGroup(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t concurrency);

    virtual void buildPipeline(
        PipelineBuilder & /*builder*/,
        Context & /*context*/,
        PipelineExecutorContext & /*exec_status*/);

    EventPtr sinkComplete(PipelineExecutorContext & exec_context);

    virtual void finalizeImpl(const Names & parent_require) = 0;
    void finalize(const Names & parent_require);

    /// Obtain a sample block that contains the names and types of result columns.
    virtual const Block & getSampleBlock() const = 0;

    bool isTiDBOperator() const { return is_tidb_operator; }

    void notTiDBOperator() { is_tidb_operator = false; }

    void disableRestoreConcurrency() { is_restore_concurrency = false; }

    const FineGrainedShuffle & getFineGrainedShuffle() const { return fine_grained_shuffle; }

    String toString();

    String toSimpleString();

protected:
    /// Used for non-fine grained shuffle sink plan node to trigger two-stage execution logic.
    virtual EventPtr doSinkComplete(PipelineExecutorContext & /*exec_status*/);

    virtual void buildBlockInputStreamImpl(DAGPipeline & /*pipeline*/, Context & /*context*/, size_t /*max_streams*/){};

    virtual void buildPipelineExecGroupImpl(
        PipelineExecutorContext & /*exec_status*/,
        PipelineExecGroupBuilder & /*group_builder*/,
        Context & /*context*/,
        size_t /*concurrency*/)
    {
        throw Exception("Unsupported");
    }

    void recordProfileStreams(DAGPipeline & pipeline, const Context & context);

    String executor_id;
    String req_id;
    PlanType type;
    NamesAndTypes schema;

    // Most operators are not aware of whether they are fine-grained shuffles or not.
    // Whether they are fine-grained shuffles or not, their execution remains unchanged.
    // Only a few operators need to sense fine-grained shuffle, such as exchange sender/receiver, join, aggregate, window and window sort.
    FineGrainedShuffle fine_grained_shuffle;

    bool is_tidb_operator = true;
    bool is_restore_concurrency = true;
    bool finalized = false;

    LoggerPtr log;
};
} // namespace DB
