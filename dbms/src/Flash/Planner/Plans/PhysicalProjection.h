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

#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalProjection : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Projection & projection,
        const PhysicalPlanNodePtr & child);

    // Generate a project action to keep the schema of Block and tidb-schema the same,
    // and guarantee that left/right block of join don't have duplicated column names.
    static PhysicalPlanNodePtr buildNonRootFinal(
        const Context & context,
        const LoggerPtr & log,
        const String & column_prefix,
        const PhysicalPlanNodePtr & child);

    // Generate a project action for root executor,
    // to keep the schema of Block and tidb-schema the same.
    // Because the output of the root executor is sent to other TiFlash or TiDB.
    static PhysicalPlanNodePtr buildRootFinal(
        const Context & context,
        const LoggerPtr & log,
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info,
        const PhysicalPlanNodePtr & child);

    PhysicalProjection(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const String & extra_info_,
        const ExpressionActionsPtr & project_actions_)
        : PhysicalUnary(executor_id_, PlanType::Projection, schema_, fine_grained_shuffle_, req_id, child_)
        , extra_info(extra_info_)
        , project_actions(project_actions_)
    {}

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    const String extra_info;

    ExpressionActionsPtr project_actions;
};
} // namespace DB
