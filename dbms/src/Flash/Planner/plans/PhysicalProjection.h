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

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalProjection : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const String & req_id,
        const tipb::Projection & projection,
        PhysicalPlanPtr child);

    static PhysicalPlanPtr buildNonRootFinal(
        const Context & context,
        const String & req_id,
        const String & column_prefix,
        PhysicalPlanPtr child);

    static PhysicalPlanPtr buildRootFinal(
        const Context & context,
        const String & req_id,
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info,
        const PhysicalPlanPtr & child);

    PhysicalProjection(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const ExpressionActionsPtr & project_actions_)
        : PhysicalUnary(executor_id_, PlanType::Projection, schema_, req_id)
        , project_actions(project_actions_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    ExpressionActionsPtr project_actions;
};
} // namespace DB
