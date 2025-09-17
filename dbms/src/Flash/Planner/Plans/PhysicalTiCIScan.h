// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalTiCIScan : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(const String & executor_id, const LoggerPtr & log, const TiCIScan & tici_scan);

    PhysicalTiCIScan(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const TiCIScan & tici_scan_,
        const Block & sample_block_);

    void setIsCountAgg(bool v) { tici_scan.setIsCountAgg(v); }

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    bool setFilterConditions(const String & filter_executor_id, const tipb::Selection & selection);

    bool hasFilterConditions() const;

    const String & getFilterConditionsId() const;

    void buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context) override;

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & /*exec_status*/,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;
    void buildProjection(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder);
    FilterConditions filter_conditions;

    TiCIScan tici_scan;

    Block sample_block;

    PipelineExecGroupBuilder pipeline_exec_builder;
};
} // namespace DB
