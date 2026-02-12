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

#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>
#include <tipb/executor.pb.h>

namespace DB
{

class PhysicalTableScan : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        const String & executor_id,
        const LoggerPtr & log,
        const TiDBTableScan & table_scan);

    PhysicalTableScan(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const TiDBTableScan & tidb_table_scan_,
        const Block & sample_block_);

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    bool setFilterConditions(const String & filter_executor_id, const tipb::Selection & selection);

    bool hasFilterConditions() const;

    const String & getFilterConditionsId() const;

    void buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context) override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & /*exec_status*/,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

    void buildProjection(DAGPipeline & pipeline);
    void buildProjection(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder);

private:
    FilterConditions filter_conditions;

    TiDBTableScan tidb_table_scan;

    Block sample_block;

    PipelineExecGroupBuilder pipeline_exec_builder;
};
} // namespace DB
