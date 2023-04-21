// Copyright 2023 PingCAP, Ltd.
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
#include <Operators/SourceOp_fwd.h>
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

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    bool setFilterConditions(const String & filter_executor_id, const tipb::Selection & selection);

    bool hasFilterConditions() const;

    const String & getFilterConditionsId() const;

    void buildPipelineExecGroup(
        PipelineExecutorStatus & exec_status,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t concurrency) override;

    // generate sourceOps in compile time
    void buildPipeline(
        PipelineBuilder & builder,
        Context & context,
        PipelineExecutorStatus & exec_status) override;


private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;
    void buildProjection(DAGPipeline & pipeline, const NamesAndTypes & storage_schema);
    void buildProjection(
        PipelineExecutorStatus & exec_status,
        PipelineExecGroupBuilder & group_builder,
        const NamesAndTypes & storage_schema);

private:
    FilterConditions filter_conditions;

    TiDBTableScan tidb_table_scan;

    std::unique_ptr<DAGStorageInterpreter> storage_interpreter;

    Block sample_block;

    SourceOps source_ops;
};
} // namespace DB
