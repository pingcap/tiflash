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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/RuntimeFilterMgr.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>
#include <tipb/executor.pb.h>

namespace DB
{
/**
 * A physical plan node that generates MockTableScanBlockInputStream.
 * Used in gtest to test execution logic.
 * Only available with `context.isTest() == true`.
 */
class PhysicalMockTableScan : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const TiDBTableScan & table_scan);

    PhysicalMockTableScan(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const Block & sample_block_,
        const BlockInputStreams & mock_streams_,
        Int64 table_id_,
        bool keep_order_,
        const std::vector<Int32> & runtime_filter_ids_);

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    void initStreams(Context & context);

    // for delta-merge test
    bool setFilterConditions(Context & context, const String & filter_executor_id, const tipb::Selection & selection);

    bool hasFilterConditions() const;

    const String & getFilterConditionsId() const;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext &,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t) override;

    void buildRuntimeFilterInLocalStream(Context & context);

    RuntimeFilteList getRuntimeFilterList(Context & context);

private:
    FilterConditions filter_conditions;
    Block sample_block;

    BlockInputStreams mock_streams;

    const Int64 table_id;

    const bool keep_order;

    std::vector<Int32> runtime_filter_ids;

    const int rf_max_wait_time_ms = 10000;
};
} // namespace DB
