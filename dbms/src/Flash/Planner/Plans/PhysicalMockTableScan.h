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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/PushDownFilter.h>
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
        Int64 table_id_);

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    void buildPipelineExec(PipelineExecGroupBuilder & group_builder, Context & /*context*/, size_t /*concurrency*/) override;

    void initStreams(Context & context);

    // for delta-merge test
    bool pushDownFilter(Context & context, const String & filter_executor_id, const tipb::Selection & selection);

    bool hasPushDownFilter() const;

    const String & getPushDownFilterId() const;

    Int64 getLogicalTableID() const;

    void updateStreams(Context & context);

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & /*context*/, size_t /*max_streams*/) override;

private:
    PushDownFilter push_down_filter;
    Block sample_block;

    BlockInputStreams mock_streams;

    const Int64 table_id;
};
} // namespace DB
