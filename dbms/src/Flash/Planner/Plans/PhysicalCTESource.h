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

#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Mpp/CTEManager.h>
#include <Flash/Planner/Plans/PhysicalLeaf.h>

namespace DB
{
class PhysicalCTESource : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::CTESource & cte_source);

    PhysicalCTESource(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const Block & sample_block_,
        size_t cte_id_)
        : PhysicalLeaf(executor_id_, PlanType::CTESource, schema_, FineGrainedShuffle(), req_id)
        , sample_block(sample_block_)
        , cte_id(cte_id_)
    {}

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & /*context*/,
        size_t /*concurrency*/) override;

private:
    Block sample_block;
    size_t cte_id;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators partition_col_collators;
};
} // namespace DB
