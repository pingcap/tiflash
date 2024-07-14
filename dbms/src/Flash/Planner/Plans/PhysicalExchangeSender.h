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

#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
class PhysicalExchangeSender : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::ExchangeSender & exchange_sender,
        const FineGrainedShuffle & fine_grained_shuffle,
        bool auto_pass_through_agg_flag,
        const PhysicalPlanNodePtr & child);

    PhysicalExchangeSender(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const std::vector<Int64> & partition_col_ids_,
        const TiDB::TiDBCollators & collators_,
        const tipb::ExchangeType & exchange_type_,
        const tipb::CompressionMode & compression_mode_,
        bool auto_pass_through_agg_)
        : PhysicalUnary(executor_id_, PlanType::ExchangeSender, schema_, fine_grained_shuffle_, req_id, child_)
        , partition_col_ids(partition_col_ids_)
        , partition_col_collators(collators_)
        , exchange_type(exchange_type_)
        , compression_mode(compression_mode_)
        , auto_pass_through_agg(auto_pass_through_agg_)
    {}

    void finalizeImpl(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t /*concurrency*/) override;

private:
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators partition_col_collators;
    tipb::ExchangeType exchange_type;
    tipb::CompressionMode compression_mode;
    bool auto_pass_through_agg; // todo del
};
} // namespace DB
