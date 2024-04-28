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
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalAggregation : public PhysicalUnary
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log,
        const tipb::Aggregation & aggregation,
        const FineGrainedShuffle & fine_grained_shuffle,
        const PhysicalPlanNodePtr & child);

    PhysicalAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const std::unordered_map<String, String> & key_from_agg_func_,
        const TiDB::TiDBCollators & aggregation_collators_,
        bool is_final_agg_,
        const AggregateDescriptions & aggregate_descriptions_,
        const ExpressionActionsPtr & expr_after_agg_)
        : PhysicalUnary(executor_id_, PlanType::Aggregation, schema_, fine_grained_shuffle_, req_id, child_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , key_from_agg_func(key_from_agg_func_)
        , aggregation_collators(aggregation_collators_)
        , is_final_agg(is_final_agg_)
        , aggregate_descriptions(aggregate_descriptions_)
        , expr_after_agg(expr_after_agg_)
    {}

    void buildPipeline(PipelineBuilder & builder, Context & context, PipelineExecutorContext & exec_context) override;

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
    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    std::unordered_map<String, String> key_from_agg_func;
    TiDB::TiDBCollators aggregation_collators;
    bool is_final_agg;
    AggregateDescriptions aggregate_descriptions;
    ExpressionActionsPtr expr_after_agg;
};
} // namespace DB
