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

#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/Plans/PhysicalUnary.h>
#include <Flash/Planner/Plans/PipelineBreakerHelper.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Operators/OperatorProfileInfo.h>

namespace DB
{
class AggregateContext;
using AggregateContextPtr = std::shared_ptr<AggregateContext>;

class PhysicalAggregationBuild : public PhysicalUnary
{
public:
    PhysicalAggregationBuild(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const TiDB::TiDBCollators & aggregation_collators_,
        const KeyRefAggFuncMap & key_ref_agg_func_,
        const AggFuncRefKeyMap & agg_func_ref_key_,
        bool is_final_agg_,
        const AggregateDescriptions & aggregate_descriptions_,
        const AggregateContextPtr & aggregate_context_)
        : PhysicalUnary(executor_id_, PlanType::AggregationBuild, schema_, FineGrainedShuffle{}, req_id, child_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , aggregation_collators(aggregation_collators_)
        , key_ref_agg_func(key_ref_agg_func_)
        , agg_func_ref_key(agg_func_ref_key_)
        , is_final_agg(is_final_agg_)
        , aggregate_descriptions(aggregate_descriptions_)
        , aggregate_context(aggregate_context_)
    {
        // The profile info of Aggregation is collected by PhysicalAggregationConvergent,
        // so calling notTiDBoPerator for PhysicalAggregationBuild to skip collecting profile info.
        notTiDBOperator();
    }

private:
    void buildPipelineExecGroupImpl(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        size_t /*concurrency*/) override;

    EventPtr doSinkComplete(PipelineExecutorContext & exec_context) override;

    DISABLE_USELESS_FUNCTION_FOR_BREAKER

private:
    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    KeyRefAggFuncMap key_ref_agg_func;
    AggFuncRefKeyMap agg_func_ref_key;
    bool is_final_agg;
    AggregateDescriptions aggregate_descriptions;
    AggregateContextPtr aggregate_context;

    OperatorProfileInfos profile_infos;
};
} // namespace DB
