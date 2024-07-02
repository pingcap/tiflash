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

#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Parsers/ASTFunction.h>

namespace DB::mock
{
class ExchangeSenderBinder;
class ExchangeReceiverBinder;

class AggregationBinder : public ExecutorBinder
{
public:
    AggregationBinder(
        size_t & index_,
        const DAGSchema & output_schema_,
        bool has_uniq_raw_res_,
        bool need_append_project_,
        ASTs && agg_exprs_,
        ASTs && gby_exprs_,
        bool is_final_mode_,
        uint64_t fine_grained_shuffle_stream_count_,
        bool auto_pass_through_)
        : ExecutorBinder(index_, "aggregation_" + std::to_string(index_), output_schema_)
        , has_uniq_raw_res(has_uniq_raw_res_)
        , need_append_project(need_append_project_)
        , agg_exprs(std::move(agg_exprs_))
        , gby_exprs(std::move(gby_exprs_))
        , is_final_mode(is_final_mode_)
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
        , auto_pass_through(auto_pass_through_)
    {}

    bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context) override;

    void columnPrune(std::unordered_set<String> & used_columns) override;

    void toMPPSubPlan(
        size_t & executor_index,
        const DAGProperties & properties,
        std::unordered_map<
            String,
            std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
        override;

    bool needAppendProject() const;

    size_t exprSize() const;

    bool hasUniqRawRes() const;

protected:
    bool has_uniq_raw_res;
    bool need_append_project;
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    bool is_final_mode;
    DAGSchema output_schema_for_partial_agg;
    uint64_t fine_grained_shuffle_stream_count;
    bool auto_pass_through;

private:
    void buildGroupBy(tipb::Aggregation * agg, int32_t collator_id, const Context & context) const;
    void buildAggExpr(tipb::Aggregation * agg, int32_t collator_id, const Context & context) const;
    void buildAggFunc(tipb::Expr * agg_func, const ASTFunction * func, int32_t collator_id) const;
};

ExecutorBinderPtr compileAggregation(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr agg_funcs,
    ASTPtr group_by_exprs,
    uint64_t fine_grained_shuffle_stream_count = 0,
    bool auto_pass_through = false);

} // namespace DB::mock
