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
#include <Debug/MockRuntimeFilter.h>

namespace DB::mock
{
class ExchangeSenderBinder;
class ExchangeReceiverBinder;
class JoinBinder : public ExecutorBinder
{
public:
    JoinBinder(
        size_t & index_,
        const DAGSchema & output_schema_,
        tipb::JoinType tp_,
        const ASTs & join_cols_,
        const ASTs & l_conds,
        const ASTs & r_conds,
        const ASTs & o_conds,
        const ASTs & o_eq_conds,
        uint64_t fine_grained_shuffle_stream_count_,
        bool is_null_aware_semi_join,
        int64_t inner_index_ = 1)
        : ExecutorBinder(index_, "Join_" + std::to_string(index_), output_schema_)
        , tp(tp_)
        , join_cols(join_cols_)
        , left_conds(l_conds)
        , right_conds(r_conds)
        , other_conds(o_conds)
        , other_eq_conds_from_in(o_eq_conds)
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
        , is_null_aware_semi_join(is_null_aware_semi_join)
        , inner_index(inner_index_)
    {}

    void addRuntimeFilter(MockRuntimeFilter & rf);

    void columnPrune(std::unordered_set<String> & used_columns) override;

    static void fillJoinKeyAndFieldType(
        ASTPtr key,
        const DAGSchema & schema,
        tipb::Expr * tipb_key,
        tipb::FieldType * tipb_field_type,
        int32_t collator_id);

    bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context) override;

    void toMPPSubPlan(
        size_t & executor_index,
        const DAGProperties & properties,
        std::unordered_map<
            String,
            std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> & exchange_map)
        override;

protected:
    tipb::JoinType tp;

    const ASTs join_cols{};
    const ASTs left_conds{};
    const ASTs right_conds{};
    const ASTs other_conds{};
    const ASTs other_eq_conds_from_in{};
    uint64_t fine_grained_shuffle_stream_count;
    bool is_null_aware_semi_join;
    int64_t inner_index;
    std::vector<std::shared_ptr<MockRuntimeFilter>> rf_list;
};
// compileJoin constructs a mocked Join executor node, note that all conditional expression params can be default
ExecutorBinderPtr compileJoin(
    size_t & executor_index,
    ExecutorBinderPtr left,
    ExecutorBinderPtr right,
    tipb::JoinType tp,
    const ASTs & join_cols,
    const ASTs & left_conds = {},
    const ASTs & right_conds = {},
    const ASTs & other_conds = {},
    const ASTs & other_eq_conds_from_in = {},
    uint64_t fine_grained_shuffle_stream_count = 0,
    bool is_null_aware_semi_join = false,
    int64_t inner_index = 1);


/// Note: this api is only used by legacy test framework for compatibility purpose, which will be depracated soon,
/// so please avoid using it.
/// Old executor test framework bases on ch's parser to translate sql string to ast tree, then manually to DAGRequest.
/// However, as for join executor, this translation, from ASTTableJoin to tipb::Join, is not a one-to-one mapping
/// because of the different join classification model used by these two structures. Therefore, under old test framework,
/// it is hard to fully test join executor. New framework aims to directly construct DAGRequest, so new framework APIs for join should
/// avoid using ASTTableJoin.
ExecutorBinderPtr compileJoin(size_t & executor_index, ExecutorBinderPtr left, ExecutorBinderPtr right, ASTPtr params);
} // namespace DB::mock
