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

namespace DB::mock
{
class SortBinder : public ExecutorBinder
{
public:
    SortBinder(
        size_t & index_,
        const DAGSchema & output_schema_,
        ASTs && by_exprs_,
        bool is_partial_sort_,
        uint64_t fine_grained_shuffle_stream_count_ = 0)
        : ExecutorBinder(index_, "sort_" + std::to_string(index_), output_schema_)
        , by_exprs(by_exprs_)
        , is_partial_sort(is_partial_sort_)
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    {}

    // Currently only use Sort Executor in Unit Test which don't call columnPrume.
    // TODO: call columnPrune in unit test and further benchmark test to eliminate compute process.
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }

    bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context) override;

private:
    std::vector<ASTPtr> by_exprs;
    bool is_partial_sort;
    uint64_t fine_grained_shuffle_stream_count;
};

ExecutorBinderPtr compileSort(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr order_by_expr_list,
    bool is_partial_sort,
    uint64_t fine_grained_shuffle_stream_count);
} // namespace DB::mock
