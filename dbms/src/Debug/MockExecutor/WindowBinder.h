// Copyright 2022 PingCAP, Ltd.
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
#include <Parsers/ASTOrderByElement.h>

#include "tipb/executor.pb.h"

namespace DB::mock
{
class MockWindowFrameBound
{
public:
    // Constructor for non-range frame type
    MockWindowFrameBound(const tipb::WindowBoundType & bound_type_, bool is_unbounded_, UInt64 offset_)
        : bound_type(bound_type_)
        , is_unbounded(is_unbounded_)
        , offset(offset_)
    {}

    // Constructor for range frame type
    MockWindowFrameBound(
        const tipb::WindowBoundType & bound_type_,
        const tipb::Expr & range_frame_,
        const tipb::RangeCmpDataType & cmp_data_type_)
        : bound_type(bound_type_)
        , is_unbounded(false)
        , offset(0)
        , range_frame(range_frame_)
        , cmp_data_type(cmp_data_type_)
    {}

    tipb::WindowBoundType getBoundType() const { return bound_type; }
    bool isUnbounded() const { return is_unbounded; }
    UInt64 getOffset() const { return offset; }
    tipb::Expr getRangeFrame() const { return range_frame; }
    tipb::RangeCmpDataType getCmpDataType() const { return cmp_data_type; }

private:
    tipb::WindowBoundType bound_type;
    bool is_unbounded; // true: unbounded, false: not unbounded
    UInt64 offset;
    tipb::Expr range_frame; // only for range frame type
    tipb::RangeCmpDataType cmp_data_type; // only for range frame type
};

struct MockWindowFrame
{
    std::optional<tipb::WindowFrameType> type;
    std::optional<MockWindowFrameBound> start;
    std::optional<MockWindowFrameBound> end;
};

using ASTPartitionByElement = ASTOrderByElement;

class WindowBinder : public ExecutorBinder
{
public:
    WindowBinder(size_t & index_, const DAGSchema & output_schema_, ASTs && func_descs_, ASTs && partition_by_exprs_, ASTs && order_by_exprs_, MockWindowFrame frame_, uint64_t fine_grained_shuffle_stream_count_ = 0)
        : ExecutorBinder(index_, "window_" + std::to_string(index_), output_schema_)
        , func_descs(std::move(func_descs_))
        , partition_by_exprs(std::move(partition_by_exprs_))
        , order_by_exprs(order_by_exprs_)
        , frame(frame_)
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    {}

    // Currently only use Window Executor in Unit Test which don't call columnPrume.
    // TODO: call columnPrune in unit test and further benchmark test to eliminate compute process.
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }

    bool toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;

private:
    std::vector<ASTPtr> func_descs;
    std::vector<ASTPtr> partition_by_exprs;
    std::vector<ASTPtr> order_by_exprs;
    MockWindowFrame frame;
    uint64_t fine_grained_shuffle_stream_count;
};

ExecutorBinderPtr compileWindow(ExecutorBinderPtr input, size_t & executor_index, ASTPtr func_desc_list, ASTPtr partition_by_expr_list, ASTPtr order_by_expr_list, mock::MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count);
} // namespace DB::mock
