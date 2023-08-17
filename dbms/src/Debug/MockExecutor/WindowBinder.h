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

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/FlashService.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

#include <memory>

namespace DB::mock
{
// This struct contains something that could help to build the range frame's tipb::Expr
struct BuildRangeFrameHelper
{
    ASTPtr range_aux_func;
    ContextPtr context;
};

class MockWindowFrameBound
{
public:
    // Constructor for non-range frame type
    MockWindowFrameBound(tipb::WindowBoundType bound_type_, bool is_unbounded_, UInt64 offset_)
        : bound_type(bound_type_)
        , is_unbounded(is_unbounded_)
        , offset(offset_)
    {}

    // Constructor for range frame type
    MockWindowFrameBound(
        tipb::WindowBoundType bound_type_,
        tipb::RangeCmpDataType cmp_data_type_,
        const BuildRangeFrameHelper & range_frame_helper_)
        : bound_type(bound_type_)
        , is_unbounded(false)
        , offset(0)
        , range_frame_helper(range_frame_helper_)
        , cmp_data_type(cmp_data_type_)
        , range_frame(nullptr)
    {}

    MockWindowFrameBound(MockWindowFrameBound && bound)
    {
        bound_type = bound.bound_type;
        is_unbounded = bound.is_unbounded;
        offset = bound.offset;
        range_frame_helper = bound.range_frame_helper;
        cmp_data_type = bound.cmp_data_type;
        range_frame = bound.range_frame;

        bound.offset = 0;
        bound.range_frame_helper.range_aux_func.reset();
        bound.range_frame_helper.context.reset();
        bound.range_frame = nullptr;
    }

    ~MockWindowFrameBound()
    {
        RUNTIME_ASSERT(range_frame == nullptr, "range_frame should not hold data when we are destructed");
    }

    MockWindowFrameBound(const MockWindowFrameBound &) = default;

    MockWindowFrameBound & operator=(const MockWindowFrameBound & bound) = default;

    tipb::WindowBoundType getBoundType() const { return bound_type; }
    bool isUnbounded() const { return is_unbounded; }
    UInt64 getOffset() const { return offset; }

    tipb::Expr * robRangeFrame()
    {
        auto * tmp = range_frame;
        range_frame = nullptr;
        return tmp;
    }

    tipb::RangeCmpDataType getCmpDataType() const { return cmp_data_type; }

    // This is a range frame type when range_frame_helper.range_aux_func is set.
    bool isRangeFrame() const { return static_cast<bool>(range_frame_helper.range_aux_func); }

    void buildRangeFrameAuxFunction(const DAGSchema & input)
    {
        range_frame = new tipb::Expr();
        auto * ast_func = typeid_cast<ASTFunction *>(range_frame_helper.range_aux_func.get());
        if (ast_func == nullptr)
        {
            delete range_frame;
            range_frame = nullptr;
            throw Exception("Building range frame needs ASTFunction");
        }

        // collator is useless when building range frame,
        // because range frame's order by column is forbidden to be string type
        functionToPB(input, ast_func, range_frame, 0, *range_frame_helper.context);
    }

private:
    tipb::WindowBoundType bound_type;
    bool is_unbounded; // true: unbounded, false: not unbounded
    UInt64 offset;

    BuildRangeFrameHelper range_frame_helper;
    tipb::RangeCmpDataType cmp_data_type; // only for range frame type

    // only for range frame type
    // Self class is responsible for creating and destroy this pointer
    tipb::Expr * range_frame = nullptr;
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
    WindowBinder(
        size_t & index_,
        const DAGSchema & output_schema_,
        ASTs && func_descs_,
        ASTs && partition_by_exprs_,
        ASTs && order_by_exprs_,
        MockWindowFrame && frame_,
        uint64_t fine_grained_shuffle_stream_count_ = 0)
        : ExecutorBinder(index_, "window_" + std::to_string(index_), output_schema_)
        , func_descs(std::move(func_descs_))
        , partition_by_exprs(std::move(partition_by_exprs_))
        , order_by_exprs(order_by_exprs_)
        , frame(std::move(frame_))
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    {}

    // Currently only use Window Executor in Unit Test which don't call columnPrume.
    // TODO: call columnPrune in unit test and further benchmark test to eliminate compute process.
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }

    bool toTiPBExecutor(
        tipb::Executor * tipb_executor,
        int32_t collator_id,
        const MPPInfo & mpp_info,
        const Context & context) override;

private:
    std::vector<ASTPtr> func_descs;
    std::vector<ASTPtr> partition_by_exprs;
    std::vector<ASTPtr> order_by_exprs;
    MockWindowFrame frame;
    uint64_t fine_grained_shuffle_stream_count;
};

ExecutorBinderPtr compileWindow(
    ExecutorBinderPtr input,
    size_t & executor_index,
    ASTPtr func_desc_list,
    ASTPtr partition_by_expr_list,
    ASTPtr order_by_expr_list,
    mock::MockWindowFrame frame,
    uint64_t fine_grained_shuffle_stream_count);
} // namespace DB::mock
