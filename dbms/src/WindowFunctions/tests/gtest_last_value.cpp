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

#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <tipb/executor.pb.h>

#include <optional>
#include <utility>


namespace DB::tests
{
// TODO support unsigned int as the test framework not supports unsigned int so far.
class LastValue : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

    template <typename IntType>
    void testInt()
    {
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, true, 0),
            mock::MockWindowFrameBound(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toNullableVec<IntType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<IntType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})},
            unbounded_type_frame);

        executeFunctionAndAssert(
            toNullableVec<IntType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<IntType>(/*value*/ {{}, 2, 3, 4, {}, 6, 7, 8, 9, {}, 11, 12, {}})},
            unbounded_type_frame);
    }

    template <typename FloatType>
    void testFloat()
    {
        MockWindowFrame unbounded_type_frame{
            tipb::WindowFrameType::Rows,
            mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, true, 0),
            mock::MockWindowFrameBound(tipb::WindowBoundType::Following, true, 0)};

        executeFunctionAndAssert(
            toNullableVec<FloatType>({1, 5, 5, 5, 5, 10, 10, 10, 10, 10, 13, 13, 13}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<FloatType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})},
            unbounded_type_frame);

        executeFunctionAndAssert(
            toNullableVec<FloatType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<FloatType>(/*value*/ {{}, 2, 3, 4, {}, 6, 7, 8, 9, {}, 11, 12, {}})},
            unbounded_type_frame);
    }

    void testIntAndTimeOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = buildRangeFrameBound(
            tipb::WindowBoundType::Preceding,
            tipb::RangeCmpDataType::Float,
            ORDER_COL_NAME,
            false,
            static_cast<Int64>(0));
        mock_frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);

        {
            // Int type const column
            std::vector<Int64> frame_end_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 4, 4, 4, 8, 3, 3, 13, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 8, 8, 8, 8, 10, 13, 15, 15, 15, 9, 9, 15, 15, 20, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            std::vector<tipb::RangeCmpDataType> cmp_data_type{
                tipb::RangeCmpDataType::Int,
                tipb::RangeCmpDataType::DateTime,
                tipb::RangeCmpDataType::Duration};

            for (auto type : cmp_data_type)
            {
                for (size_t i = 0; i < frame_end_range.size(); ++i)
                {
                    mock_frame.end = buildRangeFrameBound(
                        tipb::WindowBoundType::Following,
                        type,
                        ORDER_COL_NAME,
                        true,
                        frame_end_range[i]);
                    executeFunctionAndAssert(
                        toNullableVec<Int64>(res[i]),
                        LastValue(value_col),
                        {partition_col, order_col, val_col},
                        mock_frame);
                }
            }
        }

        {
            // Float type const column
            std::vector<Float64> frame_start_range{0, 1.1, 2.9, 9.9};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 4, 4, 8, 0, 3, 10, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 8, 8, 8, 8, 3, 10, 15, 15, 15, 9, 9, 9, 15, 20, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    true,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // Decimal type const column
            std::vector<DecimalField<Decimal32>> frame_start_range{
                DecimalField<Decimal32>(0, 0),
                DecimalField<Decimal32>(11, 1),
                DecimalField<Decimal32>(29, 1),
                DecimalField<Decimal32>(99, 1)};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 4, 4, 8, 0, 3, 10, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 8, 8, 8, 8, 3, 10, 15, 15, 15, 9, 9, 9, 15, 20, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Decimal,
                    ORDER_COL_NAME,
                    true,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }
    }

    void testFloatOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = buildRangeFrameBound(
            tipb::WindowBoundType::Preceding,
            tipb::RangeCmpDataType::Float,
            ORDER_COL_NAME,
            false,
            static_cast<Int64>(0));
        mock_frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, static_cast<Int64>(0));

        {
            // Int type const column
            std::vector<Int64> frame_start_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 4, 4, 4, 8, 0, 3, 10, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 8, 8, 8, 8, 10, 13, 15, 15, 15, 9, 9, 15, 15, 20, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.2, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    true,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // Float type const column
            std::vector<Float64> frame_start_range{0, 1.1, 2.3, 3.8};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 4, 4, 8, 0, 3, 10, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 4, 4, 4, 8, 3, 3, 13, 15, 15, 3, 5, 5, 9, 15, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.2, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    true,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // Decimal type const column
            std::vector<DecimalField<Decimal32>> frame_start_range{
                DecimalField<Decimal32>(0, 0),
                DecimalField<Decimal32>(11, 1),
                DecimalField<Decimal32>(23, 1),
                DecimalField<Decimal32>(38, 1)};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 2, 4, 4, 8, 0, 3, 10, 15, 15, 3, 5, 5, 9, 15, 20, 31},
                {0, 4, 4, 4, 8, 3, 3, 13, 15, 15, 3, 5, 5, 9, 15, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.2, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    true,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }
    }

    void testNullableOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = buildRangeFrameBound(
            tipb::WindowBoundType::Preceding,
            tipb::RangeCmpDataType::Int,
            ORDER_COL_NAME,
            false,
            static_cast<Int64>(0));
        ;
        mock_frame.end = buildRangeFrameBound(
            tipb::WindowBoundType::Following,
            tipb::RangeCmpDataType::Int,
            ORDER_COL_NAME,
            true,
            static_cast<Int64>(0));

        auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 2, 2, 2, 2, 2});
        auto order_col = toNullableVec<Int64>(/*order*/ {0, {}, 1, 2, {}, 5, 6, 9, 10});
        auto val_col = toVec<Int64>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9});

        {
            std::vector<Int64> frame_end_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {1, 2, 3, 4, 5, 6, 7, 8, 9},
                {1, 2, 4, 4, 5, 7, 7, 9, 9},
                {1, 2, 4, 4, 5, 7, 8, 9, 9},
                {1, 2, 4, 4, 5, 9, 9, 9, 9}};

            for (size_t i = 0; i < frame_end_range.size(); ++i)
            {
                mock_frame.end = buildRangeFrameBound(
                    tipb::WindowBoundType::Following,
                    tipb::RangeCmpDataType::Int,
                    ORDER_COL_NAME,
                    true,
                    frame_end_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    LastValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // <preceding, preceding>
            mock_frame.start = buildRangeFrameBound(
                tipb::WindowBoundType::Preceding,
                tipb::RangeCmpDataType::Float,
                ORDER_COL_NAME,
                false,
                static_cast<Int64>(1));
            mock_frame.end = buildRangeFrameBound(
                tipb::WindowBoundType::Preceding,
                tipb::RangeCmpDataType::Float,
                ORDER_COL_NAME,
                false,
                static_cast<Int64>(1));

            executeFunctionAndAssert(
                toNullableVec<Int64>({{}, 2, {}, 3, 5, {}, 6, {}, 8}),
                LastValue(value_col),
                {partition_col, order_col, val_col},
                mock_frame);
        }

        {
            // <following, following>
            mock_frame.start = buildRangeFrameBound(
                tipb::WindowBoundType::Following,
                tipb::RangeCmpDataType::Float,
                ORDER_COL_NAME,
                true,
                static_cast<Int64>(1));
            mock_frame.end = buildRangeFrameBound(
                tipb::WindowBoundType::Following,
                tipb::RangeCmpDataType::Float,
                ORDER_COL_NAME,
                true,
                static_cast<Int64>(1));

            executeFunctionAndAssert(
                toNullableVec<Int64>({{}, 2, 4, {}, 5, 7, {}, 9, {}}),
                LastValue(value_col),
                {partition_col, order_col, val_col},
                mock_frame);
        }
    }

    void testDecimalOrderByColForRangeFrame()
    {
        // TODO we can not assign decimal field's flen now
        // in MockStorage.cpp::mockColumnInfosToTiDBColumnInfos().
        // However, we will test this data type in fullstack tests.
    }
};

TEST_F(LastValue, lastValue)
try
{
    {
        // frame type: unbounded
        executeFunctionAndAssert(
            toNullableVec<String>({"1", "5", "5", "5", "5", "10", "10", "10", "10", "10", "13", "13", "13"}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})});

        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            LastValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<String>(/*value*/ {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}})});
    }

    {
        // frame type: offset
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 0);

        std::vector<Int64> frame_start_offset{0, 1, 3, 10};
        std::vector<std::vector<std::optional<String>>> res_not_null{
            {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"},
            {"1", "3", "4", "5", "5", "7", "8", "9", "10", "10", "12", "13", "13"},
            {"1", "5", "5", "5", "5", "9", "10", "10", "10", "10", "13", "13", "13"},
            {"1", "5", "5", "5", "5", "10", "10", "10", "10", "10", "13", "13", "13"},
        };
        std::vector<std::vector<std::optional<String>>> res_null{
            {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}},
            {{}, "3", "4", {}, {}, "7", "8", "9", {}, {}, "12", {}, {}},
            {{}, {}, {}, {}, {}, "9", {}, {}, {}, {}, {}, {}, {}},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}},
        };

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, frame_start_offset[i]);
            executeFunctionAndAssert(
                toNullableVec<String>(res_not_null[i]),
                LastValue(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
                frame);

            executeFunctionAndAssert(
                toNullableVec<String>(res_null[i]),
                LastValue(value_col),
                {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toNullableVec<String>(/*value*/ {{}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12", {}})},
                frame);
        }

        // The following are <preceding, preceding> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 2);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 1);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, "2", "3", "4", {}, "6", "7", "8", "9", {}, "11", "12"}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
            frame);

        // The following are <following, folloing> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 1);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 2);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, "4", "5", "5", {}, "8", "9", "10", "10", {}, "13", "13", {}}),
            LastValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
            frame);
    }

    testInt<Int8>();
    testInt<Int16>();
    testInt<Int32>();
    testInt<Int64>();

    testFloat<Float32>();
    testFloat<Float64>();
}
CATCH

// This is the test just for testing range type frame.
// Not every window function needs this test.
TEST_F(LastValue, lastValueWithRangeFrameType)
try
{
    testIntAndTimeOrderByColForRangeFrame();
    testFloatOrderByColForRangeFrame();
    testNullableOrderByColForRangeFrame();
    // TODO Implement testDecimalOrderByColForRangeFrame()
}
CATCH

} // namespace DB::tests
