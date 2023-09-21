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

#include <Common/Decimal.h>
#include <Core/Field.h>
#include <Debug/MockExecutor/WindowBinder.h>
#include <Interpreters/Context.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/WindowTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <memory>
#include <optional>

namespace DB::tests
{
// TODO support unsigned int as the test framework not supports unsigned int so far.
class FirstValue : public DB::tests::WindowTest
{
public:
    const ASTPtr value_col = col(VALUE_COL_NAME);

    void initializeContext() override { ExecutorTest::initializeContext(); }

    template <typename IntType>
    void testInt()
    {
        executeFunctionAndAssert(
            toNullableVec<IntType>({1, 2, 2, 2, 2, 6, 6, 6, 6, 6, 11, 11, 11}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<IntType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})});

        executeFunctionAndAssert(
            toNullableVec<IntType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<IntType>(/*value*/ {{}, {}, 3, 4, 5, {}, 7, 8, 9, 10, {}, 12, 13})});
    }

    template <typename FloatType>
    void testFloat()
    {
        executeFunctionAndAssert(
            toNullableVec<FloatType>({1, 2, 2, 2, 2, 6, 6, 6, 6, 6, 11, 11, 11}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<FloatType>(/*value*/ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})});

        executeFunctionAndAssert(
            toNullableVec<FloatType>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<FloatType>(/*value*/ {{}, {}, 3, 4, 5, {}, 7, 8, 9, 10, {}, 12, 13})});
    }

    void testIntAndTimeOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        mock_frame.end = buildRangeFrameBound(
            tipb::WindowBoundType::Following,
            tipb::RangeCmpDataType::Int,
            ORDER_COL_NAME,
            true,
            static_cast<Int64>(0));

        {
            // Int type const column
            std::vector<Int64> frame_start_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 1, 8, 0, 0, 10, 10, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 1, 0, 0, 0, 3, 10, 1, 1, 1, 1, 5, 15, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            std::vector<tipb::RangeCmpDataType> cmp_data_type{
                tipb::RangeCmpDataType::Int,
                tipb::RangeCmpDataType::DateTime,
                tipb::RangeCmpDataType::Duration};

            for (auto type : cmp_data_type)
            {
                for (size_t i = 0; i < frame_start_range.size(); ++i)
                {
                    mock_frame.start = buildRangeFrameBound(
                        tipb::WindowBoundType::Preceding,
                        type,
                        ORDER_COL_NAME,
                        false,
                        frame_start_range[i]);
                    executeFunctionAndAssert(
                        toNullableVec<Int64>(res[i]),
                        FirstValue(value_col),
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
                {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 1, 0, 0, 3, 10, 10, 1, 1, 1, 1, 9, 15, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
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
                {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 1, 0, 0, 3, 10, 10, 1, 1, 1, 1, 9, 15, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Int64>(/*order*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Decimal,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }
    }

    void testFloatOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        mock_frame.end = buildRangeFrameBound(
            tipb::WindowBoundType::Following,
            tipb::RangeCmpDataType::Float,
            ORDER_COL_NAME,
            true,
            static_cast<Int64>(0));

        {
            // Int type const column
            std::vector<Int64> frame_start_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 1, 0, 0, 0, 3, 10, 1, 1, 1, 1, 5, 15, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.0, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // Float type const column
            std::vector<Float64> frame_start_range{0, 1.8, 2.3, 3.8};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 1, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 8, 0, 0, 10, 10, 13, 1, 1, 3, 9, 15, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.0, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        {
            // Decimal type const column
            std::vector<DecimalField<Decimal32>> frame_start_range{
                DecimalField<Decimal32>(0, 0),
                DecimalField<Decimal32>(18, 1),
                DecimalField<Decimal32>(23, 1),
                DecimalField<Decimal32>(38, 1)};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 4, 8, 0, 3, 10, 13, 15, 1, 1, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 8, 0, 0, 10, 10, 13, 1, 1, 3, 9, 15, 20, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = toVec<Float64>(
                /*order*/ {0.1, 1.0, 2.1, 4.1, 8.1, 0.0, 3.1, 10.0, 13.1, 15.1, 1.1, 2.9, 5.1, 9.1, 15.0, 20.1, 31.1});
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Float,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }
    }

    void testDecimalOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
        mock_frame.end = buildRangeFrameBound(
            tipb::WindowBoundType::Following,
            tipb::RangeCmpDataType::Decimal,
            ORDER_COL_NAME,
            true,
            static_cast<Int64>(0));

        {
            // Int type const column
            std::vector<Int64> frame_start_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31},
                {0, 1, 1, 2, 8, 0, 3, 10, 13, 13, 1, 1, 3, 9, 15, 20, 31},
                {0, 1, 1, 1, 1, 0, 0, 0, 3, 10, 1, 1, 1, 1, 5, 15, 31}};

            auto partition_col = toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3});
            auto order_col = createColumn<Decimal32>(/*order*/
                                                     std::make_tuple(3, 1),
                                                     {
                                                         DecimalField<Decimal32>(1, 1),
                                                         DecimalField<Decimal32>(10, 1),
                                                         DecimalField<Decimal32>(21, 1),
                                                         DecimalField<Decimal32>(41, 1),
                                                         DecimalField<Decimal32>(81, 1),
                                                         DecimalField<Decimal32>(0, 1),
                                                         DecimalField<Decimal32>(31, 1),
                                                         DecimalField<Decimal32>(100, 1),
                                                         DecimalField<Decimal32>(131, 1),
                                                         DecimalField<Decimal32>(151, 1),
                                                         DecimalField<Decimal32>(11, 1),
                                                         DecimalField<Decimal32>(29, 1),
                                                         DecimalField<Decimal32>(51, 1),
                                                         DecimalField<Decimal32>(91, 1),
                                                         DecimalField<Decimal32>(150, 1),
                                                         DecimalField<Decimal32>(201, 1),
                                                         DecimalField<Decimal32>(311, 1),
                                                     });
            auto val_col = toVec<Int64>(/*value*/ {0, 1, 2, 4, 8, 0, 3, 10, 13, 15, 1, 3, 5, 9, 15, 20, 31});

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Decimal,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
                    {partition_col, order_col, val_col},
                    mock_frame);
            }
        }

        // TODO Support Float type const column
        // TODO Support Decimal type const column
    }

    void testNullableOrderByColForRangeFrame()
    {
        MockWindowFrame mock_frame;
        mock_frame.type = tipb::WindowFrameType::Ranges;
        mock_frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);
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
            std::vector<Int64> frame_start_range{0, 1, 3, 10};
            std::vector<std::vector<std::optional<Int64>>> res{
                {1, 2, 3, 4, 5, 6, 7, 8, 9},
                {1, 2, 3, 3, 5, 6, 6, 8, 8},
                {1, 2, 3, 3, 5, 6, 6, 7, 8},
                {1, 2, 3, 3, 5, 6, 6, 6, 6}};

            for (size_t i = 0; i < frame_start_range.size(); ++i)
            {
                mock_frame.start = buildRangeFrameBound(
                    tipb::WindowBoundType::Preceding,
                    tipb::RangeCmpDataType::Int,
                    ORDER_COL_NAME,
                    false,
                    frame_start_range[i]);
                executeFunctionAndAssert(
                    toNullableVec<Int64>(res[i]),
                    FirstValue(value_col),
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
                FirstValue(value_col),
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
                FirstValue(value_col),
                {partition_col, order_col, val_col},
                mock_frame);
        }
    }
};

TEST_F(FirstValue, firstValueWithRowsFrameType)
try
{
    {
        // boundary type: unbounded
        executeFunctionAndAssert(
            toNullableVec<String>({"1", "2", "2", "2", "2", "6", "6", "6", "6", "6", "11", "11", "11"}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})});

        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
            FirstValue(value_col),
            {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toNullableVec<String>(/*value*/ {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"})});
    }

    {
        // boundary type: offset
        MockWindowFrame frame;
        frame.type = tipb::WindowFrameType::Rows;
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 0);

        std::vector<Int64> frame_start_offset{0, 1, 3, 10};
        std::vector<std::vector<std::optional<String>>> res_not_null{
            {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"},
            {"1", "2", "2", "3", "4", "6", "6", "7", "8", "9", "11", "11", "12"},
            {"1", "2", "2", "2", "2", "6", "6", "6", "6", "7", "11", "11", "11"},
            {"1", "2", "2", "2", "2", "6", "6", "6", "6", "6", "11", "11", "11"}};
        std::vector<std::vector<std::optional<String>>> res_null{
            {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"},
            {{}, {}, {}, "3", "4", {}, {}, "7", "8", "9", {}, {}, "12"},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, "7", {}, {}, {}},
            {{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}};

        for (size_t i = 0; i < frame_start_offset.size(); ++i)
        {
            frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, frame_start_offset[i]);

            executeFunctionAndAssert(
                toNullableVec<String>(res_not_null[i]),
                FirstValue(value_col),
                {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
                frame);

            executeFunctionAndAssert(
                toNullableVec<String>(res_null[i]),
                FirstValue(value_col),
                {toNullableVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
                 toNullableVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
                 toNullableVec<String>(/*value*/ {{}, {}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13"})},
                frame);
        }

        // The following are <preceding, preceding> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 2);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Preceding, false, 1);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, {}, "2", "2", "3", {}, "6", "6", "7", "8", {}, "11", "11"}),
            FirstValue(value_col),
            {toVec<Int64>(/*partition*/ {0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}),
             toVec<Int64>(/*order*/ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
             toVec<String>(/*value*/ {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13"})},
            frame);

        // The following are <following, folloing> tests
        frame.start = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 1);
        frame.end = mock::MockWindowFrameBound(tipb::WindowBoundType::Following, false, 2);
        executeFunctionAndAssert(
            toNullableVec<String>({{}, "3", "4", "5", {}, "7", "8", "9", "10", {}, "12", "13", {}}),
            FirstValue(value_col),
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
TEST_F(FirstValue, firstValueWithRangeFrameType)
try
{
    testIntAndTimeOrderByColForRangeFrame();
    testFloatOrderByColForRangeFrame();
    testNullableOrderByColForRangeFrame();

    // TODO This test is disabled as we can not assign decimal field's flen now
    // in MockStorage.cpp::mockColumnInfosToTiDBColumnInfos().
    // However, we will test this data type in fullstack tests.
    // testDecimalOrderByColForRangeFrame();
}
CATCH

} // namespace DB::tests
