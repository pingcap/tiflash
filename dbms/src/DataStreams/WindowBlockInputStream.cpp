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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/WindowUtils.h>
#include <common/UInt128.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <magic_enum.hpp>
#include <tuple>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace
{
template <typename T>
consteval bool checkIfSimpleNumericType()
{
    return std::is_integral_v<T> || std::is_floating_point_v<T>;
}

template <typename T>
consteval bool checkIfDecimalFieldType()
{
    return std::is_same_v<T, DecimalField<Decimal32>> || std::is_same_v<T, DecimalField<Decimal64>>
        || std::is_same_v<T, DecimalField<Decimal128>> || std::is_same_v<T, DecimalField<Decimal256>>;
}

template <typename LeftType, typename RightType>
bool lessEqual(LeftType left, RightType right)
{
    if constexpr (checkIfDecimalFieldType<LeftType>() && checkIfDecimalFieldType<RightType>())
    {
        return left <= right;
    }
    else if constexpr (checkIfDecimalFieldType<LeftType>())
    {
        return DecimalComparison<typename LeftType::DecimalType, RightType, LessOrEqualsOp>::compare(
            left.getValue(),
            right,
            left.getScale(),
            0);
    }
    else if constexpr (checkIfDecimalFieldType<RightType>())
    {
        return DecimalComparison<LeftType, typename RightType::DecimalType, LessOrEqualsOp>::compare(
            left,
            right.getValue(),
            0,
            right.getScale());
    }
    else
    {
        return left <= right;
    }
}

template <typename LeftType, typename RightType>
bool greaterEqual(LeftType left, RightType right)
{
    if constexpr (checkIfDecimalFieldType<LeftType>() && checkIfDecimalFieldType<RightType>())
    {
        return left >= right;
    }
    else if constexpr (checkIfDecimalFieldType<LeftType>())
    {
        return DecimalComparison<typename LeftType::DecimalType, RightType, GreaterOrEqualsOp>::compare(
            left.getValue(),
            right,
            left.getScale(),
            0);
    }
    else if constexpr (checkIfDecimalFieldType<RightType>())
    {
        return DecimalComparison<LeftType, typename RightType::DecimalType, GreaterOrEqualsOp>::compare(
            left,
            right.getValue(),
            0,
            right.getScale());
    }
    else
    {
        return left >= right;
    }
}

// When T is Decimal, we should convert it to DecimalField type
// as we need scale value when executing the comparison operation.
template <typename T>
struct ActualCmpDataType
{
    using Type = std::conditional_t<checkIfSimpleNumericType<T>(), T, DecimalField<T>>;
};

template <typename T>
typename ActualCmpDataType<T>::Type getValue(const ColumnPtr & col_ptr, size_t idx)
{
    return (*col_ptr)[idx].get<typename ActualCmpDataType<T>::Type>();
}

template <typename T, typename U, bool is_preceding, bool is_desc, bool is_begin>
bool isInRangeCommonImpl(T current_row_aux_value, U cursor_value)
{
    if constexpr (is_begin)
    {
        if constexpr (is_desc)
            return lessEqual(cursor_value, current_row_aux_value);
        else
            return greaterEqual(cursor_value, current_row_aux_value);
    }
    else
    {
        if constexpr (!is_desc)
            return lessEqual(cursor_value, current_row_aux_value);
        else
            return greaterEqual(cursor_value, current_row_aux_value);
    }
}

template <typename T, typename U, bool is_preceding, bool is_desc, bool is_begin>
bool isInRangeIntImpl(T current_row_aux_value, U cursor_value)
{
    return isInRangeCommonImpl<T, U, is_preceding, is_desc, is_begin>(current_row_aux_value, cursor_value);
}

template <typename AuxColType, typename OrderByColType, bool is_preceding, bool is_desc, bool is_begin>
bool isInRangeDecimalImpl(AuxColType current_row_aux_value, OrderByColType cursor_value)
{
    return isInRangeCommonImpl<AuxColType, OrderByColType, is_preceding, is_desc, is_begin>(
        current_row_aux_value,
        cursor_value);
}

template <typename AuxColType, typename OrderByColType, bool is_preceding, bool is_desc, bool is_begin>
bool isInRangeFloatImpl(AuxColType current_row_aux_value, OrderByColType cursor_value)
{
    Float64 current_row_aux_value_float64;
    Float64 cursor_value_float64;

    if constexpr (checkIfDecimalFieldType<AuxColType>())
        current_row_aux_value_float64
            = current_row_aux_value.getValue().template toFloat<Float64>(current_row_aux_value.getScale());
    else
        current_row_aux_value_float64 = static_cast<Float64>(current_row_aux_value);

    if constexpr (checkIfDecimalFieldType<OrderByColType>())
        cursor_value_float64 = cursor_value.getValue().template toFloat<Float64>(cursor_value.getScale());
    else
        cursor_value_float64 = static_cast<Float64>(cursor_value);

    return isInRangeCommonImpl<Float64, Float64, is_preceding, is_desc, is_begin>(
        current_row_aux_value_float64,
        cursor_value_float64);
}

template <typename AuxColType, typename OrderByColType, int CmpDataType, bool is_preceding, bool is_desc, bool is_begin>
bool isInRange(AuxColType current_row_aux_value, OrderByColType cursor_value)
{
    if constexpr (
        CmpDataType == tipb::RangeCmpDataType::Int || CmpDataType == tipb::RangeCmpDataType::DateTime
        || CmpDataType == tipb::RangeCmpDataType::Duration)
    {
        // Two operand must be integer
        if constexpr (std::is_integral_v<OrderByColType> && std::is_integral_v<AuxColType>)
        {
            if constexpr (std::is_unsigned_v<OrderByColType> && std::is_unsigned_v<AuxColType>)
                return isInRangeIntImpl<UInt64, UInt64, is_preceding, is_desc, is_begin>(
                    current_row_aux_value,
                    cursor_value);
            return isInRangeIntImpl<Int64, Int64, is_preceding, is_desc, is_begin>(current_row_aux_value, cursor_value);
        }
        else
            throw Exception("Unexpected Data Type!");
    }
    else if constexpr (CmpDataType == tipb::RangeCmpDataType::Float)
    {
        return isInRangeFloatImpl<AuxColType, OrderByColType, is_preceding, is_desc, is_begin>(
            current_row_aux_value,
            cursor_value);
    }
    else
    {
        if constexpr (std::is_floating_point_v<OrderByColType> || std::is_floating_point_v<AuxColType>)
            throw Exception("Occurrence of float type at here is unexpected!");
        else if constexpr (!checkIfDecimalFieldType<AuxColType>() && !checkIfDecimalFieldType<OrderByColType>())
            throw Exception("At least one Decimal type is required");
        else
            return isInRangeDecimalImpl<AuxColType, OrderByColType, is_preceding, is_desc, is_begin>(
                current_row_aux_value,
                cursor_value);
    }
}

template <bool is_begin>
RowNumber getBoundary(const WindowTransformAction & action)
{
    if constexpr (is_begin)
    {
        if (action.window_description.frame.begin_preceding)
            return action.current_row;
        else
            return action.partition_end;
    }
    else
    {
        if (action.window_description.frame.end_preceding)
            return action.current_row;
        else
            return action.partition_end;
    }
}
} // namespace

WindowTransformAction::WindowTransformAction(
    const Block & input_header,
    const WindowDescription & window_description_,
    const String & req_id)
    : log(Logger::get(req_id))
    , window_description(window_description_)
{
    output_header = input_header;
    for (const auto & add_column : window_description_.add_columns)
    {
        output_header.insert({add_column.type, add_column.name});
    }

    initialWorkspaces();

    initialPartitionAndOrderColumnIndices();
}

void WindowTransformAction::cleanUp()
{
    if (!window_blocks.empty())
        window_blocks.erase(window_blocks.begin(), window_blocks.end());
    input_is_finished = true;
}

WindowBlockInputStream::WindowBlockInputStream(
    const BlockInputStreamPtr & input,
    const WindowDescription & window_description_,
    const String & req_id)
    : action(input->getHeader(), window_description_, req_id)
{
    children.push_back(input);
}

void WindowTransformAction::initialPartitionAndOrderColumnIndices()
{
    partition_column_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_column_indices.push_back(output_header.getPositionByName(column.column_name));
    }

    order_column_indices.reserve(window_description.order_by.size());
    for (const auto & column : window_description.order_by)
    {
        order_column_indices.push_back(output_header.getPositionByName(column.column_name));
    }
}

void WindowTransformAction::initialWorkspaces()
{
    // Initialize window function workspaces.
    workspaces.reserve(window_description.window_functions_descriptions.size());

    for (const auto & window_function_description : window_description.window_functions_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = window_function_description.window_function;
        workspace.arguments = window_function_description.arguments;
        workspaces.push_back(std::move(workspace));
    }
    only_have_row_number = onlyHaveRowNumber();
}

bool WindowBlockInputStream::returnIfCancelledOrKilled()
{
    if (isCancelledOrThrowIfKilled())
    {
        action.cleanUp();
        return true;
    }
    return false;
}

Block WindowBlockInputStream::readImpl()
{
    // first try to get result without reading from children
    auto block = action.tryGetOutputBlock();
    if (block || action.input_is_finished)
        return block;

    // then if input is not finished, keep reading input until one result block is generated
    if (!action.input_is_finished)
    {
        const auto & stream = children.back();
        while (!action.input_is_finished)
        {
            if (returnIfCancelledOrKilled())
                return {};

            Block block = stream->read();
            if (!block)
                action.input_is_finished = true;
            else
                action.appendBlock(block);
            if (auto block = action.tryGetOutputBlock())
                return block;
        }
    }
    return {};
}

// Judge whether current_partition_row is end row of partition in current block
// How to judge?
// Compare data in previous partition with the new scanned data.
bool WindowTransformAction::isDifferentFromPrevPartition(UInt64 current_partition_row)
{
    // prev_frame_start refers to the data in previous partition
    const Columns & reference_columns = inputAt(prev_frame_start);

    // partition_end refers to the new scanned data
    const Columns & compared_columns = inputAt(partition_end);

    for (size_t i = 0; i < partition_column_indices.size(); ++i)
    {
        const ColumnPtr & reference_column = reference_columns[partition_column_indices[i]];
        const ColumnPtr & compared_column = compared_columns[partition_column_indices[i]];

        if (window_description.partition_by[i].collator)
        {
            if (compared_column->compareAt(
                    current_partition_row,
                    prev_frame_start.row,
                    *reference_column,
                    1 /* nan_direction_hint */,
                    *window_description.partition_by[i].collator)
                != 0)
            {
                return true;
            }
        }
        else
        {
            if (compared_column->compareAt(
                    current_partition_row,
                    prev_frame_start.row,
                    *reference_column,
                    1 /* nan_direction_hint */)
                != 0)
            {
                return true;
            }
        }
    }
    return false;
}

void WindowTransformAction::advancePartitionEnd()
{
    // if partition_ended is true, we don't need to advance partition_end
    if (partition_ended)
        return;
    const RowNumber end = blocksEnd();

    // If we're at the total end of data, we must end the partition. This is one
    // of the few places in calculations where we need special handling for end
    // of data, other places will work as usual based on
    // `partition_ended` = true, because end of data is logically the same as
    // any other end of partition.
    // We must check this first, because other calculations might not be valid
    // when we're at the end of data.
    if (input_is_finished)
    {
        partition_ended = true;
        // We receive empty chunk at the end of data, so the partition_end must
        // be already at the end of data.
        assert(partition_end == end);
        return;
    }

    // If we got to the end of the block already, but we are going to get more
    // input data, wait for it.
    if (partition_end == end)
    {
        return;
    }

    // We process one block at a time, but we can process each block many times,
    // if it contains multiple partitions. The `partition_end` is a
    // past-the-end pointer, so it must be already in the "next" block we haven't
    // processed yet. This is also the last block we have.
    // The exception to this rule is end of data, for which we checked above.
    assert(end.block == partition_end.block + 1);

    // Try to advance the partition end pointer.
    const size_t partition_by_columns = partition_column_indices.size();
    if (partition_by_columns == 0)
    {
        // No PARTITION BY. All input is one partition, which will end when the
        // input ends.
        partition_end = end;
        return;
    }

    // Check for partition end.
    // The partition ends when the PARTITION BY columns change. We need
    // some reference columns for comparison. We might have already
    // dropped the blocks where the partition starts, but any other row in the
    // partition will do. We can't use frame_start or frame_end or current_row (the next row
    // for which we are calculating the window functions), because they all might be
    // past the end of the partition. prev_frame_start is suitable, because it
    // is a pointer to the first row of the previous frame that must have been
    // valid, or to the first row of the partition, and we make sure not to drop
    // its block.
    assert(partition_start <= prev_frame_start);
    // The frame start should be inside the prospective partition, except the
    // case when it still has no rows.
    assert(prev_frame_start < partition_end || partition_start == partition_end);
    assert(first_block_number <= prev_frame_start.block);
    const auto block_rows = blockRowsNumber(partition_end);

    // if the last partition row of block is same as prev, there should be no partition end in this block
    if (isDifferentFromPrevPartition(block_rows - 1))
    {
        partition_end.row = getPartitionEndRow(block_rows);
        partition_ended = true;
        return;
    }

    // go to the next.
    ++partition_end.block;
    partition_end.row = 0;

    // Went until the end of data and didn't find the new partition.
    assert(!partition_ended && partition_end == blocksEnd());
}
Int64 WindowTransformAction::getPartitionEndRow(size_t block_rows)
{
    Int64 left = partition_end.row;
    Int64 right = block_rows - 1;

    // Compare two times first.
    // It will speed up the case that the partition end is very close.
    Int64 end = std::min(left + 1, right);
    for (; left <= end; ++left)
    {
        if (isDifferentFromPrevPartition(left))
        {
            return left;
        }
    }

    while (left <= right)
    {
        Int64 middle = (left + right) >> 1;
        if (isDifferentFromPrevPartition(middle))
        {
            right = middle - 1;
        }
        else
        {
            left = middle + 1;
        }
    }
    return left;
}

// When finding frame start with Following attribute, partition end
// may haven't appeared and we can't find frame start in this case.
// Returning false in the tuple's second parameter means the failure
// of finding frame start.
std::tuple<RowNumber, bool> WindowTransformAction::stepToStartForRowsFrame(
    const RowNumber & current_row,
    const WindowFrame & frame)
{
    auto step_num = frame.begin_offset;
    if (window_description.frame.begin_preceding)
        return std::make_tuple(stepInPreceding(current_row, step_num), true);
    else
        return stepInFollowing(current_row, step_num);
}

std::tuple<RowNumber, bool> WindowTransformAction::stepToEndForRowsFrame(
    const RowNumber & current_row,
    const WindowFrame & frame)
{
    if (window_description.frame.end_preceding)
    {
        if (frame.end_offset == 0)
        {
            RowNumber frame_end_tmp = current_row;
            advanceRowNumber(frame_end_tmp);
            return std::make_tuple(frame_end_tmp, true);
        }

        // Range of rows is [frame_start, frame_end),
        // and frame_end position is behind the position of the last frame row.
        // So we need to -1
        return std::make_tuple(stepInPreceding(current_row, frame.end_offset - 1), true);
    }
    else
    {
        // Range of rows is [frame_start, frame_end),
        // and frame_end position is behind the position of the last frame row.
        // So we need to +1
        return stepInFollowing(current_row, frame.end_offset + 1);
    }
}

RowNumber WindowTransformAction::stepInPreceding(const RowNumber & moved_row, size_t step_num)
{
    RowNumber result_row = moved_row;
    while (step_num > 0 && (prev_frame_start < result_row))
    {
        // The step happens only in a block
        if (result_row.row >= step_num)
        {
            result_row.row -= step_num;
            break;
        }

        // The step happens between blocks
        step_num -= result_row.row + 1;
        if (result_row.block == 0)
        {
            result_row.row = 0;
            break;
        }
        --result_row.block;

        // We need to break the while loop when prev_frame_start.block > result_row.block
        // as the result_row.block may have been released and the calling for blockAt(result_row)
        // will trigger the assert.
        if (prev_frame_start.block > result_row.block)
            break;
        result_row.row = blockAt(result_row).rows - 1;
    }

    // prev_frame_start is the farthest position we can reach to.
    return result_row < prev_frame_start ? prev_frame_start : result_row;
}

std::tuple<RowNumber, bool> WindowTransformAction::stepInFollowing(const RowNumber & moved_row, size_t step_num)
{
    if (!partition_ended)
        // If we find the frame end and the partition_ended is false.
        // The prev_frame_start may be equal to partition_end which
        // will cause the assert fail in advancePartitionEnd function.
        return std::make_tuple(RowNumber(), false);

    auto dist = distance(partition_end, moved_row);
    RUNTIME_CHECK(dist >= 1);

    // Offset is too large and the partition_end is the longest position we can reach to
    if (dist <= step_num)
        return std::make_tuple(partition_end, true);

    // Now, result_row is impossible to reach to partition_end.
    RowNumber result_row = moved_row;
    auto & block = blockAt(result_row);

    // The step happens only in a block
    if ((block.rows - result_row.row - 1) >= step_num)
    {
        result_row.row += step_num;
        return std::make_tuple(result_row, true);
    }

    // The step happens between blocks
    step_num -= block.rows - result_row.row;
    ++result_row.block;
    result_row.row = 0;
    while (step_num > 0)
    {
        auto block_rows = blockAt(result_row).rows;
        if (step_num >= block_rows)
        {
            result_row.row = 0;
            ++result_row.block;
            step_num -= block_rows;
            continue;
        }

        result_row.row += step_num;
        step_num = 0;
    }

    return std::make_tuple(result_row, true);
}

void WindowTransformAction::stepToFrameStart()
{
    RowNumber frame_start_tmp;
    switch (window_description.frame.type)
    {
    case WindowFrame::FrameType::Rows:
    {
        std::tie(frame_start_tmp, frame_started) = stepToStartForRowsFrame(current_row, window_description.frame);
        if (frame_started)
            frame_start = frame_start_tmp;
        break;
    }
    case WindowFrame::FrameType::Ranges:
    {
        std::tie(frame_start_tmp, frame_started) = stepToStartForRangeFrame();
        if (frame_started)
            frame_start = frame_start_tmp;
        break;
    }
    case WindowFrame::FrameType::Groups:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "window function only support frame type row and range.");
    }
}

void WindowTransformAction::stepToFrameEnd()
{
    switch (window_description.frame.type)
    {
    case WindowFrame::FrameType::Rows:
        std::tie(frame_end, frame_ended) = stepToEndForRowsFrame(current_row, window_description.frame);
        break;
    case WindowFrame::FrameType::Ranges:
        std::tie(frame_end, frame_ended) = stepToEndForRangeFrame();
        break;
    case WindowFrame::FrameType::Groups:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "window function only support frame type row and range.");
    }
}

std::tuple<RowNumber, bool> WindowTransformAction::stepToStartForRangeFrame()
{
    if (!window_description.frame.begin_preceding && !partition_ended)
        // If we find the frame end and the partition_ended is false.
        // The prev_frame_start may be equal to partition_end which
        // will cause the assert fail in advancePartitionEnd function.
        return std::make_tuple(RowNumber(), false);

    if (window_description.is_desc)
        return std::make_tuple(stepToStartForRangeFrameOrderCase<true>(), true);
    else
        return std::make_tuple(stepToStartForRangeFrameOrderCase<false>(), true);
}

std::tuple<RowNumber, bool> WindowTransformAction::stepToEndForRangeFrame()
{
    if (!window_description.frame.end_preceding && !partition_ended)
        // If we find the frame end and the partition_ended is false.
        // Some previous blocks may be dropped, this is an unexpected behaviour.
        // So, we shouldn't do anything before the partition_ended is true.
        return std::make_tuple(RowNumber(), false);

    if (window_description.is_desc)
        return stepToEndForRangeFrameOrderCase<true>();
    else
        return stepToEndForRangeFrameOrderCase<false>();
}

template <bool is_desc>
RowNumber WindowTransformAction::stepToStartForRangeFrameOrderCase()
{
    switch (window_description.begin_aux_col_type)
    {
    case TypeIndex::UInt8:
        return stepToStartForRangeFrameImpl<UInt8, is_desc>();
    case TypeIndex::UInt16:
        return stepToStartForRangeFrameImpl<UInt16, is_desc>();
    case TypeIndex::UInt32:
        return stepToStartForRangeFrameImpl<UInt32, is_desc>();
    case TypeIndex::UInt64:
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
        return stepToStartForRangeFrameImpl<UInt64, is_desc>();
    case TypeIndex::Int8:
        return stepToStartForRangeFrameImpl<Int8, is_desc>();
    case TypeIndex::Int16:
        return stepToStartForRangeFrameImpl<Int16, is_desc>();
    case TypeIndex::Int32:
        return stepToStartForRangeFrameImpl<Int32, is_desc>();
    case TypeIndex::Int64:
    case TypeIndex::MyTime:
        return stepToStartForRangeFrameImpl<Int64, is_desc>();
    case TypeIndex::Float32:
        return stepToStartForRangeFrameImpl<Float32, is_desc>();
    case TypeIndex::Float64:
        return stepToStartForRangeFrameImpl<Float64, is_desc>();
    case TypeIndex::Decimal32:
        return stepToStartForRangeFrameImpl<Decimal32, is_desc>();
    case TypeIndex::Decimal64:
        return stepToStartForRangeFrameImpl<Decimal64, is_desc>();
    case TypeIndex::Decimal128:
        return stepToStartForRangeFrameImpl<Decimal128, is_desc>();
    case TypeIndex::Decimal256:
        return stepToStartForRangeFrameImpl<Decimal256, is_desc>();
    default:
        throw Exception("Unexpected column type!");
    }
}

template <bool is_desc>
std::tuple<RowNumber, bool> WindowTransformAction::stepToEndForRangeFrameOrderCase()
{
    switch (window_description.end_aux_col_type)
    {
    case TypeIndex::UInt8:
        return std::make_tuple(stepToEndForRangeFrameImpl<UInt8, is_desc>(), true);
    case TypeIndex::UInt16:
        return std::make_tuple(stepToEndForRangeFrameImpl<UInt16, is_desc>(), true);
    case TypeIndex::UInt32:
        return std::make_tuple(stepToEndForRangeFrameImpl<UInt32, is_desc>(), true);
    case TypeIndex::UInt64:
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
        return std::make_tuple(stepToEndForRangeFrameImpl<UInt64, is_desc>(), true);
    case TypeIndex::Int8:
        return std::make_tuple(stepToEndForRangeFrameImpl<Int8, is_desc>(), true);
    case TypeIndex::Int16:
        return std::make_tuple(stepToEndForRangeFrameImpl<Int16, is_desc>(), true);
    case TypeIndex::Int32:
        return std::make_tuple(stepToEndForRangeFrameImpl<Int32, is_desc>(), true);
    case TypeIndex::Int64:
    case TypeIndex::MyTime:
        return std::make_tuple(stepToEndForRangeFrameImpl<Int64, is_desc>(), true);
    case TypeIndex::Float32:
        return std::make_tuple(stepToEndForRangeFrameImpl<Float32, is_desc>(), true);
    case TypeIndex::Float64:
        return std::make_tuple(stepToEndForRangeFrameImpl<Float64, is_desc>(), true);
    case TypeIndex::Decimal32:
        return std::make_tuple(stepToEndForRangeFrameImpl<Decimal32, is_desc>(), true);
    case TypeIndex::Decimal64:
        return std::make_tuple(stepToEndForRangeFrameImpl<Decimal64, is_desc>(), true);
    case TypeIndex::Decimal128:
        return std::make_tuple(stepToEndForRangeFrameImpl<Decimal128, is_desc>(), true);
    case TypeIndex::Decimal256:
        return std::make_tuple(stepToEndForRangeFrameImpl<Decimal256, is_desc>(), true);
    default:
        throw Exception("Unexpected column type!");
    }
}

template <typename T, bool is_desc>
RowNumber WindowTransformAction::stepToStartForRangeFrameImpl()
{
    return stepForRangeFrameImpl<T, true, is_desc>();
}

template <typename T, bool is_desc>
RowNumber WindowTransformAction::stepToEndForRangeFrameImpl()
{
    return stepForRangeFrameImpl<T, false, is_desc>();
}

template <typename T, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::stepForRangeFrameImpl()
{
    bool is_col_nullable;
    if constexpr (is_begin)
        is_col_nullable = window_description.is_begin_aux_col_nullable;
    else
        is_col_nullable = window_description.is_end_aux_col_nullable;

    if (is_col_nullable)
    {
        ColumnPtr order_by_column = inputAt(current_row)[order_column_indices[0]];
        if (order_by_column->isNullAt(current_row.row))
            return findRangeFrameIfNull<is_begin>(current_row);
    }

    RowNumber cursor;
    if constexpr (is_begin)
        cursor = prev_frame_start;
    else
        cursor = prev_frame_end;

    size_t cur_row_aux_col_idx;
    if constexpr (is_begin)
        cur_row_aux_col_idx = window_description.frame.begin_range_auxiliary_column_index;
    else
        cur_row_aux_col_idx = window_description.frame.end_range_auxiliary_column_index;

    ColumnPtr cur_row_aux_column = inputAt(current_row)[cur_row_aux_col_idx];
    typename ActualCmpDataType<T>::Type current_row_aux_value = getValue<T>(cur_row_aux_column, current_row.row);
    return moveCursorAndFindRangeFrame<typename ActualCmpDataType<T>::Type, is_begin, is_desc>(
        cursor,
        current_row_aux_value);
}

template <bool is_begin>
RowNumber WindowTransformAction::findRangeFrameIfNull(RowNumber cursor)
{
    if (!is_range_null_frame_initialized)
    {
        // We always see the first cursor as frame start
        range_null_frame_start = cursor;

        while (cursor < partition_end)
        {
            const ColumnPtr & cursor_column = inputAt(cursor)[order_column_indices[0]];
            if (!cursor_column->isNullAt(cursor.row))
                break;
            advanceRowNumber(cursor);
        }

        range_null_frame_end = cursor;
        is_range_null_frame_initialized = true;
    }

    if constexpr (is_begin)
        return range_null_frame_start;
    else
        return range_null_frame_end;
}

template <typename AuxColType, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::moveCursorAndFindRangeFrame(RowNumber cursor, AuxColType current_row_aux_value)
{
    switch (window_description.order_by_col_type)
    {
    case TypeIndex::UInt8:
        return moveCursorAndFindRangeFrame<AuxColType, UInt8, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::UInt16:
        return moveCursorAndFindRangeFrame<AuxColType, UInt16, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::UInt32:
        return moveCursorAndFindRangeFrame<AuxColType, UInt32, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::UInt64:
    case TypeIndex::MyDate:
    case TypeIndex::MyDateTime:
        return moveCursorAndFindRangeFrame<AuxColType, UInt64, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Int8:
        return moveCursorAndFindRangeFrame<AuxColType, Int8, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Int16:
        return moveCursorAndFindRangeFrame<AuxColType, Int16, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Int32:
        return moveCursorAndFindRangeFrame<AuxColType, Int32, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Int64:
    case TypeIndex::MyTime:
        return moveCursorAndFindRangeFrame<AuxColType, Int64, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Float32:
        return moveCursorAndFindRangeFrame<AuxColType, Float32, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Float64:
        return moveCursorAndFindRangeFrame<AuxColType, Float64, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Decimal32:
        return moveCursorAndFindRangeFrame<AuxColType, Decimal32, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Decimal64:
        return moveCursorAndFindRangeFrame<AuxColType, Decimal64, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Decimal128:
        return moveCursorAndFindRangeFrame<AuxColType, Decimal128, is_begin, is_desc>(cursor, current_row_aux_value);
    case TypeIndex::Decimal256:
        return moveCursorAndFindRangeFrame<AuxColType, Decimal256, is_begin, is_desc>(cursor, current_row_aux_value);
    default:
        throw Exception("Unexpected column type!");
    }
}

template <typename AuxColType, typename OrderByColType, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::moveCursorAndFindRangeFrame(RowNumber cursor, AuxColType current_row_aux_value)
{
    tipb::RangeCmpDataType cmp_data_type;
    if constexpr (is_begin)
        cmp_data_type = window_description.frame.begin_cmp_data_type;
    else
        cmp_data_type = window_description.frame.end_cmp_data_type;

    if (window_description.is_order_by_col_nullable)
    {
        switch (cmp_data_type)
        {
        case tipb::RangeCmpDataType::Int:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Int,
                is_begin,
                is_desc,
                true>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::DateTime:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::DateTime,
                is_begin,
                is_desc,
                true>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Duration:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Duration,
                is_begin,
                is_desc,
                true>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Float:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Float,
                is_begin,
                is_desc,
                true>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Decimal:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Decimal,
                is_begin,
                is_desc,
                true>(cursor, current_row_aux_value);
        default:
            throw Exception(fmt::format("Unexpected RangeCmpDataType: {}", magic_enum::enum_name(cmp_data_type)));
        }
    }
    else
    {
        switch (cmp_data_type)
        {
        case tipb::RangeCmpDataType::Int:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Int,
                is_begin,
                is_desc,
                false>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::DateTime:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::DateTime,
                is_begin,
                is_desc,
                false>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Duration:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Duration,
                is_begin,
                is_desc,
                false>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Float:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Float,
                is_begin,
                is_desc,
                false>(cursor, current_row_aux_value);
        case tipb::RangeCmpDataType::Decimal:
            return moveCursorAndFindRangeFrameImpl<
                AuxColType,
                OrderByColType,
                tipb::RangeCmpDataType::Decimal,
                is_begin,
                is_desc,
                false>(cursor, current_row_aux_value);
        default:
            throw Exception("Unexpected RangeCmpDataType!");
        }
    }
}

template <
    typename AuxColType,
    typename OrderByColType,
    int CmpDataType,
    bool is_begin,
    bool is_desc,
    bool is_order_by_col_nullable>
RowNumber WindowTransformAction::moveCursorAndFindRangeFrameImpl(RowNumber cursor, AuxColType current_row_aux_value)
{
    using ActualOrderByColType = typename ActualCmpDataType<OrderByColType>::Type;

    RowNumber boundary = getBoundary<is_begin>(*this);
    while (cursor < boundary)
    {
        const ColumnPtr & cursor_column = inputAt(cursor)[order_column_indices[0]];
        if constexpr (is_order_by_col_nullable)
        {
            if (cursor_column->isNullAt(cursor.row))
            {
                if constexpr (is_begin)
                {
                    if (!is_desc)
                        advanceRowNumber(cursor);
                    else
                        return cursor;
                    continue;
                }
                else
                {
                    if (window_description.frame.end_preceding)
                    {
                        advanceRowNumber(cursor);
                        continue;
                    }
                    else
                        return cursor;
                }
            }
        }

        ActualOrderByColType cursor_value = getValue<OrderByColType>(cursor_column, cursor.row);

        if constexpr (is_begin)
        {
            if (window_description.frame.begin_preceding)
            {
                if (isInRange<AuxColType, ActualOrderByColType, CmpDataType, true, is_desc, is_begin>(
                        current_row_aux_value,
                        cursor_value))
                    return cursor;
            }
            else
            {
                if (isInRange<AuxColType, ActualOrderByColType, CmpDataType, false, is_desc, is_begin>(
                        current_row_aux_value,
                        cursor_value))
                    return cursor;
            }
        }
        else
        {
            if (window_description.frame.end_preceding)
            {
                if (!isInRange<AuxColType, ActualOrderByColType, CmpDataType, true, is_desc, is_begin>(
                        current_row_aux_value,
                        cursor_value))
                    return cursor;
            }
            else
            {
                if (!isInRange<AuxColType, ActualOrderByColType, CmpDataType, false, is_desc, is_begin>(
                        current_row_aux_value,
                        cursor_value))
                    return cursor;
            }
        }

        advanceRowNumber(cursor);
    }
    return cursor;
}

UInt64 WindowTransformAction::distance(RowNumber left, RowNumber right)
{
    if (left.block == right.block)
    {
        RUNTIME_CHECK_MSG(left.row >= right.row, "left should always be bigger than right");
        return left.row - right.row;
    }

    RUNTIME_CHECK_MSG(left.block > right.block, "left should always be bigger than right");

    Int64 dist = left.row;
    RowNumber tmp = left;
    --tmp.block;
    while (tmp.block > right.block)
    {
        dist += blockAt(tmp).rows;
        --tmp.block;
    }

    dist += blockAt(right).rows - right.row;

    return dist;
}

void WindowTransformAction::advanceFrameStart()
{
    if (frame_started)
        return;

    switch (window_description.frame.begin_type)
    {
    case WindowFrame::BoundaryType::Unbounded:
        // UNBOUNDED PRECEDING, just mark it valid. It is initialized when
        // the new partition starts.
        frame_started = true;
        break;
    case WindowFrame::BoundaryType::Current:
    {
        frame_start = current_row;
        frame_started = true;
        break;
    }
    case WindowFrame::BoundaryType::Offset:
        stepToFrameStart();
        break;
    default:
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The frame begin type '{}' is not implemented",
            magic_enum::enum_name(window_description.frame.begin_type));
    }
}

bool WindowTransformAction::arePeers(const RowNumber & peer_group_last_row, const RowNumber & current_row) const
{
    if (peer_group_last_row == current_row)
    {
        // For convenience, a row is always its own peer.
        return true;
    }

    switch (window_description.frame.type)
    {
    case WindowFrame::FrameType::Rows:
        // For ROWS frame, row is only peers with itself (checked above);
        return false;
    case WindowFrame::FrameType::Ranges:
    {
        // For RANGE frames, rows that compare equal w/ORDER BY are peers.
        const size_t n = order_column_indices.size();
        if (n == 0)
        {
            // No ORDER BY, so all rows are peers.
            return true;
        }

        for (size_t i = 0; i < n; ++i)
        {
            const auto * column_peer_last = inputAt(peer_group_last_row)[order_column_indices[i]].get();
            const auto * column_current = inputAt(current_row)[order_column_indices[i]].get();
            if (window_description.order_by[i].collator)
            {
                if (column_peer_last->compareAt(
                        peer_group_last_row.row,
                        current_row.row,
                        *column_current,
                        1 /* nan_direction_hint */,
                        *window_description.order_by[i].collator)
                    != 0)
                {
                    return false;
                }
            }
            else
            {
                if (column_peer_last->compareAt(
                        peer_group_last_row.row,
                        current_row.row,
                        *column_current,
                        1 /* nan_direction_hint */)
                    != 0)
                {
                    return false;
                }
            }
        }
        return true;
    }
    case WindowFrame::FrameType::Groups:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "window function only support frame type row and range.");
    }
}

void WindowTransformAction::advanceFrameEndCurrentRow()
{
    assert(frame_end.block == partition_end.block || frame_end.block + 1 == partition_end.block);

    frame_end = current_row;
    advanceRowNumber(frame_end);
    frame_ended = true;
}

void WindowTransformAction::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    assert(!frame_ended);

    // switch for another frame type
    switch (window_description.frame.end_type)
    {
    case WindowFrame::BoundaryType::Current:
        advanceFrameEndCurrentRow();
        break;
    case WindowFrame::BoundaryType::Unbounded:
    {
        // The UNBOUNDED FOLLOWING frame ends when the partition ends.
        frame_end = partition_end;
        frame_ended = partition_ended;
        break;
    }
    case WindowFrame::BoundaryType::Offset:
    {
        stepToFrameEnd();
        break;
    }
    default:
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The frame end type '{}' is not implemented",
            magic_enum::enum_name(window_description.frame.end_type));
    }
}

void WindowTransformAction::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];
        ws.window_function->windowInsertResultInto(*this, wi, ws.arguments);
    }
}

Block WindowTransformAction::tryGetOutputBlock()
{
    // first try calculate the result based on current data
    tryCalculate();
    // then return block if it is ready
    assert(first_not_ready_row.block >= first_block_number);
    // The first_not_ready_row might be past-the-end if we have already
    // calculated the window functions for all input rows. That's why the
    // equality is also valid here.
    assert(first_not_ready_row.block <= first_block_number + window_blocks.size());
    assert(next_output_block_number >= first_block_number);

    if (next_output_block_number < first_not_ready_row.block)
    {
        const auto i = next_output_block_number - first_block_number;
        auto & block = window_blocks[i];
        auto columns = block.input_columns;
        for (auto & res : block.output_columns)
        {
            columns.push_back(ColumnPtr(std::move(res)));
        }
        ++next_output_block_number;

        auto output_block = output_header.cloneWithColumns(std::move(columns));
        releaseAlreadyOutputWindowBlock();
        return output_block;
    }
    return {};
}

bool WindowTransformAction::onlyHaveRowNumber()
{
    for (const auto & workspace : workspaces)
    {
        if (workspace.window_function->getName() != "row_number")
            return false;
    }
    return true;
}

void WindowTransformAction::releaseAlreadyOutputWindowBlock()
{
    // We don't really have to keep the entire partition, and it can be big, so
    // we want to drop the starting blocks to save memory. We can drop the old
    // blocks if we already returned them as output, and the frame and the
    // current row are already past them. We also need to keep the previous
    // frame start because we use it as the partition standard. It is always less
    // than the current frame start, so we don't have to check the latter. Note
    // that the frame start can be further than current row for some frame specs
    // (e.g. EXCLUDE CURRENT ROW), so we have to check both.
    assert(prev_frame_start <= frame_start);
    const auto first_used_block = std::min(
        std::min(next_output_block_number, peer_group_last.block),
        std::min(prev_frame_start.block, current_row.block));


    if (first_block_number < first_used_block)
    {
        window_blocks.erase(window_blocks.begin(), window_blocks.begin() + (first_used_block - first_block_number));
        first_block_number = first_used_block;

        assert(next_output_block_number >= first_block_number);
        assert(frame_start.block >= first_block_number);
        assert(prev_frame_start.block >= first_block_number);
        assert(current_row.block >= first_block_number);
    }
}

void WindowTransformAction::appendBlock(Block & current_block)
{
    assert(!input_is_finished);
    assert(current_block);

    if (current_block.rows() == 0)
        return;

    window_blocks.push_back({});
    auto & window_block = window_blocks.back();
    window_block.rows = current_block.rows();

    // Initialize output columns and add new columns to output block.
    for (auto & ws : workspaces)
    {
        MutableColumnPtr res = ws.window_function->getReturnType()->createColumn();
        res->reserve(window_block.rows);
        window_block.output_columns.push_back(std::move(res));
    }

    window_block.input_columns = current_block.getColumns();
}

void WindowTransformAction::tryCalculate()
{
    // if there is no input data, we don't need to calculate
    if (window_blocks.empty())
        return;
    auto start_block_index = current_row.block;
    // Start the calculations. First, advance the partition end.
    for (;;)
    {
        advancePartitionEnd();

        // Either we ran out of data or we found the end of partition (maybe
        // both, but this only happens at the total end of data).
        assert(partition_ended || partition_end == blocksEnd());
        if (partition_ended && partition_end == blocksEnd())
        {
            assert(input_is_finished);
        }


        while (current_row < partition_end)
        {
            // if window only have row_number function, we can ignore judging peers
            if (!only_have_row_number)
            {
                // peer_group_last save the row before current_row
                if (!arePeers(peer_group_last, current_row))
                {
                    peer_group_start_row_number = current_row_number;
                    ++peer_group_number;
                }
            }
            peer_group_last = current_row;

            // Advance the frame start.
            advanceFrameStart();

            if (!frame_started)
            {
                // Wait for more input data to find the start of frame.
                assert(!input_is_finished);
                assert(!partition_ended);
                return;
            }

            // Advance the frame end.
            advanceFrameEnd();

            if (!frame_ended)
            {
                // Wait for more input data to find the end of frame.
                assert(!input_is_finished);
                assert(!partition_ended);
                return;
            }

            // The frame can be empty sometimes, e.g. the boundaries coincide
            // or the start is after the partition end. But hopefully start is
            // not after end.
            assert(frame_started);
            assert(frame_ended);

            // Write out the results.
            // TODO execute the window function by block instead of row.
            writeOutCurrentRow();

            prev_frame_start = frame_start;
            prev_frame_end = frame_end;

            // Move to the next row. The frame will have to be recalculated.
            // The peer group start is updated at the beginning of the loop,
            // because current_row might now be past-the-end.
            advanceRowNumber(current_row);
            ++current_row_number;
            first_not_ready_row = current_row;
            frame_ended = false;
            frame_started = false;
            // each `tryCalculate()` will calculate at most 1 block's data
            // this is to make sure that in pipeline mode, the execution time
            // of each iterator won't be too long
            if (current_row.block != start_block_index)
                return;
        }

        if (input_is_finished)
        {
            // We finalized the last partition in the above loop, and don't have
            // to do anything else.
            assert(current_row == blocksEnd());
            return;
        }

        if (!partition_ended)
        {
            // Wait for more input data to find the end of partition.
            // Assert that we processed all the data we currently have, and that
            // we are going to receive more data.
            assert(partition_end == blocksEnd());
            assert(!input_is_finished);
            break;
        }

        // Start the next partition.
        partition_start = partition_end;
        advanceRowNumber(partition_end);
        partition_ended = false;
        // We have to reset the frame and other pointers when the new partition starts.
        frame_start = partition_start;
        frame_end = partition_start;
        prev_frame_start = partition_start;
        prev_frame_end = partition_end;
        assert(current_row == partition_start);
        current_row_number = 1;
        peer_group_last = partition_start;
        peer_group_start_row_number = 1;
        peer_group_number = 1;
        is_range_null_frame_initialized = false;
    }
}

void WindowTransformAction::appendInfo(FmtBuffer & buffer) const
{
    buffer.append(", function: {");
    buffer.joinStr(
        window_description.window_functions_descriptions.begin(),
        window_description.window_functions_descriptions.end(),
        [&](const auto & func, FmtBuffer & b) { b.append(func.window_function->getName()); },
        ", ");
    buffer.fmtAppend(
        "}}, frame: {{type: {}, boundary_begin: {}, boundary_end: {}}}",
        frameTypeToString(window_description.frame.type),
        boundaryTypeToString(window_description.frame.begin_type),
        boundaryTypeToString(window_description.frame.end_type));
}

void WindowBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    action.appendInfo(buffer);
}

void WindowTransformAction::advanceRowNumber(RowNumber & row_num) const
{
    assert(row_num.block >= first_block_number);
    assert(row_num.block - first_block_number < window_blocks.size());

    const auto block_rows = blockAt(row_num).rows;
    assert(row_num.row < block_rows);

    ++row_num.row;
    if (row_num.row < block_rows)
    {
        return;
    }

    row_num.row = 0;
    ++row_num.block;
}

RowNumber WindowTransformAction::getPreviousRowNumber(const RowNumber & row_num) const
{
    assert(row_num.block >= first_block_number);
    assert(row_num.block != 0 || row_num.row != 0);

    RowNumber prev_row_num = row_num;
    if (row_num.row > 0)
    {
        --prev_row_num.row;
        return prev_row_num;
    }

    --prev_row_num.block;
    assert(prev_row_num.block < window_blocks.size() + first_block_number);
    const auto new_block_rows = blockAt(prev_row_num).rows;
    prev_row_num.row = new_block_rows - 1;
    return prev_row_num;
}

bool WindowTransformAction::lead(RowNumber & x, size_t offset) const
{
    assert(frame_started);
    assert(frame_ended);

    assert(x.block >= first_block_number);
    assert(x.block - first_block_number < window_blocks.size());

    const auto block_rows = blockAt(x).rows;
    assert(x.row < block_rows);

    x.row += offset;
    if (x.row < block_rows)
    {
        return x < frame_end;
    }

    ++x.block;
    if (x.block - first_block_number == window_blocks.size())
        return false;
    size_t new_offset = x.row - block_rows;
    x.row = 0;
    return lead(x, new_offset);
}

bool WindowTransformAction::lag(RowNumber & x, size_t offset) const
{
    assert(frame_started);
    assert(frame_ended);

    assert(x.block >= first_block_number);
    assert(x.block - first_block_number < window_blocks.size());

    if (x.row >= offset)
    {
        x.row -= offset;
        return frame_start <= x;
    }

    if (x.block <= first_block_number)
        return false;

    --x.block;
    size_t new_offset = offset - x.row - 1;
    x.row = blockAt(x.block).rows - 1;
    return lag(x, new_offset);
}
} // namespace DB
