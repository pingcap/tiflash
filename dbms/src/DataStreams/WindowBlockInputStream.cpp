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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Decimal.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Interpreters/WindowDescription.h>
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
extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
} // namespace ErrorCodes

namespace
{
template <typename T>
consteval bool checkIfSimpleNumericType()
{
    return std::is_integral_v<T> || std::is_floating_point_v<T>;
}

template <typename T>
struct DecimalWithScale
{
    DecimalWithScale(T decimal_, Int64 scale_)
        : decimal(decimal_)
        , scale(scale_)
    {
        if constexpr (!IsDecimal<T>)
            throw Exception("DecimalWithScale only accepts Decimal type.");
    }

    T decimal; // This T should be decimal type
    Int64 scale;
};

using DecimalWithScale32 = DecimalWithScale<Decimal32>;
using DecimalWithScale64 = DecimalWithScale<Decimal64>;
using DecimalWithScale128 = DecimalWithScale<Decimal128>;
using DecimalWithScale256 = DecimalWithScale<Decimal256>;

template <typename T>
consteval bool checkIfDecimalWithScaleType()
{
    return std::is_same_v<T, DecimalWithScale32> || std::is_same_v<T, DecimalWithScale64> || std::is_same_v<T, DecimalWithScale128> || std::is_same_v<T, DecimalWithScale256>;
}

// When T is Decimal, we should convert it to DecimalWithScale type
// as we need scale value when executing the comparison operation.
template <typename T>
struct ActualCmpDataType
{
    using Type = std::conditional_t<checkIfSimpleNumericType<T>(), T, DecimalWithScale<T>>;
};

template <typename T>
typename ActualCmpDataType<T>::Type getValue(const ColumnPtr & col_ptr, size_t idx)
{
    if constexpr (checkIfSimpleNumericType<T>())
    {
        const auto * col = static_cast<const ColumnVector<T> *>(&(*col_ptr));
        const typename ColumnVector<T>::Container & data = col->getData();
        return data[idx];
    }
    else if (IsDecimal<T>)
    {
        const auto * col = static_cast<const ColumnDecimal<T> *>(&(*col_ptr));
        const typename ColumnDecimal<T>::Container & data = col->getData();

        // This is a Decimal type.
        // T(...) is equal to Decimal<U>(xxx)
        return typename ActualCmpDataType<T>::Type(T(data[idx]), data.getScale());
    }
    else
        throw Exception("Unexpected column type!");
}

template <typename T, bool is_start, bool is_desc>
bool isInRangeCommonImpl(T current_row_aux_value, T cursor_value)
{
    if constexpr ((is_start && is_desc) || (!is_start && !is_desc))
        return cursor_value <= current_row_aux_value;
    else
        return cursor_value >= current_row_aux_value;
}

template <bool is_start, bool is_desc>
bool isInRangeIntImpl(Int64 current_row_aux_value, Int64 cursor_value)
{
    return isInRangeCommonImpl<Int64, is_start, is_desc>(current_row_aux_value, cursor_value);
}

template <typename AuxColType, typename OrderByColType, bool is_start, bool is_desc>
bool isInRangeNotIntImpl(AuxColType current_row_aux_value, OrderByColType cursor_value)
{
    Float64 current_row_aux_value_float64;
    Float64 cursor_value_float64;

    if constexpr (checkIfDecimalWithScaleType<AuxColType>())
        current_row_aux_value_float64 = current_row_aux_value.decimal.template toFloat<Float64>(current_row_aux_value.scale);
    else
        current_row_aux_value_float64 = static_cast<Float64>(current_row_aux_value);

    if constexpr (checkIfDecimalWithScaleType<OrderByColType>())
        cursor_value_float64 = cursor_value.decimal.template toFloat<Float64>(cursor_value.scale);
    else
        cursor_value_float64 = static_cast<Float64>(cursor_value);

    return isInRangeCommonImpl<Float64, is_start, is_desc>(current_row_aux_value_float64, cursor_value_float64);
}

template <typename AuxColType, typename OrderByColType, int CmpDataType, bool is_start, bool is_desc>
bool isInRange(AuxColType current_row_aux_value, OrderByColType cursor_value)
{
    if constexpr (CmpDataType == tipb::RangeCmpDataType::Int)
    {
        // Two operand must be integer
        if constexpr (std::is_integral_v<OrderByColType> && std::is_integral_v<AuxColType>)
            return isInRangeIntImpl<is_start, is_desc>(current_row_aux_value, cursor_value);
        throw Exception("Unexpected Data Type!");
    }
    else if (CmpDataType == tipb::RangeCmpDataType::Float)
    {
        return isInRangeNotIntImpl<AuxColType, OrderByColType, is_start, is_desc>(current_row_aux_value, cursor_value);
    }
    else
    {
        if constexpr (std::is_floating_point_v<OrderByColType> || std::is_floating_point_v<AuxColType>)
            throw Exception("Occurrence of float type at here is unexpected!");

        return isInRangeNotIntImpl<AuxColType, OrderByColType, is_start, is_desc>(current_row_aux_value, cursor_value);
    }
}

ColumnPtr checkAndGetNestedColumn(const ColumnPtr & col)
{
    const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
    const auto & null_map = nullable_col.getNullMapData();

    if (!mem_utils::memoryIsZero(null_map.data(), null_map.size()))
        throw Exception{
            "Null value should not occur when frame type is range",
            ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};

    return nullable_col.getNestedColumnPtr();
}
} // namespace

WindowTransformAction::WindowTransformAction(const Block & input_header, const WindowDescription & window_description_, const String & req_id)
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

WindowBlockInputStream::WindowBlockInputStream(const BlockInputStreamPtr & input, const WindowDescription & window_description_, const String & req_id)
    : action(input->getHeader(), window_description_, req_id)
{
    children.push_back(input);
}

void WindowTransformAction::initialPartitionAndOrderColumnIndices()
{
    partition_column_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_column_indices.push_back(
            output_header.getPositionByName(column.column_name));
    }

    order_column_indices.reserve(window_description.order_by.size());
    for (const auto & column : window_description.order_by)
    {
        order_column_indices.push_back(
            output_header.getPositionByName(column.column_name));
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
    const auto & stream = children.back();
    while (!action.input_is_finished)
    {
        if (returnIfCancelledOrKilled())
            return {};

        if (Block output_block = action.tryGetOutputBlock())
            return output_block;

        Block block = stream->read();
        if (!block)
            action.input_is_finished = true;
        else
            action.appendBlock(block);
        action.tryCalculate();
    }

    if (returnIfCancelledOrKilled())
        return {};
    // return last partition block, if already return then return null
    return action.tryGetOutputBlock();
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
            if (compared_column->compareAt(current_partition_row,
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
            if (compared_column->compareAt(current_partition_row,
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
    RUNTIME_ASSERT(!partition_ended, log, "partition_ended should be false here.");
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

RowNumber WindowTransformAction::stepToFrameStartForRows()
{
    auto step_num = window_description.frame.begin_offset;
    auto dist = distance(current_row, partition_start);

    if (dist <= step_num)
        return partition_start;

    RowNumber result_row = current_row;

    // The step happens only in a block
    if (result_row.row >= step_num)
    {
        result_row.row -= step_num;
        return result_row;
    }

    // The step happens between blocks
    step_num -= (result_row.row + 1);
    --result_row.block;
    result_row.row = blockAt(result_row).rows - 1;
    while (step_num > 0)
    {
        auto & block = blockAt(result_row);
        if (block.rows > step_num)
        {
            result_row.row = block.rows - step_num - 1; // index, so we need to -1
            break;
        }
        step_num -= block.rows;
        --result_row.block;
        result_row.row = blockAt(result_row).rows - 1;
    }
    return result_row;
}

std::tuple<RowNumber, bool> WindowTransformAction::stepToFrameEndForRows()
{
    auto step_num = window_description.frame.end_offset;
    if (!partition_ended)
        // If we find the frame end and the partition_ended is false.
        // Some previous blocks may be dropped, this is an unexpected behaviour.
        // So, we shouldn't do anything before the partition_ended is true.
        return std::make_tuple(RowNumber(), false);

    // Range of rows is [frame_start, frame_end),
    // and frame_end position is behind the position of the last frame row.
    // So we need to ++n.
    ++step_num;

    auto dist = distance(partition_end, current_row);
    RUNTIME_CHECK(dist >= 1);

    // Offset is too large and the partition_end is the longest position we can reach
    if (dist <= step_num)
        return std::make_tuple(partition_end, true);

    // Now, frame_end is impossible to reach to partition_end.
    RowNumber frame_end_row = current_row;
    auto & block = blockAt(frame_end_row);

    // The step happens only in a block
    if ((block.rows - frame_end_row.row - 1) >= step_num)
    {
        frame_end_row.row += step_num;
        return std::make_tuple(frame_end_row, true);
    }

    // The step happens between blocks
    step_num -= block.rows - frame_end_row.row;
    ++frame_end_row.block;
    frame_end_row.row = 0;
    while (step_num > 0)
    {
        auto block_rows = blockAt(frame_end_row).rows;
        if (step_num >= block_rows)
        {
            frame_end_row.row = 0;
            ++frame_end_row.block;
            step_num -= block_rows;
            continue;
        }

        frame_end_row.row += step_num;
        step_num = 0;
    }

    return std::make_tuple(frame_end_row, true);
}

void WindowTransformAction::stepToFrameStartWithOffsetBoundry()
{
    if (window_description.frame.type == WindowFrame::FrameType::Rows)
        frame_start = stepToFrameStartForRows();
    else
        frame_start = stepToFrameStartForRange();
}

void WindowTransformAction::stepToFrameEndWithOffsetBoundary()
{
    if (window_description.frame.type == WindowFrame::FrameType::Rows)
        std::tie(frame_end, frame_ended) = stepToFrameEndForRows();
    else
        std::tie(frame_end, frame_ended) = stepToFrameEndForRange();
}

RowNumber WindowTransformAction::stepToFrameStartForRange()
{
    if (window_description.is_desc)
        return stepToFrameStartForRange<true>();
    else
        return stepToFrameStartForRange<false>();
}

std::tuple<RowNumber, bool> WindowTransformAction::stepToFrameEndForRange()
{
    if (window_description.is_desc)
        return stepToFrameEndForRange<true>();
    else
        return stepToFrameEndForRange<false>();
}

template <bool is_desc>
RowNumber WindowTransformAction::stepToFrameStartForRange()
{
    switch (window_description.begin_aux_col_type)
    {
    case Window::ColumnType::UInt8:
        return stepToFrameStartForRangeImpl<UInt8, is_desc>();
    case Window::ColumnType::UInt16:
        return stepToFrameStartForRangeImpl<UInt16, is_desc>();
    case Window::ColumnType::UInt32:
        return stepToFrameStartForRangeImpl<UInt32, is_desc>();
    case Window::ColumnType::UInt64:
        return stepToFrameStartForRangeImpl<UInt64, is_desc>();
    case Window::ColumnType::Int8:
        return stepToFrameStartForRangeImpl<Int8, is_desc>();
    case Window::ColumnType::Int16:
        return stepToFrameStartForRangeImpl<Int16, is_desc>();
    case Window::ColumnType::Int32:
        return stepToFrameStartForRangeImpl<Int32, is_desc>();
    case Window::ColumnType::Int64:
        return stepToFrameStartForRangeImpl<Int64, is_desc>();
    case Window::ColumnType::Float32:
        return stepToFrameStartForRangeImpl<Float32, is_desc>();
    case Window::ColumnType::Float64:
        return stepToFrameStartForRangeImpl<Float64, is_desc>();
    case Window::ColumnType::Decimal32:
        return stepToFrameStartForRangeImpl<Decimal32, is_desc>();
    case Window::ColumnType::Decimal64:
        return stepToFrameStartForRangeImpl<Decimal64, is_desc>();
    case Window::ColumnType::Decimal128:
        return stepToFrameStartForRangeImpl<Decimal128, is_desc>();
    case Window::ColumnType::Decimal256:
        return stepToFrameStartForRangeImpl<Decimal256, is_desc>();
    default:
        throw Exception("Unexpected column type!");
    }
}

template <bool is_desc>
std::tuple<RowNumber, bool> WindowTransformAction::stepToFrameEndForRange()
{
    if (!partition_ended)
        // If we find the frame end and the partition_ended is false.
        // Some previous blocks may be dropped, this is an unexpected behaviour.
        // So, we shouldn't do anything before the partition_ended is true.
        return std::make_tuple(RowNumber(), false);

    switch (window_description.end_aux_col_type)
    {
    case Window::ColumnType::UInt8:
        return std::make_tuple(stepToFrameEndForRangeImpl<UInt8, is_desc>(), true);
    case Window::ColumnType::UInt16:
        return std::make_tuple(stepToFrameEndForRangeImpl<UInt16, is_desc>(), true);
    case Window::ColumnType::UInt32:
        return std::make_tuple(stepToFrameEndForRangeImpl<UInt32, is_desc>(), true);
    case Window::ColumnType::UInt64:
        return std::make_tuple(stepToFrameEndForRangeImpl<UInt64, is_desc>(), true);
    case Window::ColumnType::Int8:
        return std::make_tuple(stepToFrameEndForRangeImpl<Int8, is_desc>(), true);
    case Window::ColumnType::Int16:
        return std::make_tuple(stepToFrameEndForRangeImpl<Int16, is_desc>(), true);
    case Window::ColumnType::Int32:
        return std::make_tuple(stepToFrameEndForRangeImpl<Int32, is_desc>(), true);
    case Window::ColumnType::Int64:
        return std::make_tuple(stepToFrameEndForRangeImpl<Int64, is_desc>(), true);
    case Window::ColumnType::Float32:
        return std::make_tuple(stepToFrameEndForRangeImpl<Float32, is_desc>(), true);
    case Window::ColumnType::Float64:
        return std::make_tuple(stepToFrameEndForRangeImpl<Float64, is_desc>(), true);
    case Window::ColumnType::Decimal32:
        return std::make_tuple(stepToFrameEndForRangeImpl<Decimal32, is_desc>(), true);
    case Window::ColumnType::Decimal64:
        return std::make_tuple(stepToFrameEndForRangeImpl<Decimal64, is_desc>(), true);
    case Window::ColumnType::Decimal128:
        return std::make_tuple(stepToFrameEndForRangeImpl<Decimal128, is_desc>(), true);
    case Window::ColumnType::Decimal256:
        return std::make_tuple(stepToFrameEndForRangeImpl<Decimal256, is_desc>(), true);
    default:
        throw Exception("Unexpected column type!");
    }
}

template <typename T, bool is_desc>
RowNumber WindowTransformAction::stepToFrameStartForRangeImpl()
{
    return stepToFrameForRangeImpl<T, true, is_desc>();
}

template <typename T, bool is_desc>
RowNumber WindowTransformAction::stepToFrameEndForRangeImpl()
{
    return stepToFrameForRangeImpl<T, false, is_desc>();
}

template <typename T, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::stepToFrameForRangeImpl()
{
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

    const ColumnPtr & cur_row_order_column = inputAt(current_row)[cur_row_aux_col_idx];
    typename ActualCmpDataType<T>::Type current_row_aux_value = getValue<T>(cur_row_order_column, current_row.row);
    return moveCursorAndFindFrameBoundary<typename ActualCmpDataType<T>::Type, is_begin, is_desc>(cursor, current_row_aux_value);
}

template <typename AuxColType, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::moveCursorAndFindFrameBoundary(RowNumber cursor, AuxColType current_row_aux_value)
{
    switch (window_description.order_by_col_type)
    {
    case Window::ColumnType::UInt8:
        return moveCursorAndFindFrameBoundary<AuxColType, UInt8, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::UInt16:
        return moveCursorAndFindFrameBoundary<AuxColType, UInt16, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::UInt32:
        return moveCursorAndFindFrameBoundary<AuxColType, UInt32, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::UInt64:
        return moveCursorAndFindFrameBoundary<AuxColType, UInt64, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Int8:
        return moveCursorAndFindFrameBoundary<AuxColType, Int8, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Int16:
        return moveCursorAndFindFrameBoundary<AuxColType, Int16, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Int32:
        return moveCursorAndFindFrameBoundary<AuxColType, Int32, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Int64:
        return moveCursorAndFindFrameBoundary<AuxColType, Int64, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Float32:
        return moveCursorAndFindFrameBoundary<AuxColType, Float32, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Float64:
        return moveCursorAndFindFrameBoundary<AuxColType, Float64, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Decimal32:
        return moveCursorAndFindFrameBoundary<AuxColType, Decimal32, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Decimal64:
        return moveCursorAndFindFrameBoundary<AuxColType, Decimal64, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Decimal128:
        return moveCursorAndFindFrameBoundary<AuxColType, Decimal128, is_begin, is_desc>(cursor, current_row_aux_value);
    case Window::ColumnType::Decimal256:
        return moveCursorAndFindFrameBoundary<AuxColType, Decimal256, is_begin, is_desc>(cursor, current_row_aux_value);
    default:
        throw Exception("Unexpected column type!");
    }
}

template <typename AuxColType, typename OrderByColType, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::moveCursorAndFindFrameBoundary(RowNumber cursor, AuxColType current_row_aux_value)
{
    tipb::RangeCmpDataType cmp_data_type;
    if constexpr (is_begin)
        cmp_data_type = window_description.frame.begin_cmp_data_type;
    else
        cmp_data_type = window_description.frame.end_cmp_data_type;

    switch (cmp_data_type)
    {
    case tipb::RangeCmpDataType::Int:
        return moveCursorAndFindFrameBoundaryImpl<AuxColType, OrderByColType, tipb::RangeCmpDataType::Int, is_begin, is_desc>(cursor, current_row_aux_value);
    case tipb::RangeCmpDataType::Float:
        return moveCursorAndFindFrameBoundaryImpl<AuxColType, OrderByColType, tipb::RangeCmpDataType::Float, is_begin, is_desc>(cursor, current_row_aux_value);
    case tipb::RangeCmpDataType::Decimal:
        return moveCursorAndFindFrameBoundaryImpl<AuxColType, OrderByColType, tipb::RangeCmpDataType::Decimal, is_begin, is_desc>(cursor, current_row_aux_value);
    default:
        throw Exception("Unexpected RangeCmpDataType!");
    }
}

template <typename AuxColType, typename OrderByColType, int CmpDataType, bool is_begin, bool is_desc>
RowNumber WindowTransformAction::moveCursorAndFindFrameBoundaryImpl(RowNumber cursor, AuxColType current_row_aux_value)
{
    using ActualOrderByColType = typename ActualCmpDataType<OrderByColType>::Type;

    while (cursor < partition_end)
    {
        const ColumnPtr & cursor_column = inputAt(cursor)[order_column_indices[0]];
        ActualOrderByColType cursor_value = getValue<OrderByColType>(cursor_column, cursor.row);

        if constexpr (is_begin)
        {
            if (isInRange<AuxColType, ActualOrderByColType, CmpDataType, is_begin, is_desc>(current_row_aux_value, cursor_value))
                return cursor;
        }
        else
        {
            if (!isInRange<AuxColType, ActualOrderByColType, CmpDataType, is_begin, is_desc>(current_row_aux_value, cursor_value))
                return cursor;
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
        stepToFrameStartWithOffsetBoundry();
        frame_started = true;
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
                if (column_peer_last->compareAt(peer_group_last_row.row, current_row.row, *column_current, 1 /* nan_direction_hint */, *window_description.order_by[i].collator) != 0)
                {
                    return false;
                }
            }
            else
            {
                if (column_peer_last->compareAt(peer_group_last_row.row, current_row.row, *column_current, 1 /* nan_direction_hint */) != 0)
                {
                    return false;
                }
            }
        }
        return true;
    }
    case WindowFrame::FrameType::Groups:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "window function only support frame type row and range.");
    }
}

void WindowTransformAction::advanceFrameEndCurrentRow()
{
    assert(frame_end.block == partition_end.block
           || frame_end.block + 1 == partition_end.block);

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
        stepToFrameEndWithOffsetBoundary();
        break;
    }
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
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
    const auto first_used_block = std::min(std::min(next_output_block_number, peer_group_last.block),
                                           std::min(prev_frame_start.block, current_row.block));


    if (first_block_number < first_used_block)
    {
        window_blocks.erase(window_blocks.begin(),
                            window_blocks.begin() + (first_used_block - first_block_number));
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

    // When frame type is range and the aux col in nullable, we need to ensure that all row are not null
    // and get the nested column. They are auxiliary columns and will not be used any more, so it's ok to strip the nullable column
    if (window_description.is_casted_begin_col_nullable)
    {
        auto aux_col_idx = window_description.frame.begin_range_auxiliary_column_index;
        window_block.input_columns[aux_col_idx] = checkAndGetNestedColumn(window_block.input_columns[aux_col_idx]);
    }

    if (window_description.is_casted_end_col_nullable)
    {
        auto aux_col_idx = window_description.frame.end_range_auxiliary_column_index;
        window_block.input_columns[aux_col_idx] = checkAndGetNestedColumn(window_block.input_columns[aux_col_idx]);
    }
}

void WindowTransformAction::tryCalculate()
{
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
            assert(frame_start <= frame_end);

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
    }
}

void WindowTransformAction::appendInfo(FmtBuffer & buffer) const
{
    buffer.append(", function: {");
    buffer.joinStr(
        window_description.window_functions_descriptions.begin(),
        window_description.window_functions_descriptions.end(),
        [&](const auto & func, FmtBuffer & b) {
            b.append(func.window_function->getName());
        },
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
    assert(!(row_num.block == 0 && row_num.row == 0));

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
    assert(frame_start <= frame_end);

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
    assert(frame_start <= frame_end);

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
