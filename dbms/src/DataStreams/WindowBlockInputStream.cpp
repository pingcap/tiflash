#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Common/Arena.h>
#include <Common/FieldVisitors.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <common/arithmeticOverflow.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes


// Compares ORDER BY column values at given rows to find the boundaries of frame:
// [compared] with [reference] +/- offset. Return value is -1/0/+1, like in
// sorting predicates -- -1 means [compared] is less than [reference] +/- offset.
template <typename ColumnType>
static int compareValuesWithOffset(const IColumn * _compared_column,
                                   size_t compared_row,
                                   const IColumn * _reference_column,
                                   size_t reference_row,
                                   const Field & _offset,
                                   bool offset_is_preceding)
{
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    const auto * compared_column = assert_cast<const ColumnType *>(
        _compared_column);
    const auto * reference_column = assert_cast<const ColumnType *>(
        _reference_column);
    // Note that the storage type of offset returned by get<> is different, so
    // we need to specify the type explicitly.
    const typename ColumnType::value_type offset
        = _offset.get<typename ColumnType::value_type>();
    assert(offset >= 0);
    const auto compared_value_data = compared_column->getDataAt(compared_row);
    assert(compared_value_data.size == sizeof(typename ColumnType::value_type));
    auto compared_value = unalignedLoad<typename ColumnType::value_type>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(typename ColumnType::value_type));
    auto reference_value = unalignedLoad<typename ColumnType::value_type>(
        reference_value_data.data);

    bool is_overflow;
    if (offset_is_preceding)
        is_overflow = common::subOverflow(reference_value, offset, reference_value);
    else
        is_overflow = common::addOverflow(reference_value, offset, reference_value);

    if (is_overflow)
    {
        if (offset_is_preceding)
        {
            // Overflow to the negative, [compared] must be greater.
            // We know that because offset is >= 0.
            return 1;
        }
        else
        {
            // Overflow to the positive, [compared] must be less.
            return -1;
        }
    }
    else
    {
        // No overflow, compare normally.
        return compared_value < reference_value ? -1
            : compared_value == reference_value ? 0
                                                : 1;
    }
}

// A specialization of compareValuesWithOffset for floats.
template <typename ColumnType>
static int compareValuesWithOffsetFloat(const IColumn * _compared_column,
                                        size_t compared_row,
                                        const IColumn * _reference_column,
                                        size_t reference_row,
                                        const Field & _offset,
                                        bool offset_is_preceding)
{
    // Casting the columns to the known type here makes it faster, probably
    // because the getData call can be devirtualized.
    const auto * compared_column = assert_cast<const ColumnType *>(
        _compared_column);
    const auto * reference_column = assert_cast<const ColumnType *>(
        _reference_column);
    const auto offset = _offset.get<typename ColumnType::value_type>();
    assert(offset >= 0);

    const auto compared_value_data = compared_column->getDataAt(compared_row);
    assert(compared_value_data.size == sizeof(typename ColumnType::value_type));
    auto compared_value = unalignedLoad<typename ColumnType::value_type>(
        compared_value_data.data);

    const auto reference_value_data = reference_column->getDataAt(reference_row);
    assert(reference_value_data.size == sizeof(typename ColumnType::value_type));
    auto reference_value = unalignedLoad<typename ColumnType::value_type>(
        reference_value_data.data);

    // Floats overflow to Inf and the comparison will work normally, so we don't
    // have to do anything.
    if (offset_is_preceding)
    {
        reference_value -= offset;
    }
    else
    {
        reference_value += offset;
    }

    const auto result = compared_value < reference_value ? -1
        : compared_value == reference_value              ? 0
                                                         : 1;

    return result;
}

// Helper macros to dispatch on type of the ORDER BY column
#define APPLY_FOR_ONE_TYPE(FUNCTION, TYPE)                                 \
    else if (typeid_cast<const TYPE *>(column))                            \
    {                                                                      \
        /* clang-tidy you're dumb, I can't put FUNCTION in braces here. */ \
        compare_values_with_offset = FUNCTION<TYPE>; /* NOLINT */          \
    }

#define APPLY_FOR_TYPES(FUNCTION)                                                             \
    if (false) /* NOLINT */                                                                   \
    {                                                                                         \
        /* Do nothing, a starter condition. */                                                \
    }                                                                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt8>)                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt16>)                                        \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt32>)                                        \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<UInt64>)                                        \
                                                                                              \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int8>)                                          \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int16>)                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int32>)                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int64>)                                         \
    APPLY_FOR_ONE_TYPE(FUNCTION, ColumnVector<Int128>)                                        \
                                                                                              \
    APPLY_FOR_ONE_TYPE(FUNCTION##Float, ColumnVector<Float32>)                                \
    APPLY_FOR_ONE_TYPE(FUNCTION##Float, ColumnVector<Float64>)                                \
                                                                                              \
    else                                                                                      \
    {                                                                                         \
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,                                          \
                        "The RANGE OFFSET frame for '{}' ORDER BY column is not implemented", \
                        demangle(typeid(*column).name()));                                    \
    }

WindowBlockInputStream::WindowBlockInputStream(const BlockInputStreamPtr & input, const WindowDescription & window_description_)
    : window_description(window_description_)
{
    children.push_back(input);
    input_header = input->getHeader();
    for (auto & add_column : window_description_.add_columns)
    {
        input_header.insert({add_column.type, add_column.name});
    }
    auto input_columns = input_header.getColumns();

    if (window_description.window_functions_descriptions.size() * window_description.aggregate_descriptions.size() != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "can not have window and agg functions together in one window");
    }

    initialWorkspaces();

    initialPartitionByIndices();

    checkRangeOffsetFrameValid();
}

void WindowBlockInputStream::checkRangeOffsetFrameValid()
{
    // Choose a row comparison function for RANGE OFFSET frame based on the
    // type of the ORDER BY column.
    if (window_description.frame.type == WindowFrame::FrameType::Ranges
        && (window_description.frame.begin_type
                == WindowFrame::BoundaryType::Offset
            || window_description.frame.end_type
                == WindowFrame::BoundaryType::Offset))
    {
        assert(order_by_indices.size() == 1);
        const auto & entry = input_header.getByPosition(order_by_indices[0]);
        const IColumn * column = entry.column.get();
        APPLY_FOR_TYPES(compareValuesWithOffset)

        // Convert the offsets to the ORDER BY column type. We can't just check
        // that the type matches, because e.g. the int literals are always
        // (U)Int64, but the column might be Int8 and so on.
        if (window_description.frame.begin_type
            == WindowFrame::BoundaryType::Offset)
        {
            window_description.frame.begin_offset = convertFieldToTypeOrThrow(
                window_description.frame.begin_offset,
                *entry.type);

            if (applyVisitor(FieldVisitorAccurateLess{},
                             window_description.frame.begin_offset,
                             Field(Int64(0))))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Window frame start offset must be nonnegative, {} given",
                                window_description.frame.begin_offset.toString());
            }
        }
        if (window_description.frame.end_type
            == WindowFrame::BoundaryType::Offset)
        {
            window_description.frame.end_offset = convertFieldToTypeOrThrow(
                window_description.frame.end_offset,
                *entry.type);

            if (applyVisitor(FieldVisitorAccurateLess{},
                             window_description.frame.end_offset,
                             Field(Int64(0))))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Window frame start offset must be nonnegative, {} given",
                                window_description.frame.end_offset.toString());
            }
        }
    }
}

void WindowBlockInputStream::initialPartitionByIndices()
{
    partition_by_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_by_indices.push_back(
            input_header.getPositionByName(column.column_name));
    }

    order_by_indices.reserve(window_description.order_by.size());
    for (const auto & column : window_description.order_by)
    {
        order_by_indices.push_back(
            input_header.getPositionByName(column.column_name));
    }
}

void WindowBlockInputStream::initialWorkspaces()
{
    // Initialize window function workspaces.
    workspaces.reserve(window_description.window_functions_descriptions.size() + window_description.aggregate_descriptions.size());

    for (auto window_function_description : window_description.window_functions_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = window_function_description.window_function;
        workspace.column_name = window_function_description.column_name;
        workspace.argument_column_indices = window_function_description.arguments;
        workspace.argument_columns.assign(window_function_description.argument_names.size(), nullptr);
        workspace.is_agg_workspace = false;
        workspaces.push_back(std::move(workspace));
    }

    for (auto aggregate_description : window_description.aggregate_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.aggregate_function = aggregate_description.function;
        const auto & aggregate_function = workspace.aggregate_function;
        if (!arena && aggregate_function->allocatesMemoryInArena())
        {
            arena = std::make_unique<Arena>();
        }
        workspace.argument_column_indices = aggregate_description.arguments;
        workspace.column_name = aggregate_description.column_name;
        workspace.argument_columns.assign(aggregate_description.argument_names.size(), nullptr);
        workspace.aggregate_function_state.reset(
            aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(workspace.aggregate_function_state.data());
        workspace.is_agg_workspace = true;
        workspaces.push_back(std::move(workspace));
    }
}

bool WindowBlockInputStream::outputBlockEmpty()
{
    if (output_index >= output_blocks.size())
        return true;
    return false;
}

Block WindowBlockInputStream::getOutputBlock()
{
    if (outputBlockEmpty())
        return {};

    int output_column_index = 0;
    for (auto & workspace : workspaces)
    {
        output_blocks[output_index].getByPosition(workspace.result).column = std::move(window_blocks[output_index].output_columns[output_column_index++]);
    }
    return output_blocks[output_index++];
}

Block WindowBlockInputStream::readImpl()
{
    const auto & stream = children.back();
    while (!input_is_finished)
    {
        Block block = stream->read();
        if (!block)
            input_is_finished = true;
        // if input_is_finished is true, we will do noting to the null block, and we need handle the last partition_start to partition_end data
        appendBlock(block);
    }

    return getOutputBlock();
}

void WindowBlockInputStream::advancePartitionEnd()
{
    if (partition_ended)
    {
        return;
    }

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
    const size_t partition_by_columns = partition_by_indices.size();
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
    for (; partition_end.row < block_rows; ++partition_end.row)
    {
        size_t i = 0;
        for (; i < partition_by_columns; i++)
        {
            const auto * reference_column
                = inputAt(prev_frame_start)[partition_by_indices[i]].get();
            const auto * compared_column
                = inputAt(partition_end)[partition_by_indices[i]].get();
            if (compared_column->compareAt(partition_end.row,
                                           prev_frame_start.row,
                                           *reference_column,
                                           1 /* nan_direction_hint */)
                != 0)
            {
                break;
            }
        }

        if (i < partition_by_columns)
        {
            partition_ended = true;
            return;
        }
    }

    // Went until the end of block, go to the next.
    assert(partition_end.row == block_rows);
    ++partition_end.block;
    partition_end.row = 0;

    // Went until the end of data and didn't find the new partition.
    assert(!partition_ended && partition_end == blocksEnd());
}

std::tuple<RowNumber, int64_t> WindowBlockInputStream::moveRowNumberNoCheck(const RowNumber & _x, int64_t offset) const
{
    RowNumber x = _x;

    if (offset > 0)
    {
        for (;;)
        {
            assertValid(x);
            assert(offset >= 0);

            const auto block_rows = blockRowsNumber(x);
            x.row += offset;
            if (x.row >= block_rows)
            {
                offset = x.row - block_rows;
                x.row = 0;
                x.block++;

                if (x == blocksEnd())
                {
                    break;
                }
            }
            else
            {
                offset = 0;
                break;
            }
        }
    }
    else if (offset < 0)
    {
        for (;;)
        {
            assertValid(x);
            assert(offset <= 0);

            // abs(offset) is less than INT64_MAX, as checked in the parser, so
            // this negation should always work.
            assert(offset >= -INT64_MAX);
            if (x.row >= static_cast<uint64_t>(-offset))
            {
                x.row -= -offset;
                offset = 0;
                break;
            }

            // Move to the first row in current block. Note that the offset is
            // negative.
            offset += x.row;
            x.row = 0;

            // Move to the last row of the previous block, if we are not at the
            // first one. Offset also is incremented by one, because we pass over
            // the first row of this block.
            if (x.block == first_block_number)
            {
                break;
            }

            --x.block;
            offset += 1;
            x.row = blockRowsNumber(x) - 1;
        }
    }

    return std::tuple{x, offset};
}

std::tuple<RowNumber, int64_t> WindowBlockInputStream::moveRowNumber(const RowNumber & _x, int64_t offset) const
{
    auto [x, o] = moveRowNumberNoCheck(_x, offset);

#ifndef NDEBUG
    // Check that it was reversible.
    auto [xx, oo] = moveRowNumberNoCheck(x, -(offset - o));
    assert(xx == _x);
    assert(oo == 0);
#endif

    return std::tuple{x, o};
}


void WindowBlockInputStream::advanceFrameStartRowsOffset()
{
    // Just recalculate it each time by walking blocks.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
                                                        window_description.frame.begin_offset.get<UInt64>()
                                                            * (window_description.frame.begin_preceding ? -1 : 1));

    frame_start = moved_row;

    assertValid(frame_start);

    if (frame_start <= partition_start)
    {
        // Got to the beginning of partition and can't go further back.
        frame_start = partition_start;
        frame_started = true;
        return;
    }

    if (partition_end <= frame_start)
    {
        // A FOLLOWING frame start ran into the end of partition.
        frame_start = partition_end;
        frame_started = partition_ended;
        return;
    }

    // Handled the equality case above. Now the frame start is inside the
    // partition, if we walked all the offset, it's final.
    assert(partition_start < frame_start);
    frame_started = offset_left == 0;

    // If we ran into the start of data (offset left is negative), we won't be
    // able to make progress. Should have handled this case above.
    assert(offset_left >= 0);
}


void WindowBlockInputStream::advanceFrameStartRangeOffset()
{
    // See the comment for advanceFrameEndRangeOffset().
    const int direction = window_description.order_by[0].direction;
    const bool preceding = window_description.frame.begin_preceding
        == (direction > 0);
    const auto * reference_column
        = inputAt(current_row)[order_by_indices[0]].get();
    for (; frame_start < partition_end; advanceRowNumber(frame_start))
    {
        // The first frame value is [current_row] with offset, so we advance
        // while [frames_start] < [current_row] with offset.
        const auto * compared_column
            = inputAt(frame_start)[order_by_indices[0]].get();
        if (compare_values_with_offset(compared_column, frame_start.row, reference_column, current_row.row, window_description.frame.begin_offset, preceding) * direction >= 0)
        {
            frame_started = true;
            return;
        }
    }
    frame_started = partition_ended;
}

void WindowBlockInputStream::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    const auto frame_start_before = frame_start;

    switch (window_description.frame.begin_type)
    {
    case WindowFrame::BoundaryType::Unbounded:
        // UNBOUNDED PRECEDING, just mark it valid. It is initialized when
        // the new partition starts.
        frame_started = true;
        break;
    case WindowFrame::BoundaryType::Current:
        // CURRENT ROW differs between frame types only in how the peer
        // groups are accounted.
        assert(partition_start <= peer_group_start);
        assert(peer_group_start < partition_end);
        assert(peer_group_start <= current_row);
        frame_start = peer_group_start;
        frame_started = true;
        break;
    case WindowFrame::BoundaryType::Offset:
        switch (window_description.frame.type)
        {
        case WindowFrame::FrameType::Rows:
            advanceFrameStartRowsOffset();
            break;
        case WindowFrame::FrameType::Ranges:
            advanceFrameStartRangeOffset();
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Frame start type '{}' for frame '{}' is not implemented",
                            window_description.frame.begin_type,
                            window_description.frame.type);
        }
        break;
    }

    assert(frame_start_before <= frame_start);
    if (frame_start == frame_start_before)
    {
        // If the frame start didn't move, this means we validated that the frame
        // starts at the point we reached earlier but were unable to validate.
        // This probably only happens in degenerate cases where the frame start
        // is further than the end of partition, and the partition ends at the
        // last row of the block, but we can only tell for sure after a new
        // block arrives. We still have to update the state of aggregate
        // functions when the frame start becomes valid, so we continue.
        assert(frame_started);
    }

    assert(partition_start <= frame_start);
    assert(frame_start <= partition_end);
    if (partition_ended && frame_start == partition_end)
    {
        // Check that if the start of frame (e.g. FOLLOWING) runs into the end
        // of partition, it is marked as valid -- we can't advance it any
        // further.
        assert(frame_started);
    }
}

bool WindowBlockInputStream::arePeers(const RowNumber & x, const RowNumber & y) const
{
    if (x == y)
    {
        // For convenience, a row is always its own peer.
        return true;
    }

    if (window_description.frame.type == WindowFrame::FrameType::Rows)
    {
        // For ROWS frame, row is only peers with itself (checked above);
        return false;
    }

    // For RANGE and GROUPS frames, rows that compare equal w/ORDER BY are peers.
    assert(window_description.frame.type == WindowFrame::FrameType::Ranges);
    const size_t n = order_by_indices.size();
    if (n == 0)
    {
        // No ORDER BY, so all rows are peers.
        return true;
    }

    size_t i = 0;
    for (; i < n; i++)
    {
        const auto * column_x = inputAt(x)[order_by_indices[i]].get();
        const auto * column_y = inputAt(y)[order_by_indices[i]].get();
        if (column_x->compareAt(x.row, y.row, *column_y, 1 /* nan_direction_hint */) != 0)
        {
            return false;
        }
    }

    return true;
}

void WindowBlockInputStream::advanceFrameEndCurrentRow()
{
    // We only process one block here, and frame_end must be already in it: if
    // we didn't find the end in the previous block, frame_end is now the first
    // row of the current block. We need this knowledge to write a simpler loop
    // (only loop over rows and not over blocks), that should hopefully be more
    // efficient.
    // partition_end is either in this new block or past-the-end.
    assert(frame_end.block == partition_end.block
           || frame_end.block + 1 == partition_end.block);

    if (frame_end == partition_end)
    {
        // The case when we get a new block and find out that the partition has
        // ended.
        assert(partition_ended);
        frame_ended = partition_ended;
        return;
    }

    // We advance until the partition end. It's either in the current block or
    // in the next one, which is also the past-the-end block. Figure out how
    // many rows we have to process.
    uint64_t rows_end;
    if (partition_end.row == 0)
    {
        assert(partition_end == blocksEnd());
        rows_end = blockRowsNumber(frame_end);
    }
    else
    {
        assert(frame_end.block == partition_end.block);
        rows_end = partition_end.row;
    }
    // Equality would mean "no data to process", for which we checked above.
    assert(frame_end.row < rows_end);

    // Advance frame_end while it is still peers with the current row.
    for (; frame_end.row < rows_end; ++frame_end.row)
    {
        if (!arePeers(current_row, frame_end))
        {
            frame_ended = true;
            return;
        }
    }

    // Might have gotten to the end of the current block, have to properly
    // update the row number.
    if (frame_end.row == blockRowsNumber(frame_end))
    {
        ++frame_end.block;
        frame_end.row = 0;
    }

    // Got to the end of partition (frame ended as well then) or end of data.
    assert(frame_end == partition_end);
    frame_ended = partition_ended;
}

void WindowBlockInputStream::advanceFrameEndUnbounded()
{
    // The UNBOUNDED FOLLOWING frame ends when the partition ends.
    frame_end = partition_end;
    frame_ended = partition_ended;
}

void WindowBlockInputStream::advanceFrameEndRowsOffset()
{
    // Walk the specified offset from the current row. The "+1" is needed
    // because the frame_end is a past-the-end pointer.
    const auto [moved_row, offset_left] = moveRowNumber(current_row,
                                                        window_description.frame.end_offset.get<UInt64>()
                                                                * (window_description.frame.end_preceding ? -1 : 1)
                                                            + 1);

    if (partition_end <= moved_row)
    {
        // Clamp to the end of partition. It might not have ended yet, in which
        // case wait for more data.
        frame_end = partition_end;
        frame_ended = partition_ended;
        return;
    }

    if (moved_row <= partition_start)
    {
        // Clamp to the start of partition.
        frame_end = partition_start;
        frame_ended = true;
        return;
    }

    // Frame end inside partition, if we walked all the offset, it's final.
    frame_end = moved_row;
    frame_ended = offset_left == 0;

    // If we ran into the start of data (offset left is negative), we won't be
    // able to make progress. Should have handled this case above.
    assert(offset_left >= 0);
}

void WindowBlockInputStream::advanceFrameEndRangeOffset()
{
    // PRECEDING/FOLLOWING change direction for DESC order.
    const int direction = window_description.order_by[0].direction;
    const bool preceding = window_description.frame.end_preceding
        == (direction > 0);
    const auto * reference_column
        = inputAt(current_row)[order_by_indices[0]].get();
    for (; frame_end < partition_end; advanceRowNumber(frame_end))
    {
        // The last frame value is current_row with offset, and we need a
        // past-the-end pointer, so we advance while
        // [frame_end] <= [current_row] with offset.
        const auto * compared_column
            = inputAt(frame_end)[order_by_indices[0]].get();
        if (compare_values_with_offset(compared_column, frame_end.row, reference_column, current_row.row, window_description.frame.end_offset, preceding)
                * direction
            > 0)
        {
            frame_ended = true;
            return;
        }
    }

    frame_ended = partition_ended;
}

void WindowBlockInputStream::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    assert(!frame_ended);

    const auto frame_end_before = frame_end;

    switch (window_description.frame.end_type)
    {
    case WindowFrame::BoundaryType::Current:
        advanceFrameEndCurrentRow();
        break;
    case WindowFrame::BoundaryType::Unbounded:
        advanceFrameEndUnbounded();
        break;
    case WindowFrame::BoundaryType::Offset:
        switch (window_description.frame.type)
        {
        case WindowFrame::FrameType::Rows:
            advanceFrameEndRowsOffset();
            break;
        case WindowFrame::FrameType::Ranges:
            advanceFrameEndRangeOffset();
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "The frame end type '{}' is not implemented",
                            window_description.frame.end_type);
        }
        break;
    }

    // We might not have advanced the frame end if we found out we reached the
    // end of input or the partition, or if we still don't know the frame start.
    if (frame_end_before == frame_end)
    {
        return;
    }
}

// Update the aggregation states after the frame has changed.
void WindowBlockInputStream::updateAggregationState()
{
    // Assert that the frame boundaries are known, have proper order wrt each
    // other, and have not gone back wrt the previous frame.
    assert(frame_started);
    assert(frame_ended);
    assert(frame_start <= frame_end);
    assert(prev_frame_start <= prev_frame_end);
    assert(prev_frame_start <= frame_start);
    assert(prev_frame_end <= frame_end);
    assert(partition_start <= frame_start);
    assert(frame_end <= partition_end);

    // We might have to reset aggregation state and/or add some rows to it.
    // Figure out what to do.
    bool reset_aggregation = false;
    RowNumber rows_to_add_start;
    RowNumber rows_to_add_end;
    if (frame_start == prev_frame_start)
    {
        // The frame start didn't change, add the tail rows.
        reset_aggregation = false;
        rows_to_add_start = prev_frame_end;
        rows_to_add_end = frame_end;
    }
    else
    {
        // The frame start changed, reset the state and aggregate over the
        // entire frame. This can be made per-function after we learn to
        // subtract rows from some types of aggregation states, but for now we
        // always have to reset when the frame start changes.
        reset_aggregation = true;
        rows_to_add_start = frame_start;
        rows_to_add_end = frame_end;
    }

    for (auto & ws : workspaces)
    {
        if (!ws.is_agg_workspace)
        {
            // No need to do anything for true window functions.
            continue;
        }

        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (reset_aggregation)
        {
            a->destroy(buf);
            a->create(buf);
        }

        // To achieve better performance, we will have to loop over blocks and
        // rows manually, instead of using advanceRowNumber().
        // For this purpose, the past-the-end block can be different than the
        // block of the past-the-end row (it's usually the next block).
        const auto past_the_end_block = rows_to_add_end.row == 0
            ? rows_to_add_end.block
            : rows_to_add_end.block + 1;

        for (auto block_number = rows_to_add_start.block;
             block_number < past_the_end_block;
             ++block_number)
        {
            auto & block = blockAt(block_number);

            if (ws.cached_block_number != block_number)
            {
                for (size_t i = 0; i < ws.argument_column_indices.size(); ++i)
                {
                    ws.argument_columns[i] = block.input_columns[ws.argument_column_indices[i]].get();
                }
                ws.cached_block_number = block_number;
            }

            // First and last blocks may be processed partially, and other blocks
            // are processed in full.
            const auto first_row = block_number == rows_to_add_start.block
                ? rows_to_add_start.row
                : 0;
            const auto past_the_end_row = block_number == rows_to_add_end.block
                ? rows_to_add_end.row
                : block.rows;

            // We should add an addBatch analog that can accept a starting offset.
            // For now, add the values one by one.
            auto * columns = ws.argument_columns.data();
            // Removing arena.get() from the loop makes it faster somehow...
            auto * arena_ptr = arena.get();
            for (auto row = first_row; row < past_the_end_row; ++row)
            {
                a->add(buf, columns, row, arena_ptr);
            }
        }
    }

    prev_frame_start = frame_start;
    prev_frame_end = frame_end;
}

void WindowBlockInputStream::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    const auto & block = blockAt(current_row);
    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];

        if (!ws.is_agg_workspace)
        {
            ws.window_function->windowInsertResultInto(this->shared_from_this(), wi);
        }
        else
        {
            IColumn * result_column = block.output_columns[wi].get();
            const auto * a = ws.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();
            // We'll have to pass it out with blocks then...
            a->insertResultInto(buf, *result_column, arena.get());
        }
    }
}

static void assertSameColumns(const Columns & left_all,
                              const Columns & right_all)
{
    assert(left_all.size() == right_all.size());

    for (size_t i = 0; i < left_all.size(); ++i)
    {
        const auto * left_column = left_all[i].get();
        const auto * right_column = right_all[i].get();

        assert(left_column);
        assert(right_column);

        assert(typeid(*left_column).hash_code()
               == typeid(*right_column).hash_code());

        if ((*left_column).isColumnConst())
        {
            Field left_value = assert_cast<const ColumnConst &>(*left_column).getField();
            Field right_value = assert_cast<const ColumnConst &>(*right_column).getField();

            assert(left_value == right_value);
        }
    }
}

void WindowBlockInputStream::appendBlock(Block & current_block_)
{
    if (!input_is_finished)
    {
        if (current_block_.rows() == 0)
        {
            return;
        }

        output_blocks.push_back(current_block_);
        auto & current_block = output_blocks.back();

        window_blocks.push_back({});
        auto & window_block = window_blocks.back();

        window_block.rows = current_block.rows();

        // Initialize output columns and add new columns to output block.
        for (auto & ws : workspaces)
        {
            ws.result = current_block.columns();
            if (ws.is_agg_workspace)
            {
                window_block.output_columns.push_back(ws.aggregate_function->getReturnType()->createColumn());
                current_block.insert({ws.aggregate_function->getReturnType(), ws.column_name});
            }
            else
            {
                window_block.output_columns.push_back(ws.window_function->getReturnType()->createColumn());
                current_block.insert({ws.window_function->getReturnType(), ws.column_name});
            }
        }

        window_block.input_columns = current_block.getColumns();

        assertSameColumns(input_header.getColumns(), window_block.input_columns);
    }

    for (;;)
    {
        advancePartitionEnd();

        assert(partition_ended || partition_end == blocksEnd());
        if (partition_ended && partition_end == blocksEnd())
        {
            assert(input_is_finished);
        }

        while (current_row < partition_end)
        {
            // We now know that the current row is valid, so we can update the
            // peer group start.
            if (!arePeers(peer_group_start, current_row))
            {
                peer_group_start = current_row;
                peer_group_start_row_number = current_row_number;
                ++peer_group_number;
            }

            // Advance the frame start.
            advanceFrameStart();

            if (!frame_started)
            {
                // Wait for more input data to find the start of frame.
                assert(!input_is_finished);
                assert(!partition_ended);
                return;
            }

            // frame_end must be greater or equal than frame_start, so if the
            // frame_start is already past the current frame_end, we can start
            // from it to save us some work.
            if (frame_end < frame_start)
            {
                frame_end = frame_start;
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

            // Now that we know the new frame boundaries, update the aggregation
            // states. Theoretically we could do this simultaneously with moving
            // the frame boundaries, but it would require some care not to
            // perform unnecessary work while we are still looking for the frame
            // start, so do it the simple way for now.
            updateAggregationState();

            // Write out the aggregation results.
            writeOutCurrentRow();

            if (isCancelled())
            {
                // Good time to check if the query is cancelled. Checking once
                // per block might not be enough in severe quadratic cases.
                // Just leave the work halfway through and return, the 'prepare'
                // method will figure out what to do. Note that this doesn't
                // handle 'max_execution_time' and other limits, because these
                // limits are only updated between blocks. Eventually we should
                // start updating them in background and canceling the processor,
                // like we do for Ctrl+C handling.
                //
                // This class is final, so the check should hopefully be
                // devirtualized and become a single never-taken branch that is
                // basically free.
                return;
            }

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
        // We have to reset the frame and other pointers when the new partition
        // starts.
        frame_start = partition_start;
        frame_end = partition_start;
        prev_frame_start = partition_start;
        prev_frame_end = partition_start;
        assert(current_row == partition_start);
        current_row_number = 1;
        peer_group_start = partition_start;
        peer_group_start_row_number = 1;
        peer_group_number = 1;


        // Reinitialize the aggregate function states because the new partition
        // has started.
        for (auto & ws : workspaces)
        {
            if (!ws.is_agg_workspace)
            {
                continue;
            }

            const auto * a = ws.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();

            a->destroy(buf);
        }

        if (arena)
        {
            arena = std::make_unique<Arena>();
        }

        for (auto & ws : workspaces)
        {
            if (!ws.is_agg_workspace)
            {
                continue;
            }

            const auto * a = ws.aggregate_function.get();
            auto * buf = ws.aggregate_function_state.data();

            a->create(buf);
        }
    }
}

} // namespace DB