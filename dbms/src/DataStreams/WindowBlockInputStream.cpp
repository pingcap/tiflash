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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Common/Arena.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

WindowBlockInputStream::WindowBlockInputStream(const BlockInputStreamPtr & input, const WindowDescription & window_description_)
    : window_description(window_description_)
{
    children.push_back(input);
    output_header = input->getHeader();
    for (const auto & add_column : window_description_.add_columns)
    {
        output_header.insert({add_column.type, add_column.name});
    }

    if (window_description.window_functions_descriptions.size() * window_description.aggregate_descriptions.size() != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "can not have window and agg functions together in one window");
    }

    initialWorkspaces();

    initialPartitionByIndices();
}


void WindowBlockInputStream::initialPartitionByIndices()
{
    partition_by_indices.reserve(window_description.partition_by.size());
    for (const auto & column : window_description.partition_by)
    {
        partition_by_indices.push_back(
            output_header.getPositionByName(column.column_name));
    }

    order_by_indices.reserve(window_description.order_by.size());
    for (const auto & column : window_description.order_by)
    {
        order_by_indices.push_back(
            output_header.getPositionByName(column.column_name));
    }
}

void WindowBlockInputStream::initialWorkspaces()
{
    // Initialize window function workspaces.
    workspaces.reserve(window_description.window_functions_descriptions.size() + window_description.aggregate_descriptions.size());

    for (const auto & window_function_description : window_description.window_functions_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = window_function_description.window_function;
        workspace.column_name = window_function_description.column_name;
        workspace.argument_columns.assign(window_function_description.argument_names.size(), nullptr);
        workspace.is_agg_workspace = false;
        workspaces.push_back(std::move(workspace));
    }

    // TODO: handle agg functions
    for (auto aggregate_description : window_description.aggregate_descriptions)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Unsupport agg function in window.");
    }
}

Block WindowBlockInputStream::readImpl()
{
    const auto & stream = children.back();
    while (!input_is_finished)
    {
        if (Block output_block = getOutputBlock())
        {
            return output_block;
        }

        Block block = stream->read();
        if (!block)
            input_is_finished = true;
        appendBlock(block);
    }

    // return last partition block, if already return then return null
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
        for (; i < partition_by_columns; ++i)
        {
            const auto reference_column
                = inputAt(prev_frame_start)[partition_by_indices[i]];
            const auto * compared_column
                = inputAt(partition_end)[partition_by_indices[i]].get();
            if (window_description.partition_by[i].collator)
            {
                if (compared_column->compareAtWithCollation(partition_end.row,
                                                            prev_frame_start.row,
                                                            *reference_column,
                                                            1 /* nan_direction_hint */,
                                                            *window_description.partition_by[i].collator)
                    != 0)
                {
                    break;
                }
            }
            else
            {
                if (compared_column->compareAt(partition_end.row,
                                               prev_frame_start.row,
                                               *reference_column,
                                               1 /* nan_direction_hint */)
                    != 0)
                {
                    break;
                }
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
            assert(x.block >= 0);
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

void WindowBlockInputStream::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    const auto frame_start_before = frame_start;

    // switch for another frame type
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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "The frame begin type '{}' is not implemented",
                        window_description.frame.begin_type);
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


    for (size_t i = 0; i < n; ++i)
    {
        const auto * column_x = inputAt(x)[order_by_indices[i]].get();
        const auto * column_y = inputAt(y)[order_by_indices[i]].get();
        if (window_description.order_by[i].collator)
        {
            if (column_x->compareAtWithCollation(x.row, y.row, *column_y, 1 /* nan_direction_hint */, *window_description.order_by[i].collator) != 0)
            {
                return false;
            }
        }
        else
        {
            if (column_x->compareAt(x.row, y.row, *column_y, 1 /* nan_direction_hint */) != 0)
            {
                return false;
            }
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

void WindowBlockInputStream::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    assert(!frame_ended);

    const auto frame_end_before = frame_end;

    // switch for another frame type
    switch (window_description.frame.end_type)
    {
    case WindowFrame::BoundaryType::Current:
        advanceFrameEndCurrentRow();
        break;
    case WindowFrame::BoundaryType::Unbounded:
        advanceFrameEndUnbounded();
        break;
    case WindowFrame::BoundaryType::Offset:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "The frame end type '{}' is not implemented",
                        window_description.frame.end_type);
    }

    // We might not have advanced the frame end if we found out we reached the
    // end of input or the partition, or if we still don't know the frame start.
    if (frame_end_before == frame_end)
    {
        return;
    }
}

void WindowBlockInputStream::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];

        if (!ws.is_agg_workspace)
        {
            ws.window_function->windowInsertResultInto(this->shared_from_this(), wi);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                            "Unsupport agg function in window.");
        }
    }
}

Block WindowBlockInputStream::getOutputBlock()
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

void WindowBlockInputStream::releaseAlreadyOutputWindowBlock()
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
    const auto first_used_block = std::min(next_output_block_number,
                                           std::min(prev_frame_start.block, current_row.block));

    if (first_block_number < first_used_block)
    {
        //        std::cout<<fmt::format("will drop blocks from {} to {}\n", first_block_number, first_used_block)<<std::endl;

        window_blocks.erase(window_blocks.begin(),
                            window_blocks.begin() + (first_used_block - first_block_number));
        first_block_number = first_used_block;

        assert(next_output_block_number >= first_block_number);
        assert(frame_start.block >= first_block_number);
        assert(prev_frame_start.block >= first_block_number);
        assert(current_row.block >= first_block_number);
        assert(peer_group_start.block >= first_block_number);
    }
}

void WindowBlockInputStream::appendBlock(Block & current_block)
{
    if (!input_is_finished)
    {
        if (current_block.rows() == 0)
        {
            return;
        }

        window_blocks.push_back({});
        auto & window_block = window_blocks.back();

        window_block.rows = current_block.rows();

        // Initialize output columns and add new columns to output block.
        for (auto & ws : workspaces)
        {
            ws.result = current_block.columns();
            if (ws.is_agg_workspace)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "agg functions in window are not implemented");
            }
            else
            {
                window_block.output_columns.push_back(ws.window_function->getReturnType()->createColumn());
            }
        }

        window_block.input_columns = current_block.getColumns();
    }

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

            // Write out the results.
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
        assert(current_row == partition_start);
        current_row_number = 1;
        peer_group_start = partition_start;
        peer_group_start_row_number = 1;
        peer_group_number = 1;
    }
}
} // namespace DB
