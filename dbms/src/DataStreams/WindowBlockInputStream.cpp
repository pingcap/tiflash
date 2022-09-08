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

WindowBlockInputStream::WindowBlockInputStream(const BlockInputStreamPtr & input, const WindowDescription & window_description_, const String & req_id)
    : log(Logger::get(NAME, req_id))
    , window_description(window_description_)
{
    children.push_back(input);
    output_header = input->getHeader();
    for (const auto & add_column : window_description_.add_columns)
    {
        output_header.insert({add_column.type, add_column.name});
    }

    initialWorkspaces();

    initialPartitionAndOrderColumnIndices();
}


void WindowBlockInputStream::initialPartitionAndOrderColumnIndices()
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

void WindowBlockInputStream::initialWorkspaces()
{
    // Initialize window function workspaces.
    workspaces.reserve(window_description.window_functions_descriptions.size());

    for (const auto & window_function_description : window_description.window_functions_descriptions)
    {
        WindowFunctionWorkspace workspace;
        workspace.window_function = window_function_description.window_function;
        workspaces.push_back(std::move(workspace));
    }
    only_have_row_number = onlyHaveRowNumber();
    only_have_pure_window = onlyHaveRowNumberAndRank();
}

bool WindowBlockInputStream::returnIfCancelledOrKilled()
{
    if (isCancelledOrThrowIfKilled())
    {
        if (!window_blocks.empty())
            window_blocks.erase(window_blocks.begin(), window_blocks.end());
        input_is_finished = true;
        return true;
    }
    return false;
}

Block WindowBlockInputStream::readImpl()
{
    const auto & stream = children.back();
    while (!input_is_finished)
    {
        if (returnIfCancelledOrKilled())
            return {};

        if (Block output_block = tryGetOutputBlock())
            return output_block;

        Block block = stream->read();
        if (!block)
            input_is_finished = true;
        else
            appendBlock(block);
        tryCalculate();
    }

    if (returnIfCancelledOrKilled())
        return {};
    // return last partition block, if already return then return null
    return tryGetOutputBlock();
}

// Judge whether current_partition_row is end row of partition in current block
bool WindowBlockInputStream::isDifferentFromPrevPartition(UInt64 current_partition_row)
{
    const auto reference_columns = inputAt(prev_frame_start);
    const auto compared_columns = inputAt(partition_end);

    for (size_t i = 0; i < partition_column_indices.size(); ++i)
    {
        const auto reference_column = reference_columns[partition_column_indices[i]];
        const auto * compared_column = compared_columns[partition_column_indices[i]].get();
        if (window_description.partition_by[i].collator)
        {
            if (compared_column->compareAtWithCollation(current_partition_row,
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

void WindowBlockInputStream::advancePartitionEnd()
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
Int64 WindowBlockInputStream::getPartitionEndRow(size_t block_rows)
{
    Int64 left = partition_end.row;
    Int64 right = block_rows - 1;

    while (left <= right)
    {
        Int64 middle = left + (right - left) / 2;
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

void WindowBlockInputStream::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    if (only_have_pure_window)
    {
        frame_start = current_row;
        frame_started = true;
        return;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "window function only support pure window function now.");
}

bool WindowBlockInputStream::arePeers(const RowNumber & x, const RowNumber & y) const
{
    if (x == y)
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
            const auto * column_x = inputAt(x)[order_column_indices[i]].get();
            const auto * column_y = inputAt(y)[order_column_indices[i]].get();
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
    case WindowFrame::FrameType::Groups:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "window function only support frame type row and range.");
    }
}

void WindowBlockInputStream::advanceFrameEndCurrentRow()
{
    assert(frame_end.block == partition_end.block
           || frame_end.block + 1 == partition_end.block);

    // If window only have row_number or rank/dense_rank functions, set frame_end to the next row of current_row and frame_ended to true
    if (only_have_pure_window)
    {
        frame_end = current_row;
        advanceRowNumber(frame_end);
        frame_ended = true;
        return;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "window function only support pure window function now.");
}

void WindowBlockInputStream::advanceFrameEnd()
{
    // frame_end must be greater or equal than frame_start, so if the
    // frame_start is already past the current frame_end, we can start
    // from it to save us some work.
    if (frame_end < frame_start)
    {
        frame_end = frame_start;
    }

    // No reason for this function to be called again after it succeeded.
    assert(!frame_ended);

    // switch for another frame type
    switch (window_description.frame.end_type)
    {
    case WindowFrame::BoundaryType::Current:
        advanceFrameEndCurrentRow();
        break;
    case WindowFrame::BoundaryType::Unbounded:
    case WindowFrame::BoundaryType::Offset:
    default:
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "The frame end type '{}' is not implemented",
                        window_description.frame.end_type);
    }
}

void WindowBlockInputStream::writeOutCurrentRow()
{
    assert(current_row < partition_end);
    assert(current_row.block >= first_block_number);

    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];
        ws.window_function->windowInsertResultInto(this->shared_from_this(), wi);
    }
}

Block WindowBlockInputStream::tryGetOutputBlock()
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

bool WindowBlockInputStream::onlyHaveRowNumber()
{
    for (const auto & workspace : workspaces)
    {
        if (workspace.window_function->getName() != "row_number")
            return false;
    }
    return true;
}

bool WindowBlockInputStream::onlyHaveRowNumberAndRank()
{
    for (const auto & workspace : workspaces)
    {
        if (workspace.window_function->getName() != "row_number" && workspace.window_function->getName() != "rank" && workspace.window_function->getName() != "dense_rank")
            return false;
    }
    return true;
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

void WindowBlockInputStream::appendBlock(Block & current_block)
{
    assert(!input_is_finished);
    assert(current_block);

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
        MutableColumnPtr res = ws.window_function->getReturnType()->createColumn();
        res->reserve(window_block.rows);
        window_block.output_columns.push_back(std::move(res));
    }

    window_block.input_columns = current_block.getColumns();
}

void WindowBlockInputStream::tryCalculate()
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
            writeOutCurrentRow();

            prev_frame_start = frame_start;

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
        // We have to reset the frame and other pointers when the new partition
        // starts.
        frame_start = partition_start;
        frame_end = partition_start;
        prev_frame_start = partition_start;
        assert(current_row == partition_start);
        current_row_number = 1;
        peer_group_last = partition_start;
        peer_group_start_row_number = 1;
        peer_group_number = 1;
    }
}
} // namespace DB
