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

#include <DataStreams/SquashingTransform.h>
#include <common/logger_useful.h>


namespace DB
{
SquashingTransform::SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes, const String & req_id)
    : min_block_size_rows(min_block_size_rows)
    , min_block_size_bytes(min_block_size_bytes)
    , log(Logger::get(req_id))
{
    LOG_TRACE(
        log,
        "Squashing config - min_block_size_rows: {} min_block_size_bytes: {}",
        min_block_size_rows,
        min_block_size_bytes);
}


SquashingTransform::Result SquashingTransform::add(Block && block)
{
    if (!block)
        return Result(std::move(accumulated_block));

    auto block_rows = block.rows();
    auto block_bytes = block.bytes();

    /// Just read block is alredy enough.
    if (isEnoughSize(block_rows, block_bytes))
    {
        LOG_DEBUG(log, "Block size enough - rows: {} bytes: {}", block_rows, block_bytes);
        /// If no accumulated data, return just read block.
        if (!accumulated_block)
            return Result(std::move(block));

        /// Return accumulated data (may be it has small size) and place new block to accumulated data.
        accumulated_block.swap(block);
        return Result(std::move(block));
    }

    auto accumulated_block_rows = accumulated_block.rows();
    auto accumulated_block_bytes = accumulated_block.bytes();

    /// Accumulated block is already enough.
    if (accumulated_block && isEnoughSize(accumulated_block_rows, accumulated_block_bytes))
    {
        LOG_DEBUG(
            log,
            "Accumulated block size enough - rows: {} bytes: {}",
            accumulated_block_rows,
            accumulated_block_bytes);
        /// Return accumulated data and place new block to accumulated data.
        accumulated_block.swap(block);
        return Result(std::move(block));
    }

    append(std::move(block));

    accumulated_block_rows = accumulated_block.rows();
    accumulated_block_bytes = accumulated_block.bytes();

    if (isEnoughSize(accumulated_block_rows, accumulated_block_bytes))
    {
        LOG_DEBUG(
            log,
            "Combined block size enough - rows: {} bytes: {}",
            accumulated_block_rows,
            accumulated_block_bytes);
        Block res;
        res.swap(accumulated_block);
        return Result(std::move(res));
    }

    /// Squashed block is not ready.
    return Result(false);
}


void SquashingTransform::append(Block && block)
{
    if (!accumulated_block)
    {
        accumulated_block = std::move(block);
        return;
    }

    size_t columns = block.columns();
    size_t rows = block.rows();

    for (size_t i = 0; i < columns; ++i)
    {
        MutableColumnPtr mutable_column = (*std::move(accumulated_block.getByPosition(i).column)).mutate();
        mutable_column->insertRangeFrom(*block.getByPosition(i).column, 0, rows);
        accumulated_block.getByPosition(i).column = std::move(mutable_column);
    }
}


bool SquashingTransform::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes) || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

} // namespace DB
