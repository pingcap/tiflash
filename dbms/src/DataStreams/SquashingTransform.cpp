#include <DataStreams/SquashingTransform.h>
#include <common/logger_useful.h>


namespace DB
{

SquashingTransform::SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes)
    : min_block_size_rows(min_block_size_rows), min_block_size_bytes(min_block_size_bytes)
    , log(&Logger::get("SquashingTransform"))
{
    LOG_DEBUG(log, "Squashing config - min_block_size_rows: " << min_block_size_rows << " min_block_size_bytes: " << min_block_size_bytes);
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
        LOG_DEBUG(log, "Block size enough - rows: " << block_rows << " bytes: " << block_bytes);
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
        LOG_DEBUG(log, "Accumulated block size enough - rows: " << accumulated_block_rows << " bytes: " << accumulated_block_bytes);
        /// Return accumulated data and place new block to accumulated data.
        accumulated_block.swap(block);
        return Result(std::move(block));
    }

    append(std::move(block));

    accumulated_block_rows = accumulated_block.rows();
    accumulated_block_bytes = accumulated_block.bytes();

    if (isEnoughSize(accumulated_block_rows, accumulated_block_bytes))
    {
        LOG_DEBUG(log, "Combined block size enough - rows: " << accumulated_block_rows << " bytes: " << accumulated_block_bytes);
        Block res;
        res.swap(accumulated_block);
        return Result(std::move(res));
    }

    /// Squashed block is not ready.
    return false;
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
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

}
