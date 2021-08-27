#include <DataStreams/SquashingBlockInputStream.h>


namespace DB
{
SquashingBlockInputStream::SquashingBlockInputStream(
    const BlockInputStreamPtr & src,
    size_t min_block_size_rows,
    size_t min_block_size_bytes,
    const std::shared_ptr<LogWithPrefix> & log_)
    : transform(min_block_size_rows, min_block_size_bytes)
    , log(getLogWithPrefix(log_, getName()))
{
    children.emplace_back(src);
}


Block SquashingBlockInputStream::readImpl()
{
    if (all_read)
        return {};

    while (true)
    {
        Block block = children[0]->read();
        if (!block)
            all_read = true;

        SquashingTransform::Result result = transform.add(std::move(block));
        if (result.ready)
            return result.block;
    }
}

} // namespace DB
