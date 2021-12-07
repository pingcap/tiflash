#include <DataStreams/SquashingBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>


namespace DB
{
SquashingBlockInputStream::SquashingBlockInputStream(
    const BlockInputStreamPtr & src,
    size_t min_block_size_rows,
    size_t min_block_size_bytes,
    const LogWithPrefixPtr & log_)
    : log(getMPPTaskLog(log_, getName()))
    , transform(min_block_size_rows, min_block_size_bytes, log)
{
    children.emplace_back(src);
}


Block SquashingBlockInputStream::readImpl()
{
    auto timer = newTimer(Timeline::SELF);

    if (all_read)
        return {};

    while (true)
    {
        timer.switchTo(Timeline::PULL);
        Block block = children[0]->read();
        timer.switchTo(Timeline::SELF);

        if (!block)
            all_read = true;

        SquashingTransform::Result result = transform.add(std::move(block));
        if (result.ready)
            return result.block;
    }
}

} // namespace DB
