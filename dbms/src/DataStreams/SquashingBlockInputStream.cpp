#include <DataStreams/SquashingBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>


namespace DB
{
SquashingBlockInputStream::SquashingBlockInputStream(
    const BlockInputStreamPtr & src,
    size_t min_block_size_rows,
    size_t min_block_size_bytes,
    const LogWithPrefixPtr & log_)
    : transform(min_block_size_rows, min_block_size_bytes)
    , log(getMPPTaskLog(log_, getName()))
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

void SquashingBlockInputStream::dumpExtra(std::ostream & ostr) const
{
    ostr << "min_block_size_rows: [" << transform.getMinBlockSizeRows() << "] min_block_size_bytes: [" << transform.getMinBlockSizeBytes() << ']';
}

} // namespace DB
