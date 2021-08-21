#include <DataStreams/SquashingBlockInputStream.h>


namespace DB
{

SquashingBlockInputStream::SquashingBlockInputStream(const BlockInputStreamPtr & src,
                                                     size_t min_block_size_rows,
                                                     size_t min_block_size_bytes,
                                                     Logger * mpp_task_log_)
    : transform(min_block_size_rows, min_block_size_bytes), mpp_task_log(mpp_task_log_)
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

void SquashingBlockInputStream::readSuffixImpl()
{
    if (mpp_task_log != nullptr)
    {
        LOG_TRACE(mpp_task_log, "SquashingBlockInputStream total time:"
            << std::to_string(info.execution_time / 1000000UL) + "ms"
            << " total rows: " << info.rows
            << " total blocks: " << info.blocks
            << " total bytes:" << info.bytes);
    }
}

}
