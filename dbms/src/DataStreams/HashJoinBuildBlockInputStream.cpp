
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <chrono>
namespace DB
{

Block HashJoinBuildBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (!block)
        return block;
    join->insertFromBlock(block, stream_index);
    return block;
}

void HashJoinBuildBlockInputStream::readSuffixImpl()
{
    LOG_TRACE(mpp_task_log, "HashJoinBuildBlockInputStream- total time:"
        << std::to_string(info.execution_time / 1000000UL) + "ms"
        << " total rows: " << info.rows
        << " total blocks: " << info.blocks
        << " total bytes:" << info.bytes);
}

} // namespace DB
