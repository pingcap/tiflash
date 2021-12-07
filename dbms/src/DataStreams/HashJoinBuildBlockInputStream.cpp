
#include <DataStreams/HashJoinBuildBlockInputStream.h>

namespace DB
{
Block HashJoinBuildBlockInputStream::readImpl()
{
    auto timer = newTimer(Timeline::PULL);
    Block block = children.back()->read();
    timer.switchTo(Timeline::SELF);

    if (!block)
        return block;

    join->insertFromBlock(block, stream_index);
    return block;
}
} // namespace DB
