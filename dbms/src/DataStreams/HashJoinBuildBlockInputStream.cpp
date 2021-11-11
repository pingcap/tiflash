
#include <DataStreams/HashJoinBuildBlockInputStream.h>
namespace DB
{

Block HashJoinBuildBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    auto timer = getSelfTimer();

    if (!block)
        return block;
    std::this_thread::sleep_for(std::chrono::seconds(3));
    join->insertFromBlock(block, stream_index);
    return block;
}

} // namespace DB
