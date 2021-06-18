
#include <DataStreams/HashJoinBuildBlockInputStream.h>
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

} // namespace DB
