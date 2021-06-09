
#include <DataStreams/HashJoinBuildBlockInputStream.h>
namespace DB
{

Block HashJoinBuildBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    join->insertFromBlockASync(block);
    return block;
}

} // namespace DB