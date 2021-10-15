
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

void namesToString(Names names, std::ostream & ostr)
{
    if (names.empty())
    {
        return;
    }

    auto iter = names.cbegin();
    ostr << *iter++;
    for (; iter != names.cend(); ++iter)
    {
        ostr << ", " << *iter;
    }
}

} // namespace DB
