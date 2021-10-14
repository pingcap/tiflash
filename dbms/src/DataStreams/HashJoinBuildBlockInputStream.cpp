
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

void HashJoinBuildBlockInputStream::dumpExtra(std::ostream & ostr) const
{
    static const std::unordered_map<ASTTableJoin::Kind, String> join_type_map{
        {ASTTableJoin::Kind::Inner, "Inner"},
        {ASTTableJoin::Kind::Left, "Left"},
        {ASTTableJoin::Kind::Right, "Right"},
        {ASTTableJoin::Kind::Full, "Full"},
        {ASTTableJoin::Kind::Cross, "Cross"},
        {ASTTableJoin::Kind::Comma, "Comma"},
        {ASTTableJoin::Kind::Anti, "Anti"},
        {ASTTableJoin::Kind::Cross_Left, "Cross_Left"},
        {ASTTableJoin::Kind::Cross_Right, "Cross_Right"},
        {ASTTableJoin::Kind::Cross_Anti, "Cross_Anti"}};
    auto join_type_it = join_type_map.find(join->getKind());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type", Errors::Coprocessor::Internal);
    ostr << "build_concurrency: [" << join->getBuildConcurrency() << "] join_kind: [" << join_type_it->second;
    ostr << "] key_names_left: [";
    namesToString(join->getLeftJoinKeys(), ostr);
    ostr << "]";
}

} // namespace DB
