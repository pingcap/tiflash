// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#include <Common/FmtUtils.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
namespace DB
{
Block HashJoinBuildBlockInputStream::readImpl()
{
    try
    {
        Block block = children.back()->read();
        if (!block)
        {
            join->finishOneBuild();
            return block;
        }
        join->insertFromBlock(block, concurrency_build_index);
        return block;
    }
    catch (...)
    {
        join->meetError();
        throw;
    }
}

void HashJoinBuildBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    static const std::unordered_map<ASTTableJoin::Kind, String> join_type_map{
        {ASTTableJoin::Kind::Inner, "Inner"},
        {ASTTableJoin::Kind::Left, "Left"},
        {ASTTableJoin::Kind::Right, "Right"},
        {ASTTableJoin::Kind::Full, "Full"},
        {ASTTableJoin::Kind::Cross, "Cross"},
        {ASTTableJoin::Kind::Comma, "Comma"},
        {ASTTableJoin::Kind::Anti, "Anti"},
        {ASTTableJoin::Kind::LeftSemi, "Left_Semi"},
        {ASTTableJoin::Kind::LeftAnti, "Left_Anti"},
        {ASTTableJoin::Kind::Cross_Left, "Cross_Left"},
        {ASTTableJoin::Kind::Cross_Right, "Cross_Right"},
        {ASTTableJoin::Kind::Cross_Anti, "Cross_Anti"},
        {ASTTableJoin::Kind::Cross_LeftSemi, "Cross_LeftSemi"},
        {ASTTableJoin::Kind::Cross_LeftAnti, "Cross_LeftAnti"}};
    auto join_type_it = join_type_map.find(join->getKind());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type", Errors::Coprocessor::Internal);
    buffer.fmtAppend(", join_kind = {}", join_type_it->second);
}
} // namespace DB
