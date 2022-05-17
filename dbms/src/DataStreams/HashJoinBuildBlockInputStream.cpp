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
    Block block = children.back()->read();
    if (!block)
        return block;
    join->insertFromBlock(block, concurrency_build_index);
    return block;
}

void HashJoinBuildBlockInputStream::print(FmtBuffer & buffer, size_t indent, size_t multiplier) const
{
    IProfilingBlockInputStream::print(buffer, indent, multiplier);
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
    buffer.fmtAppend(", build_concurrency{{{}}}, join_kind{{{}}}", join->getBuildConcurrency(), join_type_it->second);
}
} // namespace DB
