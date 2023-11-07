// Copyright 2023 PingCAP, Inc.
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
            if (join->finishOneBuild(stream_index))
            {
                if (join->hasBuildSideMarkedSpillData(stream_index))
                    join->flushBuildSideMarkedSpillData(stream_index);
                join->finalizeBuild();
            }
            return block;
        }
        join->insertFromBlock(block, stream_index);
        if (join->hasBuildSideMarkedSpillData(stream_index))
            join->flushBuildSideMarkedSpillData(stream_index);
        return block;
    }
    catch (...)
    {
        auto error_message = getCurrentExceptionMessage(false, true);
        join->meetError(error_message);
        throw Exception(error_message);
    }
}

void HashJoinBuildBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    static const std::unordered_map<ASTTableJoin::Kind, String> join_type_map{
        {ASTTableJoin::Kind::Inner, "Inner"},
        {ASTTableJoin::Kind::LeftOuter, "Left"},
        {ASTTableJoin::Kind::RightOuter, "Right"},
        {ASTTableJoin::Kind::Full, "Full"},
        {ASTTableJoin::Kind::Cross, "Cross"},
        {ASTTableJoin::Kind::Comma, "Comma"},
        {ASTTableJoin::Kind::Semi, "Semi"},
        {ASTTableJoin::Kind::Anti, "Anti"},
        {ASTTableJoin::Kind::LeftOuterSemi, "Left_Semi"},
        {ASTTableJoin::Kind::LeftOuterAnti, "Left_Anti"},
        {ASTTableJoin::Kind::Cross_LeftOuter, "Cross_Left"},
        {ASTTableJoin::Kind::Cross_RightOuter, "Cross_Right"},
        {ASTTableJoin::Kind::Cross_Semi, "Cross_Semi"},
        {ASTTableJoin::Kind::Cross_Anti, "Cross_Anti"},
        {ASTTableJoin::Kind::Cross_LeftOuterSemi, "Cross_LeftSemi"},
        {ASTTableJoin::Kind::Cross_LeftOuterAnti, "Cross_LeftAnti"},
        {ASTTableJoin::Kind::NullAware_Anti, "NullAware_Anti"},
        {ASTTableJoin::Kind::NullAware_LeftOuterSemi, "NullAware_LeftSemi"},
        {ASTTableJoin::Kind::NullAware_LeftOuterAnti, "NullAware_LeftAnti"},
        {ASTTableJoin::Kind::RightSemi, "RightSemi"},
        {ASTTableJoin::Kind::RightAnti, "RightAnti"},
    };
    auto join_type_it = join_type_map.find(join->getKind());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type", Errors::Coprocessor::Internal);
    buffer.fmtAppend(", join_kind = {}", join_type_it->second);
}
} // namespace DB
