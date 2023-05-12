// Copyright 2023 PingCAP, Ltd.
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

#pragma once

#include <Core/Block.h>
#include <Interpreters/JoinUtils.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
struct ProbeProcessInfo;
/// crossProbeBlock construct the probe block using CrossJoinAdder, the right rows will be added to probe block row by row, used when right side has few rows
Block crossProbeBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks);
/// crossProbeBlockNoCopyRightBlock construct the probe block without copy right block, the left row is appended to the right block, used when right side has many rows
/// return <probed_block, is_matched_rows>
std::pair<Block, bool> crossProbeBlockNoCopyRightBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks);
} // namespace DB
