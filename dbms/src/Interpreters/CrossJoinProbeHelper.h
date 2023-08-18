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

#pragma once

#include <Core/Block.h>
#include <Interpreters/JoinUtils.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
struct ProbeProcessInfo;
Block crossProbeBlockDeepCopyRightBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks);
/// return <probed_block, is_matched_rows>
std::pair<Block, bool> crossProbeBlockShallowCopyRightBlock(
    ASTTableJoin::Kind kind,
    ASTTableJoin::Strictness strictness,
    ProbeProcessInfo & probe_process_info,
    const Blocks & right_blocks);
} // namespace DB
