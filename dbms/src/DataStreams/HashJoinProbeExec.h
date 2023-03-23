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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Join.h>

#include <memory>

#pragma once

namespace DB
{
class HashJoinProbeExec;
using HashJoinProbeExecPtr = std::shared_ptr<HashJoinProbeExec>;

class HashJoinProbeExec
{
public:
    JoinPtr join;

    BlockInputStreamPtr restore_build_stream;

    BlockInputStreamPtr probe_stream;

    BlockInputStreamPtr non_joined_stream;
};
} // namespace DB
