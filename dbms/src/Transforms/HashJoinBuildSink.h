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

#pragma once

#include <Interpreters/Join.h>
#include <Transforms/Sink.h>

namespace DB
{
class HashJoinBuildSink : public Sink
{
public:
    HashJoinBuildSink(
        const JoinPtr & join_,
        size_t index_)
        : join(join_)
        , index(index_)
    {}

    bool write(Block & block) override
    {
        if (unlikely(!block))
            return false;

        join->insertFromBlock(block, index);
        return true;
    }

private:
    JoinPtr join;
    size_t index;
};
} // namespace DB
