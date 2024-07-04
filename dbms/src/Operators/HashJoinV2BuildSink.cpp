// Copyright 2024 PingCAP, Inc.
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

#include <Operators/HashJoinV2BuildSink.h>

namespace DB
{
OperatorStatus HashJoinV2BuildSink::writeImpl(Block && block)
{
    if unlikely (!block)
    {
        is_finish_status = true;
        join_ptr->finishOneBuild(op_index);
        return OperatorStatus::FINISHED;
    }
    join_ptr->insertFromBlock(block, op_index);
    block.clear();
    return OperatorStatus::NEED_INPUT;
}

} // namespace DB
