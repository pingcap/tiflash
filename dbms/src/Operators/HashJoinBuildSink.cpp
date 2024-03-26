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

#include <Interpreters/Join.h>
#include <Operators/HashJoinBuildSink.h>

namespace DB
{
ReturnOpStatus HashJoinBuildSink::writeImpl(Block && block)
{
    if unlikely (!block)
    {
        is_finish_status = true;
        if (join_ptr->finishOneBuild(op_index))
        {
            if (join_ptr->hasBuildSideMarkedSpillData(op_index))
                return OperatorStatus::IO_OUT;
            join_ptr->finalizeBuild();
        }
        return OperatorStatus::FINISHED;
    }
    join_ptr->insertFromBlock(block, op_index);
    block.clear();
    return join_ptr->hasBuildSideMarkedSpillData(op_index) ? OperatorStatus::IO_OUT : OperatorStatus::NEED_INPUT;
}

ReturnOpStatus HashJoinBuildSink::prepareImpl()
{
    join_ptr->checkAndMarkPartitionSpilledIfNeeded(op_index);
    return join_ptr->hasBuildSideMarkedSpillData(op_index) ? OperatorStatus::IO_OUT : OperatorStatus::NEED_INPUT;
}

ReturnOpStatus HashJoinBuildSink::executeIOImpl()
{
    join_ptr->flushBuildSideMarkedSpillData(op_index);
    if (is_finish_status)
    {
        join_ptr->finalizeBuild();
        return OperatorStatus::FINISHED;
    }
    else
    {
        return OperatorStatus::NEED_INPUT;
    }
}
} // namespace DB
