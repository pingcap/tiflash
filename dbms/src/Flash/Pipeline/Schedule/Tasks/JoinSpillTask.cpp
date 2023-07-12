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

#include <Flash/Pipeline/Schedule/Tasks/JoinSpillTask.h>

namespace DB
{
JoinSpillTask::JoinSpillTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const EventPtr & event_,
    const PipelineJoinSpillContextPtr & spill_context_,
    bool is_build_side_,
    size_t partition_index_,
    Blocks && blocks_)
    : OutputIOEventTask(exec_context_, req_id, event_)
    , spill_context(spill_context_)
    , is_build_side(is_build_side_)
    , partition_index(partition_index_)
    , blocks(std::move(blocks_))
{
    assert(spill_context);
}

void JoinSpillTask::doFinalizeImpl()
{
    blocks.clear();
    spill_context.reset();
}

ExecTaskStatus JoinSpillTask::executeIOImpl()
{
    auto & spiller = is_build_side ? *spill_context->build_spiller : *spill_context->probe_spiller;
    spiller.spillBlocks(std::move(blocks), partition_index);
    return ExecTaskStatus::FINISHED;
}
} // namespace DB
