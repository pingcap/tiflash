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

#include <Flash/Pipeline/Schedule/Events/JoinSpillEvent.h>

namespace DB
{
void JoinSpillEvent::scheduleImpl()
{
    assert(spill_context);
    for (auto & partition_block_vec : partition_block_vecs)
    {
        if (is_build_side)
            spill_context->build_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
        else
            spill_context->probe_spiller->spillBlocks(std::move(partition_block_vec.blocks), partition_block_vec.partition_index);
    }
}

void JoinSpillEvent::finishImpl()
{
    if (is_last_spill)
    {
        if (is_build_side)
            spill_context->build_spiller->finishSpill();
        else
            spill_context->probe_spiller->finishSpill();
    }
    std::lock_guard lock(spill_context->mu);
    auto & spilling_task_cnt = is_build_side ? spill_context->build_spilling_tasks[stream_index] : spill_context->probe_spilling_tasks[stream_index];
    RUNTIME_CHECK(spilling_task_cnt > 0);
    --spilling_task_cnt;
}
} // namespace DB
