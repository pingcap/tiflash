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

#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Interpreters/JoinSpillContext.h>

namespace DB
{
class JoinSpillEvent : public Event
{
public:
    JoinSpillEvent(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const PipelineJoinSpillContextPtr & spill_context_,
        size_t stream_index_,
        bool is_build_side_,
        PartitionBlockVecs && partition_block_vecs_,
        bool is_last_spill_)
        : Event(exec_context_, req_id)
        , spill_context(spill_context_)
        , stream_index(stream_index_)
        , is_build_side(is_build_side_)
        , partition_block_vecs(std::move(partition_block_vecs_))
        , is_last_spill(is_last_spill_)
    {}

protected:
    void scheduleImpl() override;

    void finishImpl() override;

private:
    PipelineJoinSpillContextPtr spill_context;
    size_t stream_index;
    bool is_build_side;
    PartitionBlockVecs partition_block_vecs;
    bool is_last_spill;
};
} // namespace DB
