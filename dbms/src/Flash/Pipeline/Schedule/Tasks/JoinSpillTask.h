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

#include <Flash/Pipeline/Schedule/Tasks/IOEventTask.h>
#include <Interpreters/JoinSpillContext.h>

namespace DB
{
class JoinSpillTask : public OutputIOEventTask
{
public:
    JoinSpillTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const EventPtr & event_,
        const PipelineJoinSpillContextPtr & spill_context_,
        bool is_build_side_,
        size_t partition_index_,
        Blocks && blocks_);

protected:
    ExecTaskStatus executeIOImpl() override;

    void doFinalizeImpl() override;

private:
    PipelineJoinSpillContextPtr spill_context;
    bool is_build_side;
    size_t partition_index;
    Blocks blocks;
};
} // namespace DB
