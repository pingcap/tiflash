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

#pragma once

#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Interpreters/JoinV2/HashJoin.h>

namespace DB
{

class HashJoinV2BuildPointerTableEvent : public Event
{
public:
    HashJoinV2BuildPointerTableEvent(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const HashJoinPtr & join_ptr_)
        : Event(exec_context_, req_id)
        , join_ptr(join_ptr_)
    {
        assert(join_ptr);
    }

protected:
    void scheduleImpl() override;

private:
    HashJoinPtr join_ptr;
};
} // namespace DB
