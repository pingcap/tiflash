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
#include <Operators/OperatorProfileInfo.h>

namespace DB
{
class AggregateContext;
using AggregateContextPtr = std::shared_ptr<AggregateContext>;

class AggregateFinalSpillEvent : public Event
{
public:
    AggregateFinalSpillEvent(
        PipelineExecutorStatus & exec_status_,
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        AggregateContextPtr agg_context_,
        std::vector<size_t> indexes_,
        OperatorProfileInfos profile_infos_)
        : Event(exec_status_, std::move(mem_tracker_), req_id)
        , agg_context(std::move(agg_context_))
        , indexes(std::move(indexes_))
        , profile_infos(std::move(profile_infos_))
    {
        assert(agg_context);
        assert(!indexes.empty());
        assert(!profile_infos.empty());
    }

protected:
    void scheduleImpl() override;

    void finishImpl() override;

private:
    AggregateContextPtr agg_context;
    std::vector<size_t> indexes;

    OperatorProfileInfos profile_infos;
};
} // namespace DB
