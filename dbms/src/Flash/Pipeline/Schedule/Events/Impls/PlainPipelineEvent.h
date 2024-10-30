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

#pragma once

#include <Flash/Pipeline/Schedule/Events/Event.h>

namespace DB
{
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;

class Context;

class PlainPipelineEvent : public Event
{
public:
    PlainPipelineEvent(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        Context & context_,
        const PipelinePtr & pipeline_,
        size_t concurrency_,
        UInt64 minTSO_wait_time_in_ms_)
        : Event(exec_context_, req_id)
        , context(context_)
        , pipeline(pipeline_)
        , concurrency(concurrency_)
        , minTSO_wait_time_in_ms(minTSO_wait_time_in_ms_)
    {}

protected:
    void scheduleImpl() override;

    void finishImpl() override;

private:
    Context & context;
    PipelinePtr pipeline;
    size_t concurrency;
    UInt64 minTSO_wait_time_in_ms;
};
} // namespace DB
