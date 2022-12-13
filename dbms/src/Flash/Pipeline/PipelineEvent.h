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

#include <Flash/Pipeline/Event.h>

namespace DB
{
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;

class PipelineEvent : public Event
{
public:
    PipelineEvent(
        PipelineExecStatus & exec_status_,
        Context & context_,
        size_t concurrency_,
        const PipelinePtr & pipeline_)
        : Event(exec_status_)
        , context(context_)
        , concurrency(concurrency_)
        , pipeline(pipeline_)
    {}

protected:
    bool scheduleImpl() override;

private:
    Context & context;
    size_t concurrency;

    PipelinePtr pipeline;
};
} // namespace DB
