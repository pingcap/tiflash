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

#include <Flash/Pipeline/Pipeline.h>

#include <memory>

namespace DB
{
enum PipelineEventType
{
    submit,
    finish,
    fail,
    cancel,
};

struct PipelineEvent
{
    static PipelineEvent submit(const PipelinePtr & pipeline)
    {
        return {pipeline, "", false, PipelineEventType::submit};
    }

    static PipelineEvent finish(const PipelinePtr & pipeline)
    {
        return {pipeline, "", false, PipelineEventType::finish};
    }

    static PipelineEvent fail(const PipelinePtr & pipeline, const String & err_msg)
    {
        return {pipeline, err_msg, false, PipelineEventType::fail};
    }

    static PipelineEvent fail(const String & err_msg)
    {
        return fail(nullptr, err_msg);
    }

    static PipelineEvent cancel(bool is_kill)
    {
        return {nullptr, "", is_kill, PipelineEventType::cancel};
    }

    PipelineEvent() = default;

    PipelineEvent(
        const PipelinePtr & pipeline_,
        const String & err_msg_,
        bool is_kill_,
        PipelineEventType type_)
        : pipeline(pipeline_)
        , err_msg(err_msg_)
        , is_kill(is_kill_)
        , type(type_)
    {}

    PipelineEvent(PipelineEvent && event)
        : pipeline(std::move(event.pipeline))
        , err_msg(std::move(event.err_msg))
        , is_kill(event.is_kill)
        , type(std::move(event.type))
    {}

    PipelineEvent & operator=(PipelineEvent && event)
    {
        if (this != &event)
        {
            pipeline = std::move(event.pipeline);
            err_msg = std::move(event.err_msg);
            is_kill = event.is_kill;
            type = std::move(event.type);
        }
        return *this;
    }

    PipelinePtr pipeline;
    String err_msg;
    bool is_kill;
    PipelineEventType type;
};
} // namespace DB
