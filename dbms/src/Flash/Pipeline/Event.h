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

struct PipelineEvent;
using PipelineEventPtr = std::shared_ptr<PipelineEvent>;

struct PipelineEvent
{
    static PipelineEventPtr submit(const PipelinePtr & pipeline)
    {
        return std::make_shared<PipelineEvent>(pipeline, "", PipelineEventType::submit);
    }

    static PipelineEventPtr finish(const PipelinePtr & pipeline)
    {
        return std::make_shared<PipelineEvent>(pipeline, "", PipelineEventType::finish);
    }

    static PipelineEventPtr fail(const PipelinePtr & pipeline, const String & err_msg)
    {
        return std::make_shared<PipelineEvent>(pipeline, err_msg, PipelineEventType::fail);
    }

    static PipelineEventPtr fail(const String & err_msg)
    {
        return fail(nullptr, err_msg);
    }

    static PipelineEventPtr cancel()
    {
        return std::make_shared<PipelineEvent>(nullptr, "", PipelineEventType::cancel);
    }

    PipelineEvent(
        const PipelinePtr & pipeline_,
        const String & err_msg_,
        PipelineEventType type_)
        : pipeline(pipeline_)
        , err_msg(err_msg_)
        , type(type_)
    {}

    const PipelinePtr pipeline;
    String err_msg;
    const PipelineEventType type;
};
} // namespace DB
