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

#include <Flash/Pipeline/Schedule/Tasks/Impls/IOEventTask.h>

namespace DB
{
class SpilledBucketInput;

class LoadBucketTask : public InputIOEventTask
{
public:
    LoadBucketTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const EventPtr & event_,
        SpilledBucketInput & input_)
        : IOEventTask(exec_context_, req_id, event_)
        , input(input_)
    {}

private:
    ExecTaskStatus executeIOImpl() override;

private:
    SpilledBucketInput & input;
};
} // namespace DB
