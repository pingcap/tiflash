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

#include <unordered_set>
#include <unordered_map>

namespace DB
{
struct PipelineStatusMachine
{
public:
    void addPipeline(const PipelinePtr & pipeline);
    PipelinePtr getPipeline(UInt32 id) const;

    bool isWaiting(UInt32 id) const;
    bool isRunning(UInt32 id) const;
    bool isCompleted(UInt32 id) const;

    void stateToWaiting(UInt32 id);
    void stateToRunning(UInt32 id);
    void stateToComplete(UInt32 id);

    std::unordered_set<PipelinePtr> nextPipelines(UInt32 id) const;

private:
    std::unordered_map<UInt32, PipelinePtr> id_to_pipeline;
    std::unordered_set<UInt32> waiting_ids;
    std::unordered_set<UInt32> running_ids;
    std::unordered_set<UInt32> complete_ids;
};
}
