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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Executor/ResultQueue.h>

namespace DB
{
class Context;

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;

class AutoSpillTrigger;

/**
 * PipelineExecutor is the implementation of the pipeline-based execution model.
 * 
 *                                            ┌────────────────────────────┐
 *                                            │      task scheduler        │
 *           generate          submit tasks   │                            │
 *  pipeline ────────►  event1 ─────────────► │    ┌───────────────────┐   │
 *                                            │ ┌──┤io task thread pool◄─┐ │
 *                        │ trigger           │ │  └──────▲──┬─────────┘ │ │
 *                        ▼                   │ │         │  │           │ │
 *                             submit tasks   │ │ ┌───────┴──▼─────────┐ │ │
 *                      event2 ─────────────► │ │ │cpu task thread pool│ │ │
 *                                            │ │ └───────▲──┬─────────┘ │ │
 *                        │ trigger           │ │         │  │           │ │
 *                        ▼                   │ │    ┌────┴──▼────┐      │ │
 *                             submit tasks   │ └────►wait reactor├──────┘ │
 *                      event3 ─────────────► │      └────────────┘        │
 *                                            │                            │
 *                                            └────────────────────────────┘
 * 
 * As shown above, the pipeline generates a number of events, which are executed in dependency order, 
 * and the events generate a number of tasks that will be submitted to the TaskScheduler for execution.
 */
class PipelineExecutor : public QueryExecutor
{
public:
    PipelineExecutor(
        const MemoryTrackerPtr & memory_tracker_,
        AutoSpillTrigger * auto_spill_trigger,
        const RegisterOperatorSpillContext & register_operator_spill_context,
        Context & context_,
        const String & req_id);

    String toString() const override;

    void cancel() override;

    int estimateNewThreadCount() override;

    RU collectRequestUnit() override;

    Block getSampleBlock() const override;

    BaseRuntimeStatistics getRuntimeStatistics() const override;

    String getExtraJsonInfo() const override;

protected:
    ExecutionResult execute(ResultHandler && result_handler) override;

private:
    void scheduleEvents();

    void wait();

    void consume(ResultHandler & result_handler);

private:
    PipelinePtr root_pipeline;

    PipelineExecutorContext exec_context;
};
} // namespace DB
