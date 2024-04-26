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

#include <Flash/Pipeline/Schedule/Tasks/AggregateFinalConvertTask.h>
#include <Operators/AggregateContext.h>

namespace DB
{
AggregateFinalConvertTask::AggregateFinalConvertTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const EventPtr & event_,
    AggregateContextPtr agg_context_,
    size_t index_)
    : EventTask(exec_context_, req_id, event_)
    , agg_context(std::move(agg_context_))
    , index(index_)
{
    assert(agg_context);
}

ExecTaskStatus AggregateFinalConvertTask::executeImpl()
{
    agg_context->convertToTwoLevel(index);
    agg_context.reset();
    return ExecTaskStatus::FINISHED;
}

} // namespace DB
