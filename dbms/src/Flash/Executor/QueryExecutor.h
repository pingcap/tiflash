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

#include <Common/MemoryTracker.h>
#include <Common/PtrHolder.h>
#include <Flash/Executor/ExecutionResult.h>
#include <Flash/Executor/ResultHandler.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Statistics/BaseRuntimeStatistics.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class Context;
class DAGContext;

class QueryExecutor
{
public:
    QueryExecutor(
        const MemoryTrackerPtr & memory_tracker_,
        Context & context_,
        const String & req_id)
        : memory_tracker(memory_tracker_)
        , context(context_)
        , log(Logger::get(req_id))
    {}

    virtual ~QueryExecutor() = default;

    ExecutionResult execute();
    ExecutionResult execute(ResultHandler::Handler handler);

    virtual void cancel() = 0;

    virtual String toString() const = 0;

    virtual int estimateNewThreadCount() = 0;

    virtual RU collectRequestUnit() = 0;

    virtual Block getSampleBlock() const = 0;

    virtual BaseRuntimeStatistics getRuntimeStatistics() const = 0;

protected:
    virtual ExecutionResult execute(ResultHandler &&) = 0;

    DAGContext & dagContext() const;

protected:
    MemoryTrackerPtr memory_tracker;
    Context & context;
    LoggerPtr log;
};

using QueryExecutorPtr = std::unique_ptr<QueryExecutor>;
using QueryExecutorHolder = PtrHolder<QueryExecutorPtr>;
} // namespace DB
