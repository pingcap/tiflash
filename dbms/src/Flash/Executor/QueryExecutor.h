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

#include <Flash/Executor/ResultHandler.h>
#include <common/types.h>

#include <memory>
#include <utility>

namespace DB
{
struct ExecutionResult
{
    bool is_success;
    String err_msg;

    void verify()
    {
        RUNTIME_CHECK(is_success, err_msg);
    }

    static ExecutionResult success()
    {
        return {true, ""};
    }

    static ExecutionResult fail(const String & err_msg)
    {
        RUNTIME_CHECK(!err_msg.empty());
        return {false, err_msg};
    }
};

class ProcessListEntry;
using ProcessListEntryPtr = std::shared_ptr<ProcessListEntry>;

class QueryExecutor
{
public:
    explicit QueryExecutor(const ProcessListEntryPtr & process_list_entry_)
        : process_list_entry(process_list_entry_)
    {}

    virtual ~QueryExecutor() = default;

    ExecutionResult execute();
    ExecutionResult execute(ResultHandler::Handler handler);

    virtual void cancel(bool is_kill) = 0;

    virtual String dump() const = 0;

    virtual int estimateNewThreadCount() = 0;

protected:
    virtual ExecutionResult execute(ResultHandler) = 0;

protected:
    ProcessListEntryPtr process_list_entry;
};

using QueryExecutorPtr = std::unique_ptr<QueryExecutor>;
} // namespace DB
