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

#include <Flash/Executor/QueryExecutor.h>

#include <mutex>

namespace DB
{
class QueryExecutorHolder
{
public:
    void set(QueryExecutorPtr && query_executor_)
    {
        std::lock_guard lock(mu);
        assert(!query_executor);
        query_executor = std::move(query_executor_);
    }

    std::optional<QueryExecutor *> tryGet()
    {
        std::optional<QueryExecutor *> res;
        std::lock_guard lock(mu);
        if (query_executor != nullptr)
            res.emplace(query_executor.get());
        return res;
    }

    QueryExecutor * operator->()
    {
        std::lock_guard lock(mu);
        assert(query_executor != nullptr);
        return query_executor.get();
    }

private:
    std::mutex mu;
    QueryExecutorPtr query_executor;
};
} // namespace DB
