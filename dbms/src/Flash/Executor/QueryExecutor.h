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
class QueryExecutor
{
public:
    QueryExecutor() = default;

    virtual ~QueryExecutor() = default;

    std::pair<bool, String> execute();
    std::pair<bool, String> execute(ResultHandler::Handler handler);

    virtual void cancel(bool is_kill) = 0;

    virtual String dump() const = 0;

protected:
    // is_success, err_msg
    virtual std::pair<bool, String> execute(ResultHandler) = 0;
};

using QueryExecutorPtr = std::shared_ptr<QueryExecutor>;
} // namespace DB
