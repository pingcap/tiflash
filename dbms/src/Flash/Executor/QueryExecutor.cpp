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

#include <Common/FailPoint.h>
#include <Flash/Executor/QueryExecutor.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace FailPoints
{
extern const char hang_in_execution[];
} // namespace FailPoints

ExecutionResult QueryExecutor::execute()
{
    FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
    return execute(ResultHandler{});
}

ExecutionResult QueryExecutor::execute(ResultHandler::Handler handler)
{
    FAIL_POINT_PAUSE(FailPoints::hang_in_execution);
    return execute(ResultHandler{handler});
}

DAGContext & QueryExecutor::dagContext() const
{
    return *context.getDAGContext();
}
} // namespace DB
