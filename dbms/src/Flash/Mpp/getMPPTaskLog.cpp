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

#include <Flash/Mpp/getMPPTaskLog.h>

namespace DB
{
LogWithPrefixPtr getMPPTaskLog(const String & name, const MPPTaskId & mpp_task_id_)
{
    return std::make_shared<LogWithPrefix>(&Poco::Logger::get(name), mpp_task_id_.toString());
}

LogWithPrefixPtr getMPPTaskLog(const DAGContext & dag_context, const String & name)
{
    return getMPPTaskLog(dag_context.log, name, dag_context.getMPPTaskId());
}

LogWithPrefixPtr getMPPTaskLog(const LogWithPrefixPtr & log, const String & name, const MPPTaskId & mpp_task_id_)
{
    if (log == nullptr)
    {
        return getMPPTaskLog(name, mpp_task_id_);
    }

    return log->append(name);
}
} // namespace DB
