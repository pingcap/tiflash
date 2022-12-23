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

#include <Flash/Mpp/getMPPTaskTracingLog.h>
#include <common/logger_useful.h>

namespace DB
{
LoggerPtr getMPPTaskTracingLog(const MPPTaskId & mpp_task_id)
{
    return Logger::get(DB::tracing_log_source, mpp_task_id.toString());
}
} // namespace DB
