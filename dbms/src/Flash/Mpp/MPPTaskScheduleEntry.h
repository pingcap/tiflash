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

#include <Common/Logger.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
class MPPTaskManager;

enum class ScheduleState
{
    WAITING,
    SCHEDULED,
    FAILED,
    EXCEEDED,
    COMPLETED
};

class MPPTaskScheduleEntry
{
public:
    int getNeededThreads() const;
    void setNeededThreads(int needed_threads_);

    bool schedule(ScheduleState state);
    void waitForSchedule();

    const MPPTaskId & getMPPTaskId() const;

    const String & getResourceGroupName() const { return id.gather_id.query_id.resource_group_name; }

    ~MPPTaskScheduleEntry();

    MPPTaskScheduleEntry(MPPTaskManager * manager_, const MPPTaskId & id_);

private:
    MPPTaskManager * manager;
    MPPTaskId id;

    int needed_threads;

    std::mutex schedule_mu;
    std::condition_variable schedule_cv;
    ScheduleState schedule_state;
    const LoggerPtr log;
};

} // namespace DB
