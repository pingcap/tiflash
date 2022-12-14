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

#include <Flash/Mpp/MPPTask.h>
#include <common/logger_useful.h>

namespace DB
{
class MinTSOScheduler;
using MPPTaskSchedulerPtr = std::unique_ptr<MinTSOScheduler>;

class MPPTaskManager;
using MPPTaskManagerPtr = std::shared_ptr<MPPTaskManager>;

struct MPPQueryTaskSet;
using MPPQueryTaskSetPtr = std::shared_ptr<MPPQueryTaskSet>;

/// scheduling tasks in the set according to the tso order under the soft limit of threads, but allow the min_query_id query to preempt threads under the hard limit of threads.
/// The min_query_id query avoids the deadlock resulted from threads competition among nodes.
/// schedule tasks under the lock protection of the task manager.
/// NOTE: if the updated min-tso query has waiting tasks, necessarily scheduling them, otherwise the query would hang.
class MinTSOScheduler : private boost::noncopyable
{
public:
    MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit, UInt64 active_set_soft_limit_);
    ~MinTSOScheduler() = default;
    /// try to schedule this task if it is the min_query_id query or there are enough threads, otherwise put it into the waiting set.
    /// NOTE: call tryToSchedule under the lock protection of MPPTaskManager
    bool tryToSchedule(MPPTaskScheduleEntry & schedule_entry, MPPTaskManager & task_manager);

    /// delete this to-be cancelled/finished query from scheduler and update min_query_id if needed, so that there aren't cancelled/finished queries in the scheduler.
    /// NOTE: call deleteQuery under the lock protection of MPPTaskManager
    void deleteQuery(const MPPQueryId & query_id, MPPTaskManager & task_manager, const bool is_cancelled);

    /// all scheduled tasks should finally call this function to release threads and schedule new tasks
    void releaseThreadsThenSchedule(const int needed_threads, MPPTaskManager & task_manager);

private:
    bool scheduleImp(const MPPQueryId & query_id, const MPPQueryTaskSetPtr & query_task_set, MPPTaskScheduleEntry & schedule_entry, const bool isWaiting, bool & has_error);
    bool updateMinQueryId(const MPPQueryId & query_id, const bool retired, const String & msg);
    void scheduleWaitingQueries(MPPTaskManager & task_manager);
    bool isDisabled()
    {
        return thread_hard_limit == 0 && thread_soft_limit == 0;
    }
    std::set<MPPQueryId> waiting_set;
    std::set<MPPQueryId> active_set;
    MPPQueryId min_query_id;
    UInt64 thread_soft_limit;
    UInt64 thread_hard_limit;
    UInt64 estimated_thread_usage;
    /// to prevent from too many queries just issue a part of tasks to occupy threads, in proportion to the hardware cores.
    size_t active_set_soft_limit;
    LoggerPtr log;
};

} // namespace DB
