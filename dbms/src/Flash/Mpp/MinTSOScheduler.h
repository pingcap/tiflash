#pragma once

#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{
/// scheduling tasks in the set according to the tso order under the soft limit of threads, but allow the min_tso query to preempt threads under the hard limit of threads.
/// The min_tso query avoids the deadlock resulted from threads competition among nodes.
/// schedule tasks under the lock protection of the task manager
class MinTSOScheduler : private boost::noncopyable
{
public:
    MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit);
    ~MinTSOScheduler() = default;
    /// try to schedule this task if it is the min_tso query or there are enough threads, otherwise put it into the waiting set.
    /// NOTE: call tryToSchedule under the lock protection of MPPTaskManager
    bool tryToSchedule(MPPTaskPtr task, MPPTaskManager & task_manager);

    /// delete this to-be cancelled query from scheduler and update min_tso if needed, so that there aren't cancelled queries in the scheduler.
    /// NOTE: call deleteCancelledQuery under the lock protection of MPPTaskManager
    void deleteCancelledQuery(UInt64 tso, MPPTaskManager & task_manager);

    /// delete the query in the active set and waiting set and release threads, then schedule waiting tasks.
    /// NOTE: call deleteThenSchedule under the lock protection of MPPTaskManager,
    /// so this func is called exactly once for a query.
    void deleteThenSchedule(UInt64 tso, MPPTaskManager & task_manager);

private:
    bool scheduleImp(UInt64 tso, MPPQueryTaskSetPtr query_task_set, MPPTaskPtr task, bool isWaiting);
    void updateMinTSO(UInt64 tso, bool valid, String msg);
    void scheduleWaitingQueries(MPPTaskManager & task_manager);
    std::set<UInt64> waiting_set;
    std::set<UInt64> active_set;
    UInt64 min_tso;
    UInt64 thread_soft_limit;
    UInt64 thread_hard_limit;
    UInt64 used_threads;
    /// to prevent from too many queries just issue a part of tasks to occupy threads, in proportion to the hardware cores.
    size_t active_set_soft_limit;
    Poco::Logger * log;
};

} // namespace DB
