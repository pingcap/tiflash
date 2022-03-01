#pragma once

#include <Flash/Mpp/MPPTask.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{
/// scheduling tasks in the set according to the tso order under the soft limit of threads, but allow the min_tso query to preempt threads under the hard limit of threads. The min_tso query avoids the deadlock resulted from threads competition among nodes.
/// schedule tasks under the lock protection of the task manager
class MinTSOScheduler : private boost::noncopyable
{
public:
    MinTSOScheduler(UInt64 soft_limit, UInt64 hard_limit);
    ~MinTSOScheduler() = default;
    bool tryToSchedule(MPPTaskPtr task, MPPTaskManager & task_manager);
    void deleteQueryIfCancelled(UInt64, MPPTaskManager &);
    void deleteAndScheduleQueries(UInt64, MPPTaskManager &);

private:
    std::set<UInt64> waiting_set;
    std::set<UInt64> active_set;
    UInt64 min_tso;
    UInt64 thread_soft_limit;
    UInt64 thread_hard_limit;
    UInt64 used_threads;
    Poco::Logger * log;
};

} // namespace DB
