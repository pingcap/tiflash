#pragma once

#include <Flash/Mpp/MPPTask.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{
class MinTSOScheduler : private boost::noncopyable
{
public:
    MinTSOScheduler(MPPTaskManagerPtr task_manager_, UInt64 soft_limit, UInt64 hard_limit);
    ~MinTSOScheduler() = default;
    bool putWaitingQuery(MPPTaskPtr);
    void deleteAndScheduleQueries(UInt64);

private:
    std::mutex mu;
    std::set<UInt64> waiting_set;
    std::set<UInt64> active_set;
    MPPTaskManagerPtr task_manager;
    UInt64 min_tso;
    UInt64 thread_soft_limit;
    UInt64 thread_hard_limit;
    UInt64 used_threads;
    Poco::Logger * log;
};

} // namespace DB
