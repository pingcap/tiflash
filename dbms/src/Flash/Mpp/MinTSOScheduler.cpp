#include <Flash/Mpp/MPPTaskManager.h>
#include <Flash/Mpp/MinTSOScheduler.h>

namespace DB
{
MinTSOScheduler::MinTSOScheduler(MPPTaskManagerPtr task_manager_)
    : task_manager(task_manager_)
    , min_tso(0)
    , thread_soft_limit(5000)
    , thread_hard_limit(8000)
    , used_threads(0)
    , default_threads(100)
    , log(&Poco::Logger::get("MinTSOScheduler"))
{
    assert(thread_hard_limit > thread_soft_limit);
}
MinTSOScheduler::~MinTSOScheduler()
{
}
bool MinTSOScheduler::putWaitingQuery(MPPTaskPtr task)
{
    auto id = task->getId();
    std::lock_guard<std::mutex> lock(mu);
    if (min_tso == 0 || id.start_ts <= min_tso) /// must executing
    {
        if (used_threads + default_threads <= thread_hard_limit) /// have threads under thread_hard_limit
        {
            task->scheduleThisTask();
            auto query_task_set = task_manager->getQueryTaskSet(id.start_ts);

            if (nullptr == query_task_set) /// cancelled
            {
                return false;
            }

            if (query_task_set->scheduled_task == 0 && query_task_set->to_be_cancelled == false)
            {
                active_set.insert(id.start_ts);
                min_tso = id.start_ts;
            }
            query_task_set->used_threads += default_threads;
            ++query_task_set->scheduled_task;
            used_threads += default_threads;
        }
        else
        {
            throw Exception("threads are unavailable!");
        }
    }
    else
    {
        if (used_threads + default_threads <= thread_soft_limit) /// have threads under thread_soft_limit
        {
            task->scheduleThisTask();
            auto query_task_set = task_manager->getQueryTaskSet(id.start_ts);

            if (nullptr == query_task_set) /// cancelled
            {
                return false;
            }
            if (query_task_set->scheduled_task == 0 && query_task_set->to_be_cancelled == false)
            {
                active_set.insert(id.start_ts);
            }
            query_task_set->used_threads += default_threads;
            ++query_task_set->scheduled_task;
            used_threads += default_threads;
        }
        else
        {
            waiting_set.insert(id.start_ts);
        }
    }
    return true;
}
void MinTSOScheduler::deleteAndScheduleQueries(UInt64 query_id)
{
    std::lock_guard<std::mutex> lock(mu);
    active_set.erase(query_id);
    waiting_set.erase(query_id);
    auto query_task_set = task_manager->getQueryTaskSet(query_id);
    if (nullptr != query_task_set)
    {
        used_threads -= query_task_set->used_threads;
        if (query_task_set->to_be_cancelled)
            for (const auto & task_it : query_task_set->task_map)
            {
                task_it.second->scheduleThisTask(); /// release this task to run for cancelled tasks
            }
    }

    min_tso = query_id == min_tso ? 0 : min_tso;
    if (min_tso == 0 && !active_set.empty()) /// update the tso from active_set
    {
        min_tso = *active_set.begin();
    }
    while (used_threads + default_threads <= thread_soft_limit)
    {
        UInt64 current_query_id = 0;
        query_task_set = task_manager->getQueryTaskSet(current_query_id);
        while (nullptr == query_task_set || query_task_set->to_be_cancelled) /// find a normal query
        {
            waiting_set.erase(current_query_id);
            if (nullptr != query_task_set && query_task_set->to_be_cancelled)
            {
                ++query_task_set->scheduled_task;
                for (const auto & task_it : query_task_set->task_map)
                {
                    task_it.second->scheduleThisTask(); /// release this task to run
                }
            }
            if (waiting_set.empty())
            {
                return;
            }
            current_query_id = *waiting_set.begin();
            query_task_set = task_manager->getQueryTaskSet(current_query_id);
        }
        for (const auto & task_it : query_task_set->task_map)
        {
            if (used_threads + default_threads <= thread_soft_limit || ((min_tso == current_query_id || min_tso == 0) && used_threads + default_threads <= thread_hard_limit))
            {
                ++query_task_set->scheduled_task;
                query_task_set->used_threads += default_threads;
                active_set.insert(current_query_id);
                used_threads += default_threads;
                task_it.second->scheduleThisTask();
                if (min_tso == 0)
                {
                    min_tso = current_query_id;
                }
            }
            else /// this query cannot fully run
            {
                if (min_tso == current_query_id || min_tso == 0) /// the min_tso query should fully run
                {
                    throw Exception("threads are unavailable!");
                }
                return;
            }
        }
        waiting_set.erase(current_query_id); /// all tasks of this query are fully active
    }
}
} // namespace DB
