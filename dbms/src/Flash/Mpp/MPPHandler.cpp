#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace FailPoints
{
extern const char exception_before_mpp_non_root_task_run[];
extern const char exception_before_mpp_root_task_run[];
} // namespace FailPoints

void MPPHandler::handleError(MPPTaskPtr task, String error)
{
    try
    {
        if (task != nullptr)
        {
            task->closeAllTunnel(error);
            task->unregisterTask();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fail to handle error and clean task");
    }
}
// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    MPPTaskPtr task = nullptr;
    try
    {
        Stopwatch stopwatch;
        task = MPPTask::newTask(task_request.meta(), context);

        auto retry_regions = task->prepare(task_request);
        for (auto region : retry_regions)
        {
            auto * retry_region = response->add_retry_regions();
            retry_region->set_id(region.region_id);
            retry_region->mutable_region_epoch()->set_conf_ver(region.region_conf_version);
            retry_region->mutable_region_epoch()->set_version(region.region_version);
        }
        if (task->isRootMPPTask())
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_root_task_run);
        }
        else
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_non_root_task_run);
        }
        task->run();
        LOG_INFO(log, "processing dispatch is over; the time cost is " << std::to_string(stopwatch.elapsedMilliseconds()) << " ms");
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.displayText());
        auto * err = response->mutable_error();
        err->set_msg(e.displayText());
        handleError(task, e.displayText());
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : " << e.what());
        auto * err = response->mutable_error();
        err->set_msg(e.what());
        handleError(task, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "dispatch task meet fatal error");
        auto * err = response->mutable_error();
        err->set_msg("fatal error");
        handleError(task, "fatal error");
    }
    return grpc::Status::OK;
}

MPPTaskManager::MPPTaskManager() : log(&Logger::get("TaskManager")) {}

MPPTaskPtr MPPTaskManager::findTaskWithTimeout(const mpp::TaskMeta & meta, std::chrono::seconds timeout, std::string & errMsg)
{
    MPPTaskId id{meta.start_ts(), meta.task_id()};
    std::map<MPPTaskId, MPPTaskPtr>::iterator it;
    bool cancelled = false;
    std::unique_lock<std::mutex> lock(mu);
    auto ret = cv.wait_for(lock, timeout, [&] {
        auto query_it = mpp_query_map.find(id.start_ts);
        // TODO: how about the query has been cancelled in advance?
        if (query_it == mpp_query_map.end())
        {
            return false;
        }
        else if (query_it->second.to_be_cancelled)
        {
            /// if the query is cancelled, return true to stop waiting timeout.
            LOG_WARNING(log, "Query " + std::to_string(id.start_ts) + " is cancelled, all its tasks are invalid.");
            cancelled = true;
            return true;
        }
        it = query_it->second.task_map.find(id);
        return it != query_it->second.task_map.end();
    });
    if (cancelled)
    {
        errMsg = "Task [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] has been cancelled.";
        return nullptr;
    }
    else if (!ret)
    {
        errMsg = "Can't find task [" + DB::toString(meta.start_ts()) + "," + DB::toString(meta.task_id()) + "] within "
            + DB::toString(timeout.count()) + " s.";
        return nullptr;
    }
    return it->second;
}

void MPPTaskManager::cancelMPPQuery(UInt64 query_id, const String & reason)
{
    MPPQueryTaskSet task_set;
    {
        /// cancel task may take a long time, so first
        /// set a flag, so we can cancel task one by
        /// one without holding the lock
        std::lock_guard<std::mutex> lock(mu);
        auto it = mpp_query_map.find(query_id);
        if (it == mpp_query_map.end() || it->second.to_be_cancelled)
            return;
        it->second.to_be_cancelled = true;
        task_set = it->second;
        cv.notify_all();
    }
    LOG_WARNING(log, "Begin cancel query: " + std::to_string(query_id));
    std::stringstream ss;
    ss << "Remaining task in query " + std::to_string(query_id) + " are: ";
    // TODO: cancel tasks in order rather than issuing so many threads to cancel tasks
    std::vector<std::thread> cancel_workers;
    for (auto task_it = task_set.task_map.rbegin(); task_it != task_set.task_map.rend(); task_it++)
    {
        ss << task_it->first.toString() << " ";
        std::thread t(&MPPTask::cancel, task_it->second, std::ref(reason));
        cancel_workers.push_back(std::move(t));
    }
    LOG_WARNING(log, ss.str());
    for (auto & worker : cancel_workers)
    {
        worker.join();
    }
    MPPQueryTaskSet canceled_task_set;
    {
        std::lock_guard<std::mutex> lock(mu);
        /// just to double check the query still exists
        auto it = mpp_query_map.find(query_id);
        if (it != mpp_query_map.end())
        {
            /// hold the canceled task set, so the mpp task will not be deconstruct when holding the
            /// `mu` of MPPTaskManager, otherwise it might cause deadlock
            canceled_task_set = it->second;
            mpp_query_map.erase(it);
        }
    }
    LOG_WARNING(log, "Finish cancel query: " + std::to_string(query_id));
}

bool MPPTaskManager::registerTask(MPPTaskPtr task)
{
    std::unique_lock<std::mutex> lock(mu);
    const auto & it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end() && it->second.to_be_cancelled)
    {
        LOG_WARNING(log, "Do not register task: " + task->id.toString() + " because the query is to be cancelled.");
        cv.notify_all();
        return false;
    }
    if (it != mpp_query_map.end() && it->second.task_map.find(task->id) != it->second.task_map.end())
    {
        throw Exception("The task " + task->id.toString() + " has been registered");
    }
    mpp_query_map[task->id.start_ts].task_map.emplace(task->id, task);
    task->manager = this;
    cv.notify_all();
    return true;
}

void MPPTaskManager::unregisterTask(MPPTask * task)
{
    std::unique_lock<std::mutex> lock(mu);
    auto it = mpp_query_map.find(task->id.start_ts);
    if (it != mpp_query_map.end())
    {
        if (it->second.to_be_cancelled)
            return;
        auto task_it = it->second.task_map.find(task->id);
        if (task_it != it->second.task_map.end())
        {
            it->second.task_map.erase(task_it);
            if (it->second.task_map.empty())
                /// remove query task map if the task is the last one
                mpp_query_map.erase(it);
            return;
        }
    }
    LOG_ERROR(log, "The task " + task->id.toString() + " cannot be found and fail to unregister");
}

MPPTaskManager::~MPPTaskManager() {}

} // namespace DB
