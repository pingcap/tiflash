#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/Utils.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_mpp_non_root_task_run[];
extern const char exception_before_mpp_root_task_run[];
} // namespace FailPoints

void MPPHandler::handleError(const MPPTaskPtr & task, String error)
{
    try
    {
        if (task)
            task->cancel(error);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fail to handle error and clean task");
    }
}
// execute is responsible for making plan, register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(Context & context, mpp::DispatchTaskResponse * response)
{
    MPPTaskPtr task = nullptr;
    try
    {
        Stopwatch stopwatch;
        task = MPPTask::newTask(task_request.meta(), context);

        auto retry_regions = task->prepare(task_request);
        for (const auto & region : retry_regions)
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

} // namespace DB
