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

#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Flash/Mpp/MPPTask.h>
#include <Flash/Mpp/Utils.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_mpp_non_root_task_run[];
extern const char exception_before_mpp_root_task_run[];
} // namespace FailPoints

namespace
{
void addRetryRegion(const ContextPtr & context, mpp::DispatchTaskResponse * response)
{
    // For tiflash_compute mode, all regions are fetched from remote, so no need to refresh TiDB's region cache.
    if (!context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        for (const auto & table_region_info : context->getDAGContext()->tables_regions_info.getTableRegionsInfoMap())
        {
            for (const auto & region : table_region_info.second.remote_regions)
            {
                auto * retry_region = response->add_retry_regions();
                retry_region->set_id(region.region_id);
                retry_region->mutable_region_epoch()->set_conf_ver(region.region_conf_version);
                retry_region->mutable_region_epoch()->set_version(region.region_version);
            }
        }
    }
}
} // namespace

void MPPHandler::handleError(const MPPTaskPtr & task, String error)
{
    if (task)
    {
        try
        {
            task->handleError(error);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Fail to handle error and clean task");
        }
        task->unregisterTask();
    }
}
// execute is responsible for making plan , register tasks and tunnels and start the running thread.
grpc::Status MPPHandler::execute(const ContextPtr & context, mpp::DispatchTaskResponse * response)
{
    MPPTaskPtr task = nullptr;
    SCOPE_EXIT({
        current_memory_tracker = nullptr; /// to avoid reusing threads in gRPC
    });
    try
    {
        Stopwatch stopwatch;
        task = MPPTask::newTask(task_request.meta(), context);
        task->prepare(task_request);

        addRetryRegion(context, response);

#ifndef NDEBUG
        if (task->isRootMPPTask())
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_root_task_run);
        else
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_mpp_non_root_task_run);
#endif

        task->run();
        LOG_INFO(log, "processing dispatch is over; the time cost is {} ms", stopwatch.elapsedMilliseconds());
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : {}", e.displayText());
        auto * err = response->mutable_error();
        err->set_msg(e.displayText());
        handleError(task, e.displayText());
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "dispatch task meet error : {}", e.what());
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
