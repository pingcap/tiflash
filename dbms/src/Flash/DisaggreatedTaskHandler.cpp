#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/DisaggreatedTaskHandler.h>
#include <Flash/Disaggregated/DisaggregatedTask.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <grpcpp/support/status.h>

#include <ext/scope_guard.h>
#include <memory>

namespace DB
{
grpc::Status DisaggregatedTaskHandler::execute(const ContextPtr & context)
{
    RUNTIME_CHECK(context->isDisaggregatedStorageMode());

    DisaggregatedTaskPtr task = nullptr;
    SCOPE_EXIT({
        current_memory_tracker = nullptr;
    });

    try
    {
        task = std::make_shared<DisaggregatedTask>(context);
        task->prepare(request);

        // We need to refresh the read node's region cache
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

        task->execute();

        // TODO: set response
        // response->set_store_id(int64_t value);
        // response->set_snapshot_id(int64_t value);
        // response->add_segments()
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "disaggregated task meet error: {}", e.displayText());
        auto * err = response->mutable_error();
        err->set_msg(e.displayText());
        // TODO unregister
    }
    catch (std::exception & e)
    {
        LOG_ERROR(log, "disaggregated task meet error: {}", e.what());
        auto * err = response->mutable_error();
        err->set_msg(e.what());
        // TODO unregister
    }
    catch (...)
    {
        LOG_ERROR(log, "disaggregated task meet fatal error");
        auto * err = response->mutable_error();
        err->set_msg("fatal error");
        // TODO unregister
    }

    return grpc::Status::OK;
}
} // namespace DB
