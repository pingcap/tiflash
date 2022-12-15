#include <Common/Exception.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/DisaggreatedTaskHandler.h>
#include <Interpreters/Context.h>
#include <grpcpp/support/status.h>
#include <Flash/executeQuery.h>

namespace DB
{
grpc::Status DisaggregatedTaskHandler::execute(const ContextPtr & context)
{
    UNUSED(request, response);
    RUNTIME_CHECK(context->isDisaggregatedStorageMode());

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

    query_executor_holder.set(queryExecute(*context));

    return grpc::Status::OK;
}
} // namespace DB
