#include <Flash/Coprocessor/DAGDriver.h>

#include <Core/QueryProcessingStage.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/DAGBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/DAGStringConverter.h>
#include <Interpreters/executeQuery.h>

namespace DB
{

DAGDriver::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, RegionID region_id_, UInt64 region_version_,
    UInt64 region_conf_version_, tipb::SelectResponse & dag_response_)
    : context(context_),
      dag_request(dag_request_),
      region_id(region_id_),
      region_version(region_version_),
      region_conf_version(region_conf_version_),
      dag_response(dag_response_)
{}

bool DAGDriver::execute()
{
    context.setSetting("read_tso", UInt64(dag_request.start_ts()));
    BlockIO streams = executeDAG();
    if (!streams.in || streams.out)
    {
        // only query is allowed, so streams.in must not be null and streams.out must be null
        return false;
    }
    BlockOutputStreamPtr outputStreamPtr = std::make_shared<DAGBlockOutputStream>(
        dag_response, context.getSettings().records_per_chunk, dag_request.encode_type(), streams.in->getHeader());
    copyData(*streams.in, *outputStreamPtr);
    return true;
}

BlockIO DAGDriver::executeDAG()
{
    String builder_version = context.getSettings().coprocessor_plan_builder_version;
    if (builder_version == "v1")
    {
        DAGStringConverter converter(context, dag_request);
        String query = converter.buildSqlString();
        if (query.empty())
        {
            return BlockIO();
        }
        return executeQuery(query, context, false, QueryProcessingStage::Complete);
    }
    else if (builder_version == "v2")
    {
        return executeQuery(dag_request, region_id, region_version, region_conf_version, context, QueryProcessingStage::Complete);
    }
    else
    {
        throw Exception("coprocessor plan builder version should be set to v1 or v2");
    }
}

} // namespace DB
