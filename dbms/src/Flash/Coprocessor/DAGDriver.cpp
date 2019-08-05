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
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

DAGDriver::DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, RegionID region_id_, UInt64 region_version_,
    UInt64 region_conf_version_, tipb::SelectResponse & dag_response_)
    : context(context_),
      dag_request(dag_request_),
      region_id(region_id_),
      region_version(region_version_),
      region_conf_version(region_conf_version_),
      dag_response(dag_response_)
{}

void DAGDriver::execute()
{
    context.setSetting("read_tso", UInt64(dag_request.start_ts()));
    BlockIO streams = executeDAG();
    if (!streams.in || streams.out)
    {
        // Only query is allowed, so streams.in must not be null and streams.out must be null
        throw Exception("DAG is not query.", ErrorCodes::LOGICAL_ERROR);
    }
    BlockOutputStreamPtr outputStreamPtr = std::make_shared<DAGBlockOutputStream>(
        dag_response, context.getSettings().dag_records_per_chunk, dag_request.encode_type(), streams.in->getHeader());
    copyData(*streams.in, *outputStreamPtr);
}

BlockIO DAGDriver::executeDAG()
{
    String planner = context.getSettings().dag_planner;
    if (planner == "sql")
    {
        DAGStringConverter converter(context, dag_request);
        String query = converter.buildSqlString();
        if (query.empty())
        {
            return BlockIO();
        }
        return executeQuery(query, context, false, QueryProcessingStage::Complete);
    }
    else if (planner == "optree")
    {
        return executeQuery(dag_request, region_id, region_version, region_conf_version, context, QueryProcessingStage::Complete);
    }
    else
    {
        throw Exception("Unknown DAG planner type " + planner, ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
