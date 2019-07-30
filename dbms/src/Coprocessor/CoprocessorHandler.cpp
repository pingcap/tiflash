#include <Coprocessor/CoprocessorHandler.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/Codec.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/TidbCopBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterDagRequest.h>
#include <Interpreters/DagStringConverter.h>
#include <Interpreters/StringQueryInfo.h>

namespace DB
{

CoprocessorHandler::CoprocessorHandler(const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_, CoprocessorContext & context_)
    : cop_request(cop_request_), cop_response(cop_response_), context(context_)
{
    if(!dag_request.ParseFromString(cop_request->data())) {
        throw Exception("Could not extract dag request from coprocessor request");
    }
}

CoprocessorHandler::~CoprocessorHandler()
{
}

BlockIO CoprocessorHandler::buildCHPlan() {
    String builder_version = context.ch_context.getSettings().coprocessor_plan_builder_version;
    if(builder_version == "v1") {
        DagStringConverter converter(context, dag_request);
        String query = converter.buildSqlString();
        return executeQuery(query, context.ch_context, false, QueryProcessingStage::Complete);
    } else if (builder_version == "v2"){
        return executeQuery(dag_request, context, QueryProcessingStage::Complete);
    } else {
        throw Exception("coprocessor plan builder version should be set to v1 or v2");
    }
}

bool CoprocessorHandler::execute() {
    context.ch_context.setSetting("read_tso", UInt64(dag_request.start_ts()));
    //todo set region related info
    BlockIO streams = buildCHPlan();
    if(!streams.in || streams.out) {
        // only query is allowed, so streams.in must not be null and streams.out must be null
        return false;
    }
    tipb::SelectResponse select_response;
    BlockOutputStreamPtr outputStreamPtr = std::make_shared<TidbCopBlockOutputStream>(
            &select_response, context.ch_context.getSettings().records_per_chunk, dag_request.encode_type(), streams.in->getHeader()
            );
    copyData(*streams.in, *outputStreamPtr);
    cop_response->set_data(select_response.SerializeAsString());
    return true;
}

}

