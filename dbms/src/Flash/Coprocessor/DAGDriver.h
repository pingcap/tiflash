#pragma once

#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGRegionInfo.h>
#include <Storages/Transaction/Types.h>
#include <tipb/select.pb.h>

namespace DB
{

class Context;

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, DAGRegionInfo & region_info_, tipb::SelectResponse & dag_response_,
        bool internal_ = false);

    void execute();

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    DAGRegionInfo & region_info;

    tipb::SelectResponse & dag_response;

    bool internal;

    Poco::Logger * log;

    void recordError(Int32 err_code, const String & err_msg);
};
} // namespace DB
