#pragma once

#include <Storages/RegionQueryInfo.h>
#include <tipb/select.pb.h>

namespace DB
{

class Context;

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, RegionID region_id_, UInt64 region_version_,
        UInt64 region_conf_version_, tipb::SelectResponse & dag_response_);
    bool execute();

private:
    BlockIO executeDAG();

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;

    tipb::SelectResponse & dag_response;
};
} // namespace DB
