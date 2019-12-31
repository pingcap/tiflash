#pragma once

#include <DataStreams/BlockIO.h>
#include <Storages/Transaction/TiKVKeyValue.h>
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
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, RegionID region_id_, UInt64 region_version_,
        UInt64 region_conf_version_, UInt64 start_ts, std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> && key_ranges_,
        tipb::SelectResponse & dag_response_, bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;
    std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;

    tipb::SelectResponse & dag_response;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
