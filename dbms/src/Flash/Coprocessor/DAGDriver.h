#pragma once

#include <DataStreams/BlockIO.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <tipb/select.pb.h>
#include <kvproto/coprocessor.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <grpcpp/server_context.h>

namespace DB
{

class Context;

class RegionInfo {
public:
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;
    RegionInfo(RegionID id, UInt64 ver, UInt64 conf_ver) : region_id(id), region_version(ver), region_conf_version(conf_ver) {}
};

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const std::vector<RegionInfo> & regions_,
        UInt64 start_ts, UInt64 schema_ver,
        std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> && key_ranges_, tipb::SelectResponse & dag_response_,
        bool internal_ = false);

    void execute();

    void batchExecute(::grpc::ServerWriter< ::coprocessor::BatchResponse>* writer);

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    const std::vector<RegionInfo> & regions;
    //RegionID region_id;
    //UInt64 region_version;
    //UInt64 region_conf_version;
    std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;

    tipb::SelectResponse & dag_response;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
