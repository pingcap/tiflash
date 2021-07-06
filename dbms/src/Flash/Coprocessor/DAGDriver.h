#pragma once

#include <DataStreams/BlockIO.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/server_context.h>
#include <kvproto/coprocessor.pb.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Storages/RegionQueryInfo.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <vector>

namespace DB
{

class Context;

struct RegionInfo
{
    RegionID region_id;
    UInt64 region_version;
    UInt64 region_conf_version;

    using RegionReadKeyRanges = std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>>;
    RegionReadKeyRanges key_ranges;
    const std::unordered_set<UInt64> * bypass_lock_ts;

    RegionInfo(
        RegionID id, UInt64 ver, UInt64 conf_ver, RegionReadKeyRanges && key_ranges_, const std::unordered_set<UInt64> * bypass_lock_ts_)
        : region_id(id),
          region_version(ver),
          region_conf_version(conf_ver),
          key_ranges(std::move(key_ranges_)),
          bypass_lock_ts(bypass_lock_ts_)
    {}
};

using RegionInfoMap = std::unordered_map<RegionID, RegionInfo>;
using RegionInfoList = std::vector<RegionInfo>;

/// An abstraction of driver running DAG request.
/// Now is a naive native executor. Might get evolved to drive MPP-like computation.

template <bool batch = false>
class DAGDriver
{
public:
    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const RegionInfoMap & regions_,
        const RegionInfoList & retry_regions_, UInt64 start_ts, UInt64 schema_ver, tipb::SelectResponse * dag_response_,
        bool internal_ = false);

    DAGDriver(Context & context_, const tipb::DAGRequest & dag_request_, const RegionInfoMap & regions_,
        const RegionInfoList & retry_regions_, UInt64 start_ts, UInt64 schema_ver,
        ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer, bool internal_ = false);

    void execute();

private:
    void recordError(Int32 err_code, const String & err_msg);

private:
    Context & context;

    const tipb::DAGRequest & dag_request;

    const RegionInfoMap & regions;
    const RegionInfoList & retry_regions;

    tipb::SelectResponse * dag_response;

    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;

    bool internal;

    Poco::Logger * log;
};
} // namespace DB
