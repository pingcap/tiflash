#pragma once

#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Storages/Transaction/TiDB.h>
#include <pingcap/coprocessor/Client.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <vector>

namespace DB
{
using RegionRetryList = std::list<std::reference_wrapper<const RegionInfo>>;
using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

struct RemoteRequest
{
    RemoteRequest(tipb::DAGRequest && dag_request_, DAGSchema && schema_, std::vector<pingcap::coprocessor::KeyRange> && key_ranges_)
        : dag_request(std::move(dag_request_))
        , schema(std::move(schema_))
        , key_ranges(std::move(key_ranges_))
    {}
    tipb::DAGRequest dag_request;
    DAGSchema schema;
    /// the sorted key ranges
    std::vector<pingcap::coprocessor::KeyRange> key_ranges;
    static RemoteRequest build(const RegionRetryList & retry_regions, DAGContext & dag_context, const TiDBTableScan & table_scan, const TiDB::TableInfo & table_info, const tipb::Executor * selection, LogWithPrefixPtr & log);
};
} // namespace DB
