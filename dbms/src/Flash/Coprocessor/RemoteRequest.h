// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <TiDB/Schema/TiDB.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <vector>

namespace DB
{
using RegionRetryList = std::list<std::reference_wrapper<const RegionInfo>>;
using DAGColumnInfo = std::pair<String, TiDB::ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

struct RemoteRequest
{
    RemoteRequest(
        tipb::DAGRequest && dag_request_,
        DAGSchema && schema_,
        std::vector<pingcap::coprocessor::KeyRange> && key_ranges_,
        UInt64 connection_id_,
        const String & connection_alias_)
        : dag_request(std::move(dag_request_))
        , schema(std::move(schema_))
        , key_ranges(std::move(key_ranges_))
        , connection_id(connection_id_)
        , connection_alias(connection_alias_)
    {}

    static RemoteRequest build(
        const RegionRetryList & retry_regions,
        DAGContext & dag_context,
        const TiDBTableScan & table_scan,
        const TiDB::TableInfo & table_info,
        const FilterConditions & filter_conditions,
        UInt64 connection_id,
        const String & connection_alias,
        const LoggerPtr & log);
    static std::vector<pingcap::coprocessor::KeyRange> buildKeyRanges(const RegionRetryList & retry_regions);
    static std::string printRetryRegions(const RegionRetryList & retry_regions, TableID table_id);

    tipb::DAGRequest dag_request;
    DAGSchema schema;
    /// the sorted key ranges
    std::vector<pingcap::coprocessor::KeyRange> key_ranges;

    UInt64 connection_id;
    String connection_alias;
};
} // namespace DB
