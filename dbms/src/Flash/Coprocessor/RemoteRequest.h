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

#include <Flash/Coprocessor/PushDownFilter.h>
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
    RemoteRequest(
        tipb::DAGRequest && dag_request_,
        DAGSchema && schema_,
        std::vector<pingcap::coprocessor::KeyRange> && key_ranges_)
        : dag_request(std::move(dag_request_))
        , schema(std::move(schema_))
        , key_ranges(std::move(key_ranges_))
    {}
    tipb::DAGRequest dag_request;
    DAGSchema schema;
    /// the sorted key ranges
    std::vector<pingcap::coprocessor::KeyRange> key_ranges;
    static RemoteRequest build(
        const RegionRetryList & retry_regions,
        DAGContext & dag_context,
        const TiDBTableScan & table_scan,
        const TiDB::TableInfo & table_info,
        const PushDownFilter & push_down_filter,
        const LoggerPtr & log);
};
} // namespace DB
