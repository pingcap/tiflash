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

#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo() = default;

SelectQueryInfo::~SelectQueryInfo() = default;

SelectQueryInfo::SelectQueryInfo(const SelectQueryInfo & rhs)
    : query(rhs.query)
    , sets(rhs.sets)
    , mvcc_query_info(rhs.mvcc_query_info != nullptr ? std::make_unique<MvccQueryInfo>(*rhs.mvcc_query_info) : nullptr)
    , dag_query(rhs.dag_query != nullptr ? std::make_unique<DAGQueryInfo>(*rhs.dag_query) : nullptr)
    , req_id(rhs.req_id)
    , keep_order(rhs.keep_order)
    , is_fast_scan(rhs.is_fast_scan)
{}

SelectQueryInfo::SelectQueryInfo(SelectQueryInfo && rhs) noexcept
    : query(std::move(rhs.query))
    , sets(std::move(rhs.sets))
    , mvcc_query_info(std::move(rhs.mvcc_query_info))
    , dag_query(std::move(rhs.dag_query))
    , req_id(std::move(rhs.req_id))
    , keep_order(rhs.keep_order)
    , is_fast_scan(rhs.is_fast_scan)
{}

} // namespace DB
