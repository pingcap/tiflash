// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/SelectQueryInfo.h>

#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Storages/RegionQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo() = default;

SelectQueryInfo::SelectQueryInfo(const SelectQueryInfo & query_info_)
    : query(query_info_.query),
      sets(query_info_.sets),
      mvcc_query_info(query_info_.mvcc_query_info != nullptr ? std::make_unique<MvccQueryInfo>(*query_info_.mvcc_query_info) : nullptr),
      dag_query(query_info_.dag_query != nullptr ? std::make_unique<DAGQueryInfo>(*query_info_.dag_query) : nullptr)
{}

SelectQueryInfo::SelectQueryInfo(SelectQueryInfo && query_info_)
    : query(query_info_.query), sets(query_info_.sets), mvcc_query_info(std::move(query_info_.mvcc_query_info)),
    dag_query(std::move(query_info_.dag_query))
{}

SelectQueryInfo::~SelectQueryInfo() = default;

} // namespace DB
