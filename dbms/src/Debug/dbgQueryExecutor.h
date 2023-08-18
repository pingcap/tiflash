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

#include <Debug/dbgQueryCompiler.h>
namespace DB
{
using MockServerConfig = tests::MockServerConfig;
struct DecodedTiKVKey;
using DecodedTiKVKeyPtr = std::shared_ptr<DecodedTiKVKey>;

BlockInputStreamPtr executeQuery(
    Context & context,
    RegionID region_id,
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    MakeResOutputStream & func_wrap_output_stream);
BlockInputStreamPtr executeMPPQuery(Context & context, const DAGProperties & properties, QueryTasks & query_tasks);
BlockInputStreamPtr executeNonMPPQuery(
    Context & context,
    RegionID region_id,
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    MakeResOutputStream & func_wrap_output_stream);
BlockInputStreamPtr executeMPPQueryWithMultipleContext(
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    std::unordered_map<size_t, MockServerConfig> & server_config_map);

tipb::SelectResponse executeDAGRequest(
    Context & context,
    tipb::DAGRequest & dag_request,
    RegionID region_id,
    UInt64 region_version,
    UInt64 region_conf_version,
    Timestamp start_ts,
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges);

bool runAndCompareDagReq(
    const coprocessor::Request & req,
    const coprocessor::Response & res,
    Context & context,
    String & unequal_msg);
} // namespace DB