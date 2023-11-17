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

#include <Core/Defines.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
struct DAGProperties
{
    String encode_type;
    Int64 tz_offset = 0;
    String tz_name;
    Int32 collator = 0;
    bool is_mpp_query = false;
    bool use_broadcast_join = false;
    Int32 mpp_partition_num = 1;
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    Int64 gather_id = 0;
    UInt64 query_ts = 0;
    UInt64 server_id = 1;
    UInt64 local_query_id = 1;
    Int64 task_id = 1;

    Int32 mpp_timeout = 60;
};
} // namespace DB
