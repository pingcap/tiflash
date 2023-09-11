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

#include <Storages/KVStore/TiKVHelpers/PDTiKVClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

std::atomic<Timestamp> PDClientHelper::cached_gc_safe_point = 0;
std::atomic<std::chrono::time_point<std::chrono::steady_clock>> PDClientHelper::safe_point_last_update_time;

// Keyspace gc safepoint cache and update time.
bool PDClientHelper::enable_safepoint_v2 = false;
std::unordered_map<KeyspaceID, KeyspaceGCInfo> PDClientHelper::ks_gc_sp_map;
std::shared_mutex PDClientHelper::ks_gc_sp_mutex;

} // namespace DB
