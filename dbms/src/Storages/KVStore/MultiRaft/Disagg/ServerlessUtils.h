// Copyright 2024 PingCAP, Ltd.
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

#include <Storages/Page/PageStorage_fwd.h>

namespace DB
{
String makeKeyspacePrefix(uint32_t keyspace_id, uint8_t suffix);
String makeRegionPrefix(uint64_t region_id, uint8_t suffix);
String getKeyspaceInnerKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id);
String getRegionInnerKey(UniversalPageStoragePtr uni_ps, uint64_t region_id);
String getCompactibleInnerKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id, uint64_t region_id);
String getRegionEncKey(UniversalPageStoragePtr uni_ps, uint64_t region_id);
String getKeyspaceEncKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id);
String getCompactibleEncKey(UniversalPageStoragePtr uni_ps, uint32_t keyspace_id, uint64_t region_id);
UInt64 getShardVer(UniversalPageStoragePtr uni_ps, uint64_t region_id);
String getTxnFileRef(UniversalPageStoragePtr uni_ps, uint64_t region_id);
} // namespace DB
