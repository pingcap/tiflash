// Copyright 2025 PingCAP, Inc.
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

#include <Common/MemoryTracker.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockTiDB.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/KVStore/ProxyStateMachine.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/RegionQueryInfo.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/config_common.h> // Included for `USE_JEMALLOC`

#include <limits>

namespace DB::tests
{

TEST(ProxyStateMachine, Life)
try
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    ProxyStateMachine m =
}
CATCH

} // namespace DB::tests
