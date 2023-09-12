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

#include <Storages/KVStore/MultiRaft/Disagg/RaftLogManager.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{

TEST(RaftLogEagerGCTasksTest, Basic)
try
{
    RaftLogEagerGcTasks tasks;

    RegionID region_id = 1000;
    // threshold == 0 always return false
    ASSERT_FALSE(tasks.updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000, /*threshold=*/0));
    ASSERT_FALSE(tasks.updateHint(region_id, /*eager_truncated_index=*/10000, /*applied_index=*/10, /*threshold=*/0));

    ASSERT_FALSE(tasks.updateHint(region_id, /*eager_truncated_index=*/10000, /*applied_index=*/10, /*threshold=*/512));

    {
        // create new hints
        ASSERT_TRUE(
            tasks.updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000, /*threshold=*/512));
        // the applied index advance, but not merged into the hints
        ASSERT_FALSE(
            tasks.updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000 + 10, /*threshold=*/512));
        auto hints = tasks.getAndClearHints();
        ASSERT_EQ(hints.size(), 1);
        ASSERT_TRUE(hints.contains(region_id));
        ASSERT_EQ(hints[region_id].applied_index, 10000);
        ASSERT_EQ(hints[region_id].eager_truncate_index, 10);
    }
    {
        auto hints = tasks.getAndClearHints();
        ASSERT_TRUE(hints.empty());
    }

    {
        // create new hints
        ASSERT_TRUE(
            tasks.updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000, /*threshold=*/512));
        // the applied index advance, and merged into the hints
        ASSERT_TRUE(
            tasks
                .updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000 + 523, /*threshold=*/512));
        // applied index rollback, just ignore
        ASSERT_FALSE(
            tasks
                .updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000 + 500, /*threshold=*/512));
        auto hints = tasks.getAndClearHints();
        ASSERT_EQ(hints.size(), 1);
        ASSERT_TRUE(hints.contains(region_id));
        ASSERT_EQ(hints[region_id].applied_index, 10000 + 523);
        ASSERT_EQ(hints[region_id].eager_truncate_index, 10);
    }

    {
        // create new hints
        ASSERT_TRUE(
            tasks.updateHint(region_id, /*eager_truncated_index=*/10, /*applied_index=*/10000, /*threshold=*/512));
        // the applied index and truncated index advance, and merged into the hints
        ASSERT_TRUE(
            tasks
                .updateHint(region_id, /*eager_truncated_index=*/30, /*applied_index=*/10000 + 523, /*threshold=*/512));
        auto hints = tasks.getAndClearHints();
        ASSERT_EQ(hints.size(), 1);
        ASSERT_TRUE(hints.contains(region_id));
        ASSERT_EQ(hints[region_id].applied_index, 10000 + 523);
        ASSERT_EQ(hints[region_id].eager_truncate_index, 10);
    }
}
CATCH

} // namespace DB::tests
