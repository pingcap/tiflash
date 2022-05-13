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


#pragma once

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{

/// This utility helps you set up a DMStore with 4 segments.
class MultiSegmentTest : public DB::base::TiFlashStorageTestBasic
{
protected:
    DeltaMergeStorePtr store;
    DMContextPtr dm_context; // Will be set after calling prepareSegments.
    std::map<size_t, size_t> rows_by_segments; // Will be set after calling prepareSegments.
    std::map<size_t, size_t> expected_stable_rows; // Will be set after calling prepareSegments.
    std::map<size_t, size_t> expected_delta_rows; // Will be set after calling prepareSegments.

    /// Retry until a segment at index is successfully split.
    void forceForegroundSplit(size_t segment_idx) const
    {
        while (true)
        {
            store->read_write_mutex.lock();
            auto seg = std::next(store->segments.begin(), segment_idx)->second;
            store->read_write_mutex.unlock();
            auto result = store->segmentSplit(*dm_context, seg, /*is_foreground*/ true);
            if (result.first)
            {
                break;
            }
        }
    }

    /// Prepare segments * 4. The rows of each segment will be roughly close to n_avg_rows_per_segment. The exact rows will be recorded in rows_by_segments.
    void prepareSegments(size_t n_avg_rows_per_segment, bool is_common_handle)
    {
        auto * log = &Poco::Logger::get(GET_GTEST_FULL_NAME);

        // Avoid bg merge.
        // TODO (wenxuan): Seems to be not very stable.
        db_context->setSetting("dt_bg_gc_max_segments_to_check_every_round", UInt64(0));
        db_context->setSetting("dt_segment_limit_rows", UInt64(n_avg_rows_per_segment));

        {
            auto cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
            ColumnDefine handle_column_define = (*cols)[0];
            store = std::make_shared<DeltaMergeStore>(*db_context,
                                                      false,
                                                      "test",
                                                      GET_GTEST_FULL_NAME,
                                                      101,
                                                      *cols,
                                                      handle_column_define,
                                                      is_common_handle,
                                                      1,
                                                      DeltaMergeStore::Settings());
        }
        dm_context = store->newDMContext(*db_context, db_context->getSettingsRef(), /*tracing_id*/ GET_GTEST_FULL_NAME);
        {
            // Write [0, 4*N) data with tso=2.
            Block block = DMTestEnv::prepareSimpleWriteBlock(0, n_avg_rows_per_segment * 4, false, 2, is_common_handle);
            store->write(*db_context, db_context->getSettingsRef(), block);
            store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
        }
        {
            // Check there is only one segment
            ASSERT_EQ(store->segments.size(), 1);
            const auto & [_key, seg] = *store->segments.begin();
            ASSERT_EQ(seg->getDelta()->getRows(), n_avg_rows_per_segment * 4);
            ASSERT_EQ(seg->getStable()->getRows(), 0);

            // Split the segment, now we have two segments with n_rows_per_segment * 2 rows per segment.
            forceForegroundSplit(0);
            ASSERT_EQ(store->segments.size(), 2);
        }
        {
            // Split the 2 segments again.
            forceForegroundSplit(1);
            forceForegroundSplit(0);
            ASSERT_EQ(store->segments.size(), 4);
        }
        {
            std::shared_lock lock(store->read_write_mutex);
            // Now we have 4 segments.
            auto total_stable_rows = 0;
            auto segment_idx = 0;
            for (auto & [_key, seg] : store->segments)
            {
                LOG_FMT_INFO(log, "Segment #{}: Range = {}", segment_idx, seg->getRowKeyRange().toDebugString());
                ASSERT_EQ(seg->getDelta()->getRows(), 0);
                ASSERT_GT(seg->getStable()->getRows(), 0); // We don't check the exact rows of each segment.
                total_stable_rows += seg->getStable()->getRows();
                rows_by_segments[segment_idx] = seg->getStable()->getRows();
                expected_stable_rows[segment_idx] = seg->getStable()->getRows();
                expected_delta_rows[segment_idx] = seg->getDelta()->getRows(); // = 0
                segment_idx++;
            }
            ASSERT_EQ(total_stable_rows, 4 * n_avg_rows_per_segment);
        }
    }

    /// Checks whether current rows in segments meets our expectation.
    void verifyExpectedRowsForAllSegments()
    {
        std::shared_lock lock(store->read_write_mutex);
        ASSERT_EQ(store->segments.size(), 4);
        auto segment_idx = 0;
        for (auto & [_key, seg] : store->segments)
        {
            ASSERT_EQ(seg->getDelta()->getRows(), expected_delta_rows[segment_idx]) << "Assert failed for segment #" << segment_idx;
            ASSERT_EQ(seg->getStable()->getRows(), expected_stable_rows[segment_idx]) << "Assert failed for segment #" << segment_idx;
            segment_idx++;
        }
    }
};

} // namespace tests
} // namespace DM
} // namespace DB