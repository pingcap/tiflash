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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/StorageDeltaMerge.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>


namespace DB
{
namespace FailPoints
{
extern const char skip_check_segment_update[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
/// Helper class to test with multiple segments.
/// You can call `prepareSegments` to prepare multiple segments. After that,
/// you can use `verifyExpectedRowsForAllSegments` to verify the expectation for each segment.
class MultiSegmentTestUtil : private boost::noncopyable
{
protected:
    String tracing_id;
    Context & db_context;
    DeltaMergeStorePtr store;

public:
    std::map<size_t, size_t> rows_by_segments;
    std::map<size_t, size_t> expected_stable_rows;
    std::map<size_t, size_t> expected_delta_rows;

    explicit MultiSegmentTestUtil(Context & db_context_)
        : tracing_id(DB::base::TiFlashStorageTestBasic::getCurrentFullTestName())
        , db_context(db_context_)
    {
        FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
    }

    ~MultiSegmentTestUtil() { FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update); }

    void resetExpectedRows()
    {
        auto * log = &Poco::Logger::get(tracing_id);

        rows_by_segments.clear();
        expected_stable_rows.clear();
        expected_delta_rows.clear();

        std::shared_lock lock(store->read_write_mutex);
        auto segment_idx = 0;
        for (auto & [_key, seg] : store->segments)
        {
            UNUSED(_key);
            LOG_INFO(log, "Segment #{}: Range = {}", segment_idx, seg->getRowKeyRange().toDebugString());
            rows_by_segments[segment_idx] = seg->getEstimatedRows();
            expected_stable_rows[segment_idx] = seg->getStable()->getRows();
            expected_delta_rows[segment_idx] = seg->getDelta()->getRows();
            segment_idx++;
        }
    }

    /// Prepare segments * 4. The rows of each segment will be roughly close to n_avg_rows_per_segment.
    /// The exact rows will be recorded in rows_by_segments.
    void prepareSegments(DeltaMergeStorePtr store_, size_t n_avg_rows_per_segment, DMTestEnv::PkType pk_type)
    {
        store = store_;

        auto dm_context = store->newDMContext(db_context, db_context.getSettingsRef(), /*tracing_id*/ tracing_id);
        {
            // Write [0, 4*N) data with tso=2.
            Block block = DMTestEnv::prepareSimpleWriteBlock(0, n_avg_rows_per_segment * 4, false, pk_type, 2);
            store->write(db_context, db_context.getSettingsRef(), block);
            store->flushCache(dm_context, RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));
        }
        {
            // Check there is only one segment
            ASSERT_EQ(store->segments.size(), 1);
            const auto & [_key, seg] = *store->segments.begin();
            (void)_key;
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
            resetExpectedRows();
            ASSERT_EQ(rows_by_segments.size(), 4);

            // Verify our expectations.
            auto total_stable_rows = 0;
            for (size_t i = 0; i < rows_by_segments.size(); i++)
            {
                ASSERT_EQ(expected_delta_rows[i], 0);
                ASSERT_GT(expected_stable_rows[i], 0); // We don't check the exact rows of each segment.
                total_stable_rows += expected_stable_rows[i];
            }
            ASSERT_EQ(total_stable_rows, 4 * n_avg_rows_per_segment);
        }
        verifyExpectedRowsForAllSegments();
    }

    /// Retry until a segment at index is successfully split.
    void forceForegroundSplit(size_t segment_idx) const
    {
        auto dm_context = store->newDMContext(db_context, db_context.getSettingsRef(), tracing_id);
        while (true)
        {
            store->read_write_mutex.lock();
            auto seg = std::next(store->segments.begin(), segment_idx)->second;
            store->read_write_mutex.unlock();
            auto result = store->segmentSplit(*dm_context, seg, DeltaMergeStore::SegmentSplitReason::ForegroundWrite);
            if (result.first)
            {
                break;
            }
        }
    }

    /// Checks whether current rows in segments meets our expectation.
    void verifyExpectedRowsForAllSegments()
    {
        std::shared_lock lock(store->read_write_mutex);
        ASSERT_EQ(store->segments.size(), expected_delta_rows.size());
        auto segment_idx = 0;
        for (auto & [_key, seg] : store->segments)
        {
            (void)_key;
            ASSERT_EQ(seg->getDelta()->getRows(), expected_delta_rows[segment_idx])
                << "Assert failed for segment #" << segment_idx;
            ASSERT_EQ(seg->getStable()->getRows(), expected_stable_rows[segment_idx])
                << "Assert failed for segment #" << segment_idx;
            segment_idx++;
        }
    }
};

} // namespace tests
} // namespace DM
} // namespace DB
