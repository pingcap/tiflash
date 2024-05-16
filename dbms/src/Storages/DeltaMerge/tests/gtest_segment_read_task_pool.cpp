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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/KVStore/KVStore.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>
namespace DB::DM::tests
{
class SegmentReadTasksPoolTest : public SegmentTestBasic
{
protected:
    static SegmentPtr createSegment(PageIdU64 seg_id)
    {
        return std::make_shared<Segment>(Logger::get(), 0, RowKeyRange{}, seg_id, seg_id + 1, nullptr, nullptr);
    }

    static SegmentSnapshotPtr createSegmentSnapshot()
    {
        auto delta_snap = std::make_shared<DeltaValueSnapshot>(CurrentMetrics::Metric{});
        delta_snap->delta = std::make_shared<DeltaValueSpace>(nullptr);
        return std::make_shared<SegmentSnapshot>(std::move(delta_snap), /*stable*/ nullptr, Logger::get());
    }

    SegmentReadTaskPtr createSegmentReadTask(PageIdU64 seg_id)
    {
        return std::make_shared<SegmentReadTask>(createSegment(seg_id), createSegmentSnapshot(), RowKeyRanges{});
    }

    static Block createBlock()
    {
        String type_name = "Int64";
        DataTypePtr types[2];
        types[0] = DataTypeFactory::instance().get(type_name);
        types[1] = makeNullable(types[0]);
        ColumnsWithTypeAndName columns;
        for (auto & type : types)
        {
            auto column = type->createColumn();
            for (size_t i = 0; i < 10; i++)
                column->insertDefault();
            columns.emplace_back(std::move(column), type);
        }
        return Block{columns};
    }

    SegmentReadTasks createSegmentReadTasks(const std::vector<PageIdU64> & seg_ids)
    {
        SegmentReadTasks tasks;
        for (PageIdU64 seg_id : seg_ids)
        {
            tasks.push_back(createSegmentReadTask(seg_id));
        }
        return tasks;
    }

    SegmentReadTaskPoolPtr createSegmentReadTaskPool(const std::vector<PageIdU64> & seg_ids)
    {
        DMContextPtr dm_context{createDMContext()};
        return std::make_shared<SegmentReadTaskPool>(
            dm_context->physical_table_id,
            /*extra_table_id_index_*/ dm_context->physical_table_id,
            dm_context,
            /*columns_to_read_*/ ColumnDefines{},
            /*filter_*/ nullptr,
            /*max_version_*/ 0,
            /*expected_block_size_*/ DEFAULT_BLOCK_SIZE,
            /*read_mode_*/ ReadMode::Bitmap,
            createSegmentReadTasks(seg_ids),
            /*after_segment_read_*/ [&](const DMContextPtr &, const SegmentPtr &) { /*do nothing*/ },
            /*tracing_id_*/ String{},
            /*enable_read_thread_*/ true,
            /*num_streams_*/ 1,
            /*res_group_name_*/ String{});
    }

    void schedulerBasic()
    {
        SegmentReadTaskScheduler scheduler{false};

        // Create
        auto pool = createSegmentReadTaskPool(test_seg_ids);
        pool->increaseUnorderedInputStreamRefCount();
        ASSERT_EQ(pool->getPendingSegmentCount(), test_seg_ids.size());

        // Submit to pending_pools
        scheduler.add(pool);
        {
            std::lock_guard lock(scheduler.pending_mtx); // Disable TSA warnnings
            ASSERT_EQ(scheduler.pending_pools.size(), 1);
        }
        ASSERT_EQ(scheduler.read_pools.size(), 0);

        // Reap the pending_pools
        scheduler.reapPendingPools();
        {
            std::lock_guard lock(scheduler.pending_mtx); // Disable TSA warnnings
            ASSERT_EQ(scheduler.pending_pools.size(), 0);
        }
        ASSERT_EQ(scheduler.read_pools.size(), 1);

        // Schedule segment to reach limitation
        auto active_segment_limits = pool->getFreeActiveSegments();
        ASSERT_GT(active_segment_limits, 0);
        std::vector<MergedTaskPtr> merged_tasks;
        for (int i = 0; i < active_segment_limits; ++i)
        {
            auto merged_task = scheduler.scheduleMergedTask(pool);
            ASSERT_NE(merged_task, nullptr);
            merged_tasks.push_back(merged_task);
        }
        ASSERT_EQ(scheduler.scheduleMergedTask(pool), nullptr);

        // Make a segment finished.
        {
            ASSERT_FALSE(scheduler.needScheduleToRead(pool));
            auto merged_task = merged_tasks.back();
            ASSERT_EQ(merged_task->units.size(), 1);
            pool->finishSegment(merged_task->units.front().task->segment);
            ASSERT_TRUE(scheduler.needScheduleToRead(pool));
        }

        // Push block to reach limitation.
        {
            auto free_slot_limits = pool->getFreeBlockSlots();
            ASSERT_GT(free_slot_limits, 0);
            for (int i = 0; i < free_slot_limits; ++i)
            {
                pool->pushBlock(createBlock());
            }
            ASSERT_EQ(pool->getFreeBlockSlots(), 0);
            ASSERT_FALSE(scheduler.needScheduleToRead(pool));

            Block blk;
            pool->popBlock(blk);
            ASSERT_TRUE(blk);
            ASSERT_EQ(pool->getFreeBlockSlots(), 1);
            ASSERT_TRUE(scheduler.needScheduleToRead(pool));

            while (pool->tryPopBlock(blk)) {}
        }

        // Finish
        {
            while (!merged_tasks.empty())
            {
                auto merged_task = merged_tasks.back();
                merged_tasks.pop_back();
                pool->finishSegment(merged_task->units.front().task->segment);
            }

            for (;;)
            {
                auto merged_task = scheduler.scheduleMergedTask(pool);
                if (merged_task == nullptr)
                {
                    break;
                }
                pool->finishSegment(merged_task->units.front().task->segment);
            }

            ASSERT_EQ(pool->q.size(), 0);
            Block blk;
            ASSERT_FALSE(pool->q.pop(blk));

            pool->decreaseUnorderedInputStreamRefCount();
            ASSERT_FALSE(pool->valid());
        }
    }

    inline static const std::vector<PageIdU64> test_seg_ids{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
};

TEST_F(SegmentReadTasksPoolTest, UnorderedWrapper)
{
    SegmentReadTasksWrapper tasks_wrapper(true, createSegmentReadTasks(test_seg_ids));

    bool exception_happened = false;
    try
    {
        tasks_wrapper.nextTask();
    }
    catch (const Exception & e)
    {
        exception_happened = true;
    }
    ASSERT_TRUE(exception_happened);

    ASSERT_FALSE(tasks_wrapper.empty());
    const auto & tasks = tasks_wrapper.getTasks();
    ASSERT_EQ(tasks.size(), test_seg_ids.size());

    std::random_device rd;
    std::mt19937 g(rd());
    std::vector<PageIdU64> v = test_seg_ids;
    std::shuffle(v.begin(), v.end(), g);
    for (PageIdU64 seg_id : v)
    {
        auto task = tasks_wrapper.getTask(seg_id);
        ASSERT_NE(task, nullptr);
        ASSERT_EQ(task->segment->segmentId(), seg_id);
        task = tasks_wrapper.getTask(seg_id);
        ASSERT_EQ(task, nullptr);
    }
    ASSERT_TRUE(tasks_wrapper.empty());
}

TEST_F(SegmentReadTasksPoolTest, OrderedWrapper)
{
    SegmentReadTasksWrapper tasks_wrapper(false, createSegmentReadTasks(test_seg_ids));

    bool exception_happened = false;
    try
    {
        tasks_wrapper.getTasks();
    }
    catch (const Exception & e)
    {
        exception_happened = true;
    }
    ASSERT_TRUE(exception_happened);

    ASSERT_FALSE(tasks_wrapper.empty());

    for (PageIdU64 seg_id : test_seg_ids)
    {
        auto task = tasks_wrapper.nextTask();
        ASSERT_EQ(task->segment->segmentId(), seg_id);
    }
    ASSERT_TRUE(tasks_wrapper.empty());
    ASSERT_EQ(tasks_wrapper.nextTask(), nullptr);
}

TEST_F(SegmentReadTasksPoolTest, SchedulerBasic)
try
{
    schedulerBasic();
}
CATCH

} // namespace DB::DM::tests