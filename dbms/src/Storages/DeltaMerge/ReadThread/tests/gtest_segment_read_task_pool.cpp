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

#include <Common/MemoryTrackerSetter.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Schedule/Tasks/Impls/SimplePipelineTask.h>
#include <Interpreters/Context.h>
#include <Operators/ConcatSourceOp.h>
#include <Operators/UnorderedSourceOp.h>
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

    SegmentSnapshotPtr createSegmentSnapshot()
    {
        auto delta = std::make_shared<DeltaValueSpace>(1);
        auto delta_snap = delta->createSnapshot(*createDMContext(), false, CurrentMetrics::Metric{});

        auto stable = std::make_shared<StableValueSpace>(1);
        auto stable_snap = stable->createSnapshot();
        return std::make_shared<SegmentSnapshot>(std::move(delta_snap), std::move(stable_snap), Logger::get());
    }

    SegmentReadTaskPtr createSegmentReadTask(PageIdU64 seg_id)
    {
        return std::make_shared<SegmentReadTask>(
            createSegment(seg_id),
            createSegmentSnapshot(),
            createDMContext(),
            RowKeyRanges{});
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

    GlobalSegmentID createGlobalSegmentID(PageIdU64 seg_id)
    {
        auto dm_context = createDMContext();
        return GlobalSegmentID{
            .store_id = dm_context->global_context.getTMTContext().getKVStore()->getStoreID(),
            .keyspace_id = dm_context->keyspace_id,
            .physical_table_id = dm_context->physical_table_id,
            .segment_id = seg_id,
            .segment_epoch = 0,
        };
    }

    SegmentReadTaskPoolPtr createSegmentReadTaskPool(const std::vector<PageIdU64> & seg_ids)
    {
        auto dm_context = createDMContext();
        return std::make_shared<SegmentReadTaskPool>(
            /*extra_table_id_index_*/ dm_context->physical_table_id,
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
            /*active_segment_limit_*/ 2,
            /*keyspace_id_*/ NullspaceID,
            /*res_group_name_*/ String{});
    }

    std::unique_ptr<UnorderedSourceOp> createRemoteSource(
        PipelineExecutorContext & exec_context,
        const SegmentReadTaskPoolPtr & pool)
    {
        return std::make_unique<UnorderedSourceOp>(
            exec_context,
            pool,
            *tableColumns(),
            MutSup::invalid_col_id,
            "remote_source_stop_test",
            RuntimeFilterList{},
            0,
            /*is_disagg_=*/true);
    }

    static SegmentReadTaskScheduler createPausedScheduler() { return SegmentReadTaskScheduler{false}; }

    static void recordRemoteConnectionInfo(const SegmentReadTaskPoolPtr & pool)
    {
        const auto & task = pool->getTasks().begin()->second;
        task->extra_remote_info.emplace();
        auto & info = task->extra_remote_info->connection_profile_info;
        info = ConnectionProfileInfo::createForInterZone();
        info.bytes = 123;
        pool->recordRemoteConnectionInfoIfNecessary(task);
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
            pool->finishSegment(merged_task->units.front().task);
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
                pool->finishSegment(merged_task->units.front().task);
            }

            for (;;)
            {
                auto merged_task = scheduler.scheduleMergedTask(pool);
                if (merged_task == nullptr)
                {
                    break;
                }
                pool->finishSegment(merged_task->units.front().task);
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
        auto global_seg_id = createGlobalSegmentID(seg_id);
        auto task = tasks_wrapper.getTask(global_seg_id);
        ASSERT_NE(task, nullptr);
        ASSERT_EQ(task->segment->segmentId(), seg_id);
        task = tasks_wrapper.getTask(global_seg_id);
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

TEST_F(SegmentReadTasksPoolTest, PipelineStopBeforeRemoteReadInitialization)
{
    MemoryTrackerSetter tracker(true, nullptr);
    for (bool whole_pipeline : {false, true})
    {
        SCOPED_TRACE(whole_pipeline);
        PipelineExecutorContext exec_context;
        auto pool = createSegmentReadTaskPool({1});
        // Register with a scheduler without a worker thread, keeping the first read pending
        // while SimplePipelineTask executes the real source prefix and suffix.
        auto scheduler = createPausedScheduler();
        std::call_once(pool->addToSchedulerFlag(), [&] { scheduler.add(pool); });
        auto source = createRemoteSource(exec_context, pool);
        auto source_profile = source->getIOProfileInfo();
        auto remaining_source = createRemoteSource(exec_context, pool);
        remaining_source->operatePrefix();
        auto stop = std::make_shared<TaskStopSource>();
        Block output;
        PipelineExecBuilder builder;
        builder.setSourceOp(std::move(source));
        builder.setSinkOp(std::make_unique<SetBlockSinkOp>(exec_context, "", output));
        builder.input_stops.push_back({stop, whole_pipeline ? std::nullopt : std::make_optional(size_t{0})});
        SimplePipelineTask task(exec_context, "", builder.build(false));
        ASSERT_GT(pool->getTotalReadTasks(), 0);
        ASSERT_FALSE(pool->getRemoteConnectionInfo().has_value());

        stop->stop();
        EXPECT_EQ(task.execute(), ExecTaskStatus::FINISHED);
        task.finalize();
        EXPECT_TRUE(exec_context.toExecutionResult().is_success) << exec_context.getExceptionMsg();
        EXPECT_FALSE(exec_context.isCancelled());
        EXPECT_FALSE(output);
        EXPECT_EQ(source_profile->connection_profile_infos[0].bytes, 0);

        // An early suffix without statistics must not consume the pool's shared once_flag.
        recordRemoteConnectionInfo(pool);
        ASSERT_NO_THROW(remaining_source->operateSuffix());
        EXPECT_EQ(remaining_source->getIOProfileInfo()->connection_profile_infos[0].bytes, 123);
    }
}

TEST_F(SegmentReadTasksPoolTest, StoppedRemoteSourceCollectsAvailableConnectionInfo)
{
    PipelineExecutorContext exec_context;
    auto pool = createSegmentReadTaskPool({1});
    auto scheduler = createPausedScheduler();
    std::call_once(pool->addToSchedulerFlag(), [&] { scheduler.add(pool); });
    auto source = createRemoteSource(exec_context, pool);
    source->operatePrefix();
    recordRemoteConnectionInfo(pool);
    source->onEarlyStop();
    ASSERT_NO_THROW(source->operateSuffix());
    EXPECT_EQ(source->getIOProfileInfo()->connection_profile_infos[0].bytes, 123);
}

TEST_F(SegmentReadTasksPoolTest, RemoteSourceRequiresConnectionInfoWithoutInputStop)
{
    PipelineExecutorContext exec_context;
    auto pool = createSegmentReadTaskPool({1});
    auto scheduler = createPausedScheduler();
    std::call_once(pool->addToSchedulerFlag(), [&] { scheduler.add(pool); });
    auto source = createRemoteSource(exec_context, pool);
    source->operatePrefix();
    EXPECT_THROW(source->operateSuffix(), DB::Exception);
}

} // namespace DB::DM::tests
