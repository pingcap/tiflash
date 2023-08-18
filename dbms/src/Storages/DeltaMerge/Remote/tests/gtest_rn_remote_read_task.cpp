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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Transaction/Types.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/types.h>

#include <magic_enum.hpp>
#include <memory>

namespace DB::DM::tests
{
using namespace DB::tests;

class RNRemoteReadTaskTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        log = Logger::get();
    }

    RNRemotePhysicalTableReadTaskPtr buildTableTestTask(
        StoreID store_id,
        KeyspaceTableID ks_table_id,
        const std::vector<UInt64> & seg_ids)
    {
        DisaggTaskId snapshot_id;
        String address;
        auto physical_table_task = std::make_shared<RNRemotePhysicalTableReadTask>(store_id, ks_table_id, snapshot_id, address);
        for (const auto & seg_id : seg_ids)
        {
            auto seg_task = std::make_shared<RNRemoteSegmentReadTask>(snapshot_id, store_id, ks_table_id, seg_id, address, log);
            physical_table_task->tasks.emplace_back(std::move(seg_task));
        }
        return physical_table_task;
    }

    RNRemoteReadTaskPtr buildTestTask()
    {
        std::vector<RNRemoteStoreReadTaskPtr> store_tasks{
            std::make_shared<RNRemoteStoreReadTask>(
                111,
                std::vector<RNRemotePhysicalTableReadTaskPtr>{
                    // partition 0
                    buildTableTestTask(111, TEST_KS_TABLE_PART0_ID, {2, 5, 100}),
                    // partition 1
                    buildTableTestTask(111, TEST_KS_TABLE_PART1_ID, {1}),
                }),
            std::make_shared<RNRemoteStoreReadTask>(
                222,
                std::vector<RNRemotePhysicalTableReadTaskPtr>{
                    // partition 0
                    buildTableTestTask(222, TEST_KS_TABLE_PART0_ID, {202, 205, 300}),
                }),
            std::make_shared<RNRemoteStoreReadTask>(
                333,
                std::vector<RNRemotePhysicalTableReadTaskPtr>{
                    // partition 0
                    buildTableTestTask(333, TEST_KS_TABLE_PART0_ID, {400, 401, 402, 403}),
                }),
        };
        return std::make_shared<RNRemoteReadTask>(std::move(store_tasks));
    }

protected:
    static constexpr KeyspaceTableID TEST_KS_TABLE_PART0_ID = {NullspaceID, 100};
    static constexpr KeyspaceTableID TEST_KS_TABLE_PART1_ID = {NullspaceID, 101};
    LoggerPtr log;
};

TEST_F(RNRemoteReadTaskTest, popTasksWithoutPreparation)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 1 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();

        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        auto ready_seg_task = read_task->nextReadyTask();
        ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
        ASSERT_EQ(seg_task->ks_table_id, ready_seg_task->ks_table_id);
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RNRemoteReadTaskTest, popPrepareTasks)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 1 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();

        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        auto prepare_seg_task = read_task->nextTaskForPrepare();
        ASSERT_EQ(prepare_seg_task->state, SegmentReadTaskState::DataReadyAndPrepraring) << magic_enum::enum_name(prepare_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, prepare_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, prepare_seg_task->store_id);
        ASSERT_EQ(seg_task->ks_table_id, prepare_seg_task->ks_table_id);
        read_task->updateTaskState(prepare_seg_task, SegmentReadTaskState::DataReadyAndPrepared, false);
    }

    // there is no more task for prepare, return nullptr quickly
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
    ASSERT_EQ(read_task->nextFetchTask(), nullptr);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto ready_task = read_task->nextReadyTask();
        ASSERT_EQ(ready_task->state, SegmentReadTaskState::DataReadyAndPrepared) << magic_enum::enum_name(ready_task->state);
    }
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RNRemoteReadTaskTest, popTasksWithAllPrepared)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 1 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();
        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        {
            auto prepare_seg_task = read_task->nextTaskForPrepare();
            ASSERT_EQ(prepare_seg_task->state, SegmentReadTaskState::DataReadyAndPrepraring) << magic_enum::enum_name(prepare_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, prepare_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, prepare_seg_task->store_id);
            ASSERT_EQ(seg_task->ks_table_id, prepare_seg_task->ks_table_id);
            read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReadyAndPrepared, false); // mock prepare done
        }
        {
            auto ready_seg_task = read_task->nextReadyTask();
            ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReadyAndPrepared) << magic_enum::enum_name(ready_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
            ASSERT_EQ(seg_task->ks_table_id, ready_seg_task->ks_table_id);
        }
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RNRemoteReadTaskTest, popTasksWithSomePrepared)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 1 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();
        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        bool do_prepare = i % 2 == 0;
        if (do_prepare)
        {
            auto prepare_seg_task = read_task->nextTaskForPrepare();
            ASSERT_EQ(prepare_seg_task->state, SegmentReadTaskState::DataReadyAndPrepraring) << magic_enum::enum_name(prepare_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, prepare_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, prepare_seg_task->store_id);
            ASSERT_EQ(seg_task->ks_table_id, prepare_seg_task->ks_table_id);
            read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReadyAndPrepared, false); // mock prepare done
        }
        {
            auto ready_seg_task = read_task->nextReadyTask();
            ASSERT_EQ(ready_seg_task->state, do_prepare ? SegmentReadTaskState::DataReadyAndPrepared : SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
            ASSERT_EQ(seg_task->ks_table_id, ready_seg_task->ks_table_id);
        }
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RNRemoteReadTaskTest, failTask)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 1 + 3 + 4);

    assert(num_segments >= 4);
    for (size_t i = 0; i < 3; ++i)
    {
        auto seg_task = read_task->nextFetchTask();
        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false);
        auto ready_seg_task = read_task->nextReadyTask();
        ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
        ASSERT_EQ(seg_task->ks_table_id, ready_seg_task->ks_table_id);
    }
    // mock meet error for this segment task
    auto seg_task = read_task->nextFetchTask();
    read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, /*meet_error*/ true);
    // cancel all
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
}


} // namespace DB::DM::tests
