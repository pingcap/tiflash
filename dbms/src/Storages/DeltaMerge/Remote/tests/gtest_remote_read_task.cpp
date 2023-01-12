#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Transaction/Types.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/types.h>

#include <magic_enum.hpp>

namespace DB::DM::tests
{
using namespace DB::tests;

class RemoteReadTaskTest : public ::testing::Test
{
public:
    void SetUp() override
    {
    }

    static RemoteTableReadTaskPtr buildTableTestTask(
        UInt64 store_id,
        TableID table_id,
        const std::vector<UInt64> & seg_ids)
    {
        DisaggregatedTaskId snapshot_id;
        String address;
        auto store = std::make_shared<RemoteTableReadTask>(store_id, table_id, snapshot_id, address);
        for (const auto & seg_id : seg_ids)
        {
            auto seg_task = std::make_shared<RemoteSegmentReadTask>(snapshot_id, store_id, table_id, seg_id, address);
            store->tasks.emplace_back(std::move(seg_task));
        }
        return store;
    }

    static RemoteReadTaskPtr buildTestTask()
    {
        std::vector<RemoteTableReadTaskPtr> table_tasks{
            buildTableTestTask(111, TEST_TABLE_ID, {2, 5, 100}),
            buildTableTestTask(222, TEST_TABLE_ID, {202, 205, 300}),
            buildTableTestTask(333, TEST_TABLE_ID, {400, 401, 402, 403}),
        };
        return std::make_shared<RemoteReadTask>(std::move(table_tasks));
    }

protected:
    static constexpr TableID TEST_TABLE_ID = 100;
};

TEST_F(RemoteReadTaskTest, popTasksWithoutPreparation)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();

        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        auto ready_seg_task = read_task->nextReadyTask();
        ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
        ASSERT_EQ(seg_task->table_id, ready_seg_task->table_id);
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RemoteReadTaskTest, popPrepareTasks)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();

        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        auto prepare_seg_task = read_task->nextTaskForPrepare();
        ASSERT_EQ(prepare_seg_task->state, SegmentReadTaskState::DataReadyAndPrepraring) << magic_enum::enum_name(prepare_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, prepare_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, prepare_seg_task->store_id);
        ASSERT_EQ(seg_task->table_id, prepare_seg_task->table_id);
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

TEST_F(RemoteReadTaskTest, popTasksWithAllPrepared)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 3 + 4);

    for (size_t i = 0; i < num_segments; ++i)
    {
        auto seg_task = read_task->nextFetchTask();
        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false); // mock fetch done
        {
            auto prepare_seg_task = read_task->nextTaskForPrepare();
            ASSERT_EQ(prepare_seg_task->state, SegmentReadTaskState::DataReadyAndPrepraring) << magic_enum::enum_name(prepare_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, prepare_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, prepare_seg_task->store_id);
            ASSERT_EQ(seg_task->table_id, prepare_seg_task->table_id);
            read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReadyAndPrepared, false); // mock prepare done
        }
        {
            auto ready_seg_task = read_task->nextReadyTask();
            ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReadyAndPrepared) << magic_enum::enum_name(ready_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
            ASSERT_EQ(seg_task->table_id, ready_seg_task->table_id);
        }
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RemoteReadTaskTest, popTasksWithSomePrepared)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 3 + 4);

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
            ASSERT_EQ(seg_task->table_id, prepare_seg_task->table_id);
            read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReadyAndPrepared, false); // mock prepare done
        }
        {
            auto ready_seg_task = read_task->nextReadyTask();
            ASSERT_EQ(ready_seg_task->state, do_prepare ? SegmentReadTaskState::DataReadyAndPrepared : SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
            ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
            ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
            ASSERT_EQ(seg_task->table_id, ready_seg_task->table_id);
        }
    }

    ASSERT_EQ(read_task->nextFetchTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
}

TEST_F(RemoteReadTaskTest, failTask)
{
    auto read_task = buildTestTask();
    const auto num_segments = read_task->numSegments();
    ASSERT_EQ(num_segments, 3 + 3 + 4);

    assert(num_segments >= 4);
    for (size_t i = 0; i < 3; ++i)
    {
        auto seg_task = read_task->nextFetchTask();
        read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, false);
        auto ready_seg_task = read_task->nextReadyTask();
        ASSERT_EQ(ready_seg_task->state, SegmentReadTaskState::DataReady) << magic_enum::enum_name(ready_seg_task->state);
        ASSERT_EQ(seg_task->segment_id, ready_seg_task->segment_id);
        ASSERT_EQ(seg_task->store_id, ready_seg_task->store_id);
        ASSERT_EQ(seg_task->table_id, ready_seg_task->table_id);
    }
    auto seg_task = read_task->nextFetchTask();
    read_task->updateTaskState(seg_task, SegmentReadTaskState::DataReady, /*meet_error*/ true);
    ASSERT_EQ(read_task->nextReadyTask(), nullptr);
    ASSERT_EQ(read_task->nextTaskForPrepare(), nullptr);
}

TEST_F(RemoteReadTaskTest, ser)
{
    TableID table_id = 111;
    auto table_task = buildTableTestTask(50, table_id, {2, 5, 11});
}

} // namespace DB::DM::tests
