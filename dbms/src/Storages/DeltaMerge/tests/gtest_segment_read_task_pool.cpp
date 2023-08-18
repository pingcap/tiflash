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

#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>

namespace DB::DM::tests
{
SegmentPtr createSegment(PageIdU64 seg_id)
{
    return std::make_shared<Segment>(Logger::get(), 0, RowKeyRange{}, seg_id, seg_id + 1, nullptr, nullptr);
}

SegmentReadTaskPtr createSegmentReadTask(PageIdU64 seg_id)
{
    return std::make_shared<SegmentReadTask>(createSegment(seg_id), nullptr, RowKeyRanges{});
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

static const std::vector<PageIdU64> test_seg_ids{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

TEST(SegmentReadTasksWrapperTest, Unordered)
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
        ASSERT_EQ(task->segment->segmentId(), seg_id);
        task = tasks_wrapper.getTask(seg_id);
        ASSERT_EQ(task, nullptr);
    }
    ASSERT_TRUE(tasks_wrapper.empty());
}

TEST(SegmentReadTasksWrapperTest, Ordered)
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

} // namespace DB::DM::tests