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

#include <Flash/Pipeline/TaskBuilder.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class PipelineRunner : public ::testing::Test
{
protected:
    void SetUp() override
    {
        DynamicThreadPool::global_instance = std::make_unique<DynamicThreadPool>(
            /*fixed_thread_num=*/300,
            std::chrono::milliseconds(100000));
    }

    void TearDown() override
    {
        DynamicThreadPool::global_instance.reset();
    }
};

TEST_F(PipelineRunner, empty)
{
    std::vector<TaskPtr> tasks;
    TaskScheduler task_scheduler(std::thread::hardware_concurrency(), tasks);
    task_scheduler.waitForFinish();
}

TEST_F(PipelineRunner, all_cpu)
{
    auto build = []() {
        return TaskBuilder().setCPUSource().appendCPUTransform().setCPUSink().build();
    };
    std::vector<TaskPtr> tasks;
    tasks.emplace_back(build());
    TaskScheduler task_scheduler(std::thread::hardware_concurrency(), tasks);
    task_scheduler.waitForFinish();
}

TEST_F(PipelineRunner, all_io)
{
    auto build = []() {
        return TaskBuilder().setIOSource().appendIOTransform().setIOSink().build();
    };
    std::vector<TaskPtr> tasks;
    tasks.emplace_back(build());
    TaskScheduler task_scheduler(std::thread::hardware_concurrency(), tasks);
    task_scheduler.waitForFinish();
}

TEST_F(PipelineRunner, io_cpu)
{
    std::vector<TaskPtr> tasks;
    tasks.emplace_back(TaskBuilder().setCPUSource().appendCPUTransform().setIOSink().build());
    tasks.emplace_back(TaskBuilder().setCPUSource().appendIOTransform().setIOSink().build());
    tasks.emplace_back(TaskBuilder().setCPUSource().appendIOTransform().setCPUSink().build());
    tasks.emplace_back(TaskBuilder().setIOSource().appendIOTransform().setCPUSink().build());
    tasks.emplace_back(TaskBuilder().setIOSource().appendCPUTransform().setCPUSink().build());
    tasks.emplace_back(TaskBuilder().setIOSource().appendCPUTransform().setIOSink().build());
    TaskScheduler task_scheduler(std::thread::hardware_concurrency(), tasks);
    task_scheduler.waitForFinish();
}
} // namespace DB::tests
