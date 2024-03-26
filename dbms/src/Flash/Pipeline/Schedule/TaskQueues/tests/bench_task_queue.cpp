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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

#include <random>

namespace DB
{
namespace tests
{

#define DEFINE_SIMPLE_TASK(TASK_NAME, TASK_RETURN_STATUS)                                     \
    class TASK_NAME : public Task                                                             \
    {                                                                                         \
    public:                                                                                   \
        explicit TASK_NAME(PipelineExecutorContext & exec_context)                            \
            : Task(exec_context)                                                              \
        {}                                                                                    \
                                                                                              \
        ReturnStatus executeImpl() override                                                   \
        {                                                                                     \
            if (task_exec_cur_count <= task_exec_total_count)                                 \
            {                                                                                 \
                std::this_thread::sleep_for(std::chrono::milliseconds(task_exec_time_in_ms)); \
                ++task_exec_cur_count;                                                        \
                return TASK_RETURN_STATUS;                                                    \
            }                                                                                 \
            return ExecTaskStatus::FINISHED;                                                  \
        }                                                                                     \
                                                                                              \
        uint64_t task_exec_time_in_ms = 0;                                                    \
        uint64_t task_exec_cur_count = 0;                                                     \
        uint64_t task_exec_total_count = 0;                                                   \
    }

DEFINE_SIMPLE_TASK(SimpleCPUTask, ExecTaskStatus::RUNNING);
DEFINE_SIMPLE_TASK(SimpleIOTask, ExecTaskStatus::IO_IN);

#define TASKQUEUE_BENCHMARK(BENCHMARK_NAME, TASK_TYPE)                 \
    class BENCHMARK_NAME : public benchmark::Fixture                   \
    {                                                                  \
    };                                                                 \
                                                                       \
    BENCHMARK_DEFINE_F(BENCHMARK_NAME, Basic)                          \
    (benchmark::State & state)                                         \
    try                                                                \
    {                                                                  \
        const int pool_size = state.range(0);                          \
        const int min_task = state.range(1);                           \
        const int max_task = state.range(2);                           \
        const int task_num = state.range(3);                           \
        const int task_exec_count = state.range(4);                    \
                                                                       \
        std::random_device rand_dev;                                   \
        std::mt19937 gen(rand_dev());                                  \
        std::uniform_int_distribution dist(min_task, max_task);        \
                                                                       \
        for (auto _ : state)                                           \
        {                                                              \
            PipelineExecutorContext exec_context;                      \
                                                                       \
            std::vector<TaskPtr> tasks;                                \
            tasks.resize(task_num);                                    \
            for (int i = 0; i < task_num; ++i)                         \
            {                                                          \
                auto task = std::make_unique<TASK_TYPE>(exec_context); \
                task->task_exec_time_in_ms = dist(gen);                \
                task->task_exec_total_count = task_exec_count;         \
                tasks[i] = std::move(task);                            \
            }                                                          \
                                                                       \
            TaskSchedulerConfig config{pool_size, pool_size};          \
            TaskScheduler task_scheduler(config);                      \
                                                                       \
            task_scheduler.submit(tasks);                              \
                                                                       \
            exec_context.wait();                                       \
        }                                                              \
    }                                                                  \
    CATCH

TASKQUEUE_BENCHMARK(MLFQBench, SimpleCPUTask)
BENCHMARK_REGISTER_F(MLFQBench, Basic)
    ->Args({10, 1, 1, 10000, 2}) // 10000 * 1 * 2 / 10 = 2s
    ->Args({10, 15, 15, 1000, 2}) // 1000 * 15 * 2 / 10 = 3s
    ->Args({10, 200, 200, 1000, 2}); // 1000 * 200 * 2 / 10 = 40s

TASKQUEUE_BENCHMARK(IOPriorityBench, SimpleIOTask)
BENCHMARK_REGISTER_F(IOPriorityBench, Basic)
    ->Args({10, 1, 1, 10000, 2}) // 10000 * 1 * 2 / 10 = 2s
    ->Args({10, 15, 15, 1000, 2}) // 1000 * 15 * 2 / 10 = 3s
    ->Args({10, 200, 200, 1000, 2}); // 1000 * 200 * 2 / 10 = 40s

TASKQUEUE_BENCHMARK(ResourceContorlQueue, SimpleIOTask)
BENCHMARK_REGISTER_F(ResourceContorlQueue, Basic)
    ->Args({10, 1, 1, 10000, 2}) // 10000 * 1 * 2 / 10 = 2s
    ->Args({10, 15, 15, 1000, 2}) // 1000 * 15 * 2 / 10 = 3s
    ->Args({10, 200, 200, 1000, 2}); // 1000 * 200 * 2 / 10 = 40s
} // namespace tests
} // namespace DB
