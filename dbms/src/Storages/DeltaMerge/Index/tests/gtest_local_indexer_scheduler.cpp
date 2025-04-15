// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

namespace DB::DM::tests
{

class LocalIndexerSchedulerTest : public ::testing::Test
{
protected:
    void pushResult(String result)
    {
        std::unique_lock lock(results_mu);
        results.push_back(result);
    }

    std::mutex results_mu;
    std::vector<String> results;
};

TEST_F(LocalIndexerSchedulerTest, StartScheduler)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 5,
        .auto_start = false,
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {},
        .request_memory = 0,
        .workload = [this]() { pushResult("foo"); },
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_EQ(results.size(), 0);

    scheduler.reset();
    ASSERT_EQ(results.size(), 0);

    scheduler = LocalIndexerScheduler::create({
        .pool_size = 5,
        .auto_start = false,
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {},
        .request_memory = 0,
        .workload = [this]() { pushResult("bar"); },
    });

    scheduler->start();
    scheduler->waitForFinish();

    ASSERT_EQ(1, results.size());
    ASSERT_STREQ("bar", results[0].c_str());
}
CATCH

TEST_F(LocalIndexerSchedulerTest, KeyspaceFair)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 1,
        .auto_start = false,
    });

    scheduler->pushTask({
        .keyspace_id = 2,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks2_t1"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 2,
        .file_ids = {LocalIndexerScheduler::DMFileID(2)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t2"); },
    });
    scheduler->pushTask({
        .keyspace_id = 3,
        .table_id = 3,
        .file_ids = {LocalIndexerScheduler::DMFileID(3)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks3_t3"); },
    });
    scheduler->pushTask({
        .keyspace_id = 2,
        .table_id = 4,
        .file_ids = {LocalIndexerScheduler::DMFileID(4)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks2_t4"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(5)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t1"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 3,
        .file_ids = {LocalIndexerScheduler::DMFileID(6)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t3"); },
    });

    scheduler->start();
    scheduler->waitForFinish();

    // Scheduler is scheduled by KeyspaceID asc order and TableID asc order.
    ASSERT_EQ(results.size(), 6);
    ASSERT_STREQ(results[0].c_str(), "ks1_t1");
    ASSERT_STREQ(results[1].c_str(), "ks2_t1");
    ASSERT_STREQ(results[2].c_str(), "ks3_t3");
    ASSERT_STREQ(results[3].c_str(), "ks1_t2");
    ASSERT_STREQ(results[4].c_str(), "ks2_t4");
    ASSERT_STREQ(results[5].c_str(), "ks1_t3");

    results.clear();

    scheduler->pushTask({
        .keyspace_id = 2,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks2_t1"); },
    });

    scheduler->waitForFinish();

    ASSERT_EQ(results.size(), 1);
    ASSERT_STREQ(results[0].c_str(), "ks2_t1");
}
CATCH

TEST_F(LocalIndexerSchedulerTest, TableFair)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 1,
        .auto_start = false,
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 3,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t3_#1"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(2)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t1_#1"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 3,
        .file_ids = {LocalIndexerScheduler::DMFileID(3)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t3_#2"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 2,
        .file_ids = {LocalIndexerScheduler::DMFileID(4)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks1_t2_#1"); },
    });
    scheduler->pushTask({
        .keyspace_id = 2,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(5)},
        .request_memory = 0,
        .workload = [&]() { pushResult("ks2_t1_#1"); },
    });

    scheduler->start();
    scheduler->waitForFinish();

    // Scheduler is scheduled by KeyspaceID asc order and TableID asc order.
    ASSERT_EQ(results.size(), 5);
    ASSERT_STREQ(results[0].c_str(), "ks1_t1_#1");
    ASSERT_STREQ(results[1].c_str(), "ks2_t1_#1");
    ASSERT_STREQ(results[2].c_str(), "ks1_t2_#1");
    ASSERT_STREQ(results[3].c_str(), "ks1_t3_#1");
    ASSERT_STREQ(results[4].c_str(), "ks1_t3_#2");
}
CATCH

TEST_F(LocalIndexerSchedulerTest, TaskExceedMemoryLimit)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 10,
        .memory_limit = 2,
        .auto_start = false,
    });

    {
        auto [ok, reason] = scheduler->pushTask({
            .keyspace_id = 1,
            .table_id = 1,
            .file_ids = {LocalIndexerScheduler::DMFileID(1)},
            .request_memory = 100, // exceed memory limit
            .workload = [&]() { pushResult("foo"); },
        });
        ASSERT_FALSE(ok);
    }
    {
        auto [ok, reason] = scheduler->pushTask({
            .keyspace_id = 1,
            .table_id = 1,
            .file_ids = {LocalIndexerScheduler::DMFileID(2)},
            .request_memory = 0,
            .workload = [&]() { pushResult("bar"); },
        });
        ASSERT_TRUE(ok);
    }

    scheduler->start();
    scheduler->waitForFinish();

    ASSERT_EQ(results.size(), 1);
    ASSERT_STREQ(results[0].c_str(), "bar");

    results.clear();

    scheduler = LocalIndexerScheduler::create({
        .pool_size = 10,
        .memory_limit = 0,
    });

    {
        auto [ok, reason] = scheduler->pushTask({
            .keyspace_id = 1,
            .table_id = 1,
            .file_ids = {LocalIndexerScheduler::DMFileID(3)},
            .request_memory = 100,
            .workload = [&]() { pushResult("foo"); },
        });
        ASSERT_TRUE(ok);
    }
    {
        auto [ok, reason] = scheduler->pushTask({
            .keyspace_id = 1,
            .table_id = 1,
            .file_ids = {LocalIndexerScheduler::DMFileID(4)},
            .request_memory = 0,
            .workload = [&]() { pushResult("bar"); },
        });
        ASSERT_TRUE(ok);
    };

    scheduler->start();
    scheduler->waitForFinish();

    ASSERT_EQ(results.size(), 2);
    ASSERT_STREQ(results[0].c_str(), "foo");
    ASSERT_STREQ(results[1].c_str(), "bar");
}
CATCH

TEST_F(LocalIndexerSchedulerTest, MemoryLimit)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 10,
        .memory_limit = 2,
        .auto_start = false,
    });

    auto task_1_is_started = std::make_shared<std::promise<void>>();
    auto task_2_is_started = std::make_shared<std::promise<void>>();
    auto task_3_is_started = std::make_shared<std::promise<void>>();

    auto task_1_wait = std::make_shared<std::promise<void>>();
    auto task_2_wait = std::make_shared<std::promise<void>>();
    auto task_3_wait = std::make_shared<std::promise<void>>();

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 1,
        .workload =
            [=]() {
                task_1_is_started->set_value();
                task_1_wait->get_future().wait();
            },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(2)},
        .request_memory = 1,
        .workload =
            [=]() {
                task_2_is_started->set_value();
                task_2_wait->get_future().wait();
            },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(3)},
        .request_memory = 1,
        .workload =
            [=]() {
                task_3_is_started->set_value();
                task_3_wait->get_future().wait();
            },
    });

    scheduler->start();

    task_1_is_started->get_future().wait();
    task_2_is_started->get_future().wait();

    auto task_3_is_started_future = task_3_is_started->get_future();

    // We should fail to got task 3 start running, because current memory limit is reached
    ASSERT_EQ(task_3_is_started_future.wait_for(std::chrono::milliseconds(500)), std::future_status::timeout);

    task_1_wait->set_value();

    task_3_is_started_future.wait();

    task_2_wait->set_value();
    task_3_wait->set_value();
}
CATCH

TEST_F(LocalIndexerSchedulerTest, ShutdownWithPendingTasks)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 1,
        .auto_start = false,
    });

    auto task_1_is_started = std::make_shared<std::promise<void>>();
    auto task_1_wait = std::make_shared<std::promise<void>>();

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload =
            [=]() {
                task_1_is_started->set_value();
                task_1_wait->get_future().wait();
            },
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload =
            [=]() {
                // Should not enter here.
                ASSERT_TRUE(false);
            },
    });

    scheduler->start();

    // Ensure task 1 is running
    task_1_is_started->get_future().wait();

    // Shutdown the scheduler.
    auto shutdown_th = std::async([&]() { scheduler.reset(); });

    // The shutdown should be waiting for task 1 to finish
    ASSERT_EQ(shutdown_th.wait_for(std::chrono::milliseconds(500)), std::future_status::timeout);

    // After task 1 finished, the scheduler shutdown should be ok.
    task_1_wait->set_value();
    shutdown_th.wait();
}
CATCH

TEST_F(LocalIndexerSchedulerTest, WorkloadException)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 1,
        .auto_start = false,
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload = [&]() { throw DB::Exception("foo"); },
    });
    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(2)},
        .request_memory = 0,
        .workload = [&]() { pushResult("bar"); },
    });

    scheduler->start();
    scheduler->waitForFinish();

    ASSERT_EQ(results.size(), 1);
    ASSERT_STREQ(results[0].c_str(), "bar");
}
CATCH

TEST_F(LocalIndexerSchedulerTest, FileIsUsing)
try
{
    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 4,
        .auto_start = false,
    });

    auto task_1_is_started = std::make_shared<std::promise<void>>();
    auto task_2_is_started = std::make_shared<std::promise<void>>();
    auto task_3_is_started = std::make_shared<std::promise<void>>();

    auto task_1_wait = std::make_shared<std::promise<void>>();

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload =
            [&]() {
                task_1_is_started->set_value();
                task_1_wait->get_future().wait();
            },
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1), LocalIndexerScheduler::DMFileID(2)},
        .request_memory = 0,
        .workload = [&]() { task_2_is_started->set_value(); },
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(3)},
        .request_memory = 0,
        .workload = [&]() { task_3_is_started->set_value(); },
    });

    scheduler->start();

    task_1_is_started->get_future().wait();

    auto task_2_is_started_future = task_2_is_started->get_future();
    // We should fail to got task 2 start running, because current dmfile is using
    ASSERT_EQ(task_2_is_started_future.wait_for(std::chrono::milliseconds(500)), std::future_status::timeout);
    // Task 3 is not using the dmfile, so it should run
    task_3_is_started->get_future().wait();

    // After task 1 is finished, task 2 should run
    task_1_wait->set_value();
    task_2_is_started_future.wait();

    scheduler->waitForFinish();
}
CATCH

TEST_F(LocalIndexerSchedulerTest, DifferentTypeFile)
try
{
    // When files are different type, should not block

    auto scheduler = LocalIndexerScheduler::create({
        .pool_size = 4,
        .auto_start = false,
    });

    auto task_1_is_started = std::make_shared<std::promise<void>>();
    auto task_2_is_started = std::make_shared<std::promise<void>>();

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::DMFileID(1)},
        .request_memory = 0,
        .workload = [&]() { task_1_is_started->set_value(); },
    });

    scheduler->pushTask({
        .keyspace_id = 1,
        .table_id = 1,
        .file_ids = {LocalIndexerScheduler::ColumnFileTinyID(1)},
        .request_memory = 0,
        .workload = [&]() { task_2_is_started->set_value(); },
    });

    scheduler->start();

    task_1_is_started->get_future().wait();
    task_2_is_started->get_future().wait();

    scheduler->waitForFinish();
}
CATCH

} // namespace DB::DM::tests
