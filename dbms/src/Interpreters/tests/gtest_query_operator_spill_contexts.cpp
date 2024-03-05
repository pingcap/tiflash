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
#include <Core/QueryOperatorSpillContexts.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/AggSpillContext.h>
#include <Interpreters/SortSpillContext.h>
#include <Poco/File.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace DB
{
namespace tests
{
class TestQueryOperatorSpillContexts : public ::testing::Test
{
protected:
    void SetUp() override
    {
        logger = Logger::get("operator_spill_context_test");
        Poco::File spiller_dir(spill_dir);
        auto key_manager = std::make_shared<MockKeyManager>(false);
        auto file_provider = std::make_shared<FileProvider>(key_manager, false);
        spill_config_ptr = std::make_shared<SpillConfig>(spill_dir, "test", 1024ULL * 1024 * 1024, 0, 0, file_provider);
    }
    void TearDown() override
    {
        Poco::File spiller_dir(spill_dir);
        /// remove spiller dir if exists
        if (spiller_dir.exists())
            spiller_dir.remove(true);
    }
    static String spill_dir;
    std::shared_ptr<SpillConfig> spill_config_ptr;
    LoggerPtr logger;
};

String TestQueryOperatorSpillContexts::spill_dir
    = DB::tests::TiFlashTestEnv::getTemporaryPath("operator_spill_context_test");

TEST_F(TestQueryOperatorSpillContexts, TestRegisterTaskOperatorSpillContext)
try
{
    /// currently only sort_spill_context support auto spill
    auto sort_spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    std::shared_ptr<TaskOperatorSpillContexts> task_operator_spill_contexts
        = std::make_shared<TaskOperatorSpillContexts>();
    task_operator_spill_contexts->registerOperatorSpillContext(sort_spill_context);
    QueryOperatorSpillContexts query_operator_spill_contexts(
        MPPQueryId(0, 0, 0, 0, /*resource_group_name=*/"", 0, ""),
        0);
    ASSERT_TRUE(query_operator_spill_contexts.getTaskOperatorSpillContextsCount() == 0);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts);
    ASSERT_TRUE(query_operator_spill_contexts.getTaskOperatorSpillContextsCount() == 1);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts);
    ASSERT_TRUE(query_operator_spill_contexts.getTaskOperatorSpillContextsCount() == 2);
}
CATCH

TEST_F(TestQueryOperatorSpillContexts, TestTriggerSpill)
try
{
    auto sort_spill_context_1 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    auto sort_spill_context_2 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    auto sort_spill_context_3 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    std::shared_ptr<TaskOperatorSpillContexts> task_operator_spill_contexts_1
        = std::make_shared<TaskOperatorSpillContexts>();
    std::shared_ptr<TaskOperatorSpillContexts> task_operator_spill_contexts_2
        = std::make_shared<TaskOperatorSpillContexts>();
    task_operator_spill_contexts_1->registerOperatorSpillContext(sort_spill_context_1);
    task_operator_spill_contexts_2->registerOperatorSpillContext(sort_spill_context_2);
    task_operator_spill_contexts_2->registerOperatorSpillContext(sort_spill_context_3);

    QueryOperatorSpillContexts query_operator_spill_contexts(
        MPPQueryId(0, 0, 0, 0, /*resource_group_name=*/"", 0, ""),
        0);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts_1);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts_2);

    /// trigger spill for all task_operator_spill_contexts
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3) == 0);
    ASSERT_TRUE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    sort_spill_context_1->finishOneSpill();
    sort_spill_context_2->finishOneSpill();
    sort_spill_context_3->finishOneSpill();

    /// trigger spill only for task_operator_spill_contexts that uses more revocable memory usage
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    ASSERT_TRUE(query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 4) == 0);
    ASSERT_FALSE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    sort_spill_context_1->finishOneSpill();
    sort_spill_context_2->finishOneSpill();
    sort_spill_context_3->finishOneSpill();

    /// auto clean finished task_operator_spill_contexts
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    task_operator_spill_contexts_2->finish();
    ASSERT_TRUE(
        query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 4)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_FALSE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_FALSE(sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(query_operator_spill_contexts.getTaskOperatorSpillContextsCount() == 1);
}
CATCH

TEST_F(TestQueryOperatorSpillContexts, TestMinCheckInterval)
try
{
    auto sort_spill_context_1 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    auto sort_spill_context_2 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    auto sort_spill_context_3 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    std::shared_ptr<TaskOperatorSpillContexts> task_operator_spill_contexts_1
        = std::make_shared<TaskOperatorSpillContexts>();
    std::shared_ptr<TaskOperatorSpillContexts> task_operator_spill_contexts_2
        = std::make_shared<TaskOperatorSpillContexts>();
    task_operator_spill_contexts_1->registerOperatorSpillContext(sort_spill_context_1);
    task_operator_spill_contexts_2->registerOperatorSpillContext(sort_spill_context_2);
    task_operator_spill_contexts_2->registerOperatorSpillContext(sort_spill_context_3);

    UInt64 min_check_interval = 1000;
    QueryOperatorSpillContexts query_operator_spill_contexts(
        MPPQueryId(0, 0, 0, 0, /*resource_group_name=*/"", 0, ""),
        min_check_interval);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts_1);
    query_operator_spill_contexts.registerTaskOperatorSpillContexts(task_operator_spill_contexts_2);

    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2);
    ASSERT_TRUE(query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 4) == 0);
    ASSERT_FALSE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3));
    ASSERT_TRUE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2));
    ASSERT_TRUE(sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2));
    /// check interval less than threshold, no spill is triggered
    ASSERT_TRUE(
        query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 3);
    ASSERT_TRUE(
        query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3, true) == 0);
    ASSERT_TRUE(
        query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(2 * min_check_interval));
    ASSERT_TRUE(query_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 3) == 0);
    sort_spill_context_1->finishOneSpill();
    sort_spill_context_2->finishOneSpill();
    sort_spill_context_3->finishOneSpill();
}
CATCH
} // namespace tests
} // namespace DB
