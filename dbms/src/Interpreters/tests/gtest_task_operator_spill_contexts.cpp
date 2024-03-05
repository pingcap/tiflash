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
#include <Core/TaskOperatorSpillContexts.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/AggSpillContext.h>
#include <Interpreters/HashJoinSpillContext.h>
#include <Interpreters/SortSpillContext.h>
#include <Poco/File.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
class TestTaskOperatorSpillContexts : public ::testing::Test
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

String TestTaskOperatorSpillContexts::spill_dir
    = DB::tests::TiFlashTestEnv::getTemporaryPath("operator_spill_context_test");

TEST_F(TestTaskOperatorSpillContexts, TestRegisterOperatorSpillContext)
try
{
    /// currently only sort_spill_context support auto spill
    auto agg_spill_context = std::make_shared<AggSpillContext>(1, *spill_config_ptr, 1000, logger);
    auto sort_spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    auto join_spill_context
        = std::make_shared<HashJoinSpillContext>(*spill_config_ptr, *spill_config_ptr, 1000, logger);
    join_spill_context->init(10);
    TaskOperatorSpillContexts task_operator_spill_contexts;
    ASSERT_TRUE(task_operator_spill_contexts.operatorSpillContextCount() == 0);
    task_operator_spill_contexts.registerOperatorSpillContext(agg_spill_context);
    ASSERT_TRUE(task_operator_spill_contexts.operatorSpillContextCount() == 1);
    task_operator_spill_contexts.registerOperatorSpillContext(sort_spill_context);
    /// register will first add spill context to additional_operator_spill_contexts
    ASSERT_TRUE(task_operator_spill_contexts.additionalOperatorSpillContextCount() == 1);
    ASSERT_TRUE(task_operator_spill_contexts.operatorSpillContextCount() == 2);
    /// additional_operator_spill_contexts has been merged to operator_spill_contexts
    ASSERT_TRUE(task_operator_spill_contexts.additionalOperatorSpillContextCount() == 0);
    task_operator_spill_contexts.registerOperatorSpillContext(join_spill_context);
    ASSERT_TRUE(task_operator_spill_contexts.operatorSpillContextCount() == 3);
    task_operator_spill_contexts.registerOperatorSpillContext(sort_spill_context);
    ASSERT_TRUE(task_operator_spill_contexts.operatorSpillContextCount() == 4);
}
CATCH

TEST_F(TestTaskOperatorSpillContexts, TestSpillAutoTrigger)
try
{
    auto sort_spill_context_1 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    auto sort_spill_context_2 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    TaskOperatorSpillContexts task_operator_spill_contexts;
    task_operator_spill_contexts.registerOperatorSpillContext(sort_spill_context_1);
    task_operator_spill_contexts.registerOperatorSpillContext(sort_spill_context_2);

    /// memory usage under OperatorSpillContext::MIN_SPILL_THRESHOLD will not trigger spill
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD - 1);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD - 1);
    ASSERT_TRUE(
        task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD / 2)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD / 2);
    auto spill_1 = sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    auto spill_2 = sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    ASSERT_TRUE(!spill_1 && !spill_2);

    /// only one spill_context will trigger spill
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD / 2) <= 0);
    spill_1 = sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    spill_2 = sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    ASSERT_TRUE(spill_1 ^ spill_2);
    if (spill_1)
        sort_spill_context_1->finishOneSpill();
    if (spill_2)
        sort_spill_context_2->finishOneSpill();

    /// two spill_context will be triggered spill
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 1.5) <= 0);
    spill_1 = sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    spill_2 = sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    ASSERT_TRUE(spill_1 && spill_2);
    sort_spill_context_1->finishOneSpill();
    sort_spill_context_2->finishOneSpill();

    /// revocable memories less than expected released memory
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(
        task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2.5)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 0.5);
    spill_1 = sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    spill_2 = sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1);
    ASSERT_TRUE(spill_1 && spill_2);
    sort_spill_context_1->finishOneSpill();
    sort_spill_context_2->finishOneSpill();

    /// one spill_context not in spilled stage
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_1->finishSpillableStage();
    ASSERT_TRUE(
        task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2.5)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 1.5);
    ASSERT_FALSE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    sort_spill_context_1->finishOneSpill();

    /// all spill_context not in spilled stage
    sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    sort_spill_context_1->finishSpillableStage();
    sort_spill_context_2->finishSpillableStage();
    ASSERT_TRUE(
        task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2.5)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 2.5);
    ASSERT_FALSE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_FALSE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));

    /// add new spill_context at runtime
    auto sort_spill_context_3 = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    task_operator_spill_contexts.registerOperatorSpillContext(sort_spill_context_3);
    sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(
        task_operator_spill_contexts.triggerAutoSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2.5)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD * 1.5);
    ASSERT_FALSE(sort_spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_FALSE(sort_spill_context_2->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
    ASSERT_TRUE(sort_spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD + 1));
}
CATCH
} // namespace tests
} // namespace DB