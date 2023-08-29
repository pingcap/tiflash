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
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
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
class TestOperatorSpillContext : public ::testing::Test
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

String TestOperatorSpillContext::spill_dir = DB::tests::TiFlashTestEnv::getTemporaryPath("operator_spill_context_test");

TEST_F(TestOperatorSpillContext, AggMarkSpill)
try
{
    auto spill_context = std::make_shared<AggSpillContext>(1, *spill_config_ptr, 1000, logger);
    ASSERT_TRUE(spill_context->isSpilled() == false);
    spill_context->markSpilled();
    ASSERT_TRUE(spill_context->isSpilled() == true);
}
CATCH

TEST_F(TestOperatorSpillContext, AggTriggerSpill)
try
{
    std::vector<size_t> concurrency{1, 10};
    for (const auto & c : concurrency)
    {
        auto spill_context = std::make_shared<AggSpillContext>(c, *spill_config_ptr, 1000, logger);
        if (c == 1)
            ASSERT_TRUE(spill_context->updatePerThreadRevocableMemory(600, 0) == false);
        else
            ASSERT_TRUE(spill_context->updatePerThreadRevocableMemory(600, 0) == true);
        ASSERT_TRUE(spill_context->updatePerThreadRevocableMemory(1200, 0) == true);
    }
}
CATCH

TEST_F(TestOperatorSpillContext, AggFinishSpillableStage)
try
{
    auto spill_context = std::make_shared<AggSpillContext>(1, *spill_config_ptr, 1000, logger);
    spill_context->updatePerThreadRevocableMemory(100, 0);
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 100);
    spill_context->finishSpillableStage();
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 0);
    ASSERT_TRUE(spill_context->updatePerThreadRevocableMemory(2000, 0) == false);
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 0);
}
CATCH

TEST_F(TestOperatorSpillContext, AggAutoTriggerSpill)
try
{
    auto agg_spill_config = *spill_config_ptr;
    /// unlimited cache bytes
    std::vector<Int64> max_cached_bytes = {0, OperatorSpillContext::MIN_SPILL_THRESHOLD + 1, OperatorSpillContext::MIN_SPILL_THRESHOLD - 1};
    std::vector<size_t> expected_spill_threads = {5, 5, 10};
    size_t threads = 10;
    for (size_t index = 0; index < max_cached_bytes.size(); ++index)
    {
        agg_spill_config.max_cached_data_bytes_in_spiller = max_cached_bytes[index];
        auto spill_context = std::make_shared<AggSpillContext>(threads, agg_spill_config, 0, logger);
        spill_context->setAutoSpillMode();
        for (size_t i = 0; i < threads; ++i)
            spill_context->updatePerThreadRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD, i);
        ASSERT_TRUE(spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 5) == 0);
        size_t spilled_partition = 0;
        for (size_t i = 0; i < threads; ++i)
        {
            if (spill_context->isThreadMarkedForAutoSpill(i))
                spilled_partition++;
        }
        ASSERT_TRUE(spilled_partition == expected_spill_threads[index]);
    }
}
CATCH

TEST_F(TestOperatorSpillContext, SortMarkSpill)
try
{
    auto spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    ASSERT_TRUE(spill_context->isSpilled() == false);
    spill_context->markSpilled();
    ASSERT_TRUE(spill_context->isSpilled() == true);
}
CATCH

TEST_F(TestOperatorSpillContext, SortTriggerSpill)
try
{
    auto spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    ASSERT_TRUE(spill_context->updateRevocableMemory(600) == false);
    ASSERT_TRUE(spill_context->updateRevocableMemory(1200) == true);
}
CATCH

TEST_F(TestOperatorSpillContext, SortFinishSpillableStage)
try
{
    auto spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    spill_context->updateRevocableMemory(100);
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 100);
    spill_context->finishSpillableStage();
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 0);
    ASSERT_TRUE(spill_context->updateRevocableMemory(2000) == false);
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 0);
}
CATCH

TEST_F(TestOperatorSpillContext, JoinMarkSpill)
try
{
    auto spill_context = std::make_shared<HashJoinSpillContext>(*spill_config_ptr, *spill_config_ptr, 1000, logger);
    spill_context->init(10);
    ASSERT_FALSE(spill_context->isSpilled());
    spill_context->markPartitionSpilled(0);
    ASSERT_TRUE(spill_context->isSpilled());
}
CATCH

TEST_F(TestOperatorSpillContext, HashJoinTriggerSpill)
try
{
    auto spill_context = std::make_shared<HashJoinSpillContext>(*spill_config_ptr, *spill_config_ptr, 1000, logger);
    spill_context->init(2);
    ASSERT_TRUE(spill_context->updatePartitionRevocableMemory(0, 600) == false);
    ASSERT_TRUE(spill_context->updatePartitionRevocableMemory(1, 800) == false);
    auto spill_partitions = spill_context->getPartitionsToSpill();
    ASSERT_TRUE(spill_partitions.size() == 1);
    ASSERT_TRUE(spill_partitions[0] == 1);
}
CATCH

TEST_F(TestOperatorSpillContext, HashJoinFinishSpillableStage)
try
{
    auto spill_context = std::make_shared<HashJoinSpillContext>(*spill_config_ptr, *spill_config_ptr, 1000, logger);
    spill_context->init(2);
    ASSERT_TRUE(spill_context->updatePartitionRevocableMemory(0, 600) == false);
    ASSERT_TRUE(spill_context->updatePartitionRevocableMemory(1, 800) == false);
    spill_context->finishSpillableStage();
    ASSERT_TRUE(spill_context->getTotalRevocableMemory() == 0);
    auto spill_partitions = spill_context->getPartitionsToSpill();
    ASSERT_TRUE(spill_partitions.empty());
}
CATCH
} // namespace tests
} // namespace DB
