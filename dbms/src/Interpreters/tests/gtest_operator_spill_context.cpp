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
#include <Core/FineGrainedOperatorSpillContext.h>
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
    std::vector<Int64> max_cached_bytes
        = {0, OperatorSpillContext::MIN_SPILL_THRESHOLD + 1, OperatorSpillContext::MIN_SPILL_THRESHOLD - 1};
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

TEST_F(TestOperatorSpillContext, AutoTriggerSpillOnEmptyData)
try
{
    auto agg_spill_context = std::make_shared<AggSpillContext>(2, *spill_config_ptr, 0, logger);
    agg_spill_context->setAutoSpillMode();
    agg_spill_context->updatePerThreadRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD, 0);
    agg_spill_context->updatePerThreadRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD, 1);
    ASSERT_TRUE(agg_spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD) == 0);
    ASSERT_TRUE(agg_spill_context->isThreadMarkedForAutoSpill(0));
    ASSERT_FALSE(agg_spill_context->isThreadMarkedForAutoSpill(1));

    auto sort_spill_context = std::make_shared<SortSpillContext>(*spill_config_ptr, 0, logger);
    sort_spill_context->setAutoSpillMode();
    ASSERT_FALSE(sort_spill_context->updateRevocableMemory(0));
    ASSERT_TRUE(
        sort_spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD);

    auto join_spill_context = std::make_shared<HashJoinSpillContext>(*spill_config_ptr, *spill_config_ptr, 0, logger);
    join_spill_context->setAutoSpillMode();
    join_spill_context->init(2);
    ASSERT_TRUE(
        join_spill_context->updatePartitionRevocableMemory(0, OperatorSpillContext::MIN_SPILL_THRESHOLD) == false);
    ASSERT_TRUE(join_spill_context->updatePartitionRevocableMemory(1, 0) == false);
    ASSERT_TRUE(
        join_spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD * 2)
        == OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(join_spill_context->isPartitionMarkedForAutoSpill(0));
    ASSERT_FALSE(join_spill_context->isPartitionMarkedForAutoSpill(1));
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

TEST_F(TestOperatorSpillContext, FineGrainedOperatorSpillContext)
try
{
    auto fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("test", logger);
    /// fine grained spill context should always support auto spill and spill
    ASSERT_TRUE(fine_grained_spill_context->supportAutoTriggerSpill() && fine_grained_spill_context->supportSpill());

    /// add to fine grained spill context always makes the operator spill context in auto spill mode
    auto spill_context_1 = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    ASSERT_FALSE(spill_context_1->isInAutoSpillMode());
    fine_grained_spill_context->addOperatorSpillContext(spill_context_1);
    ASSERT_TRUE(fine_grained_spill_context->getOperatorSpillCount() == 1);
    ASSERT_TRUE(spill_context_1->isInAutoSpillMode());

    /// operator spill context that not enable spill can not add to fine grained spill context
    auto spill_context_2 = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    spill_context_2->disableSpill();
    fine_grained_spill_context->addOperatorSpillContext(spill_context_2);
    ASSERT_TRUE(fine_grained_spill_context->getOperatorSpillCount() == 1);

    /// fine grained spill context support multiple operator spill context
    auto spill_context_3 = std::make_shared<SortSpillContext>(*spill_config_ptr, 1000, logger);
    fine_grained_spill_context->addOperatorSpillContext(spill_context_3);
    ASSERT_TRUE(fine_grained_spill_context->getOperatorSpillCount() == 2);

    /// test auto spill trigger if all operator spill contexts inside fine grained spill context have at least OperatorSpillContext::MIN_SPILL_THRESHOLD
    spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(fine_grained_spill_context->getTotalRevocableMemory() == 2 * OperatorSpillContext::MIN_SPILL_THRESHOLD);
    fine_grained_spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(spill_context_1->needFinalSpill());
    ASSERT_TRUE(spill_context_3->needFinalSpill());
    spill_context_1->finishOneSpill();
    spill_context_3->finishOneSpill();

    /// test auto spill trigger if only part of operators spill context inside fine grained spill context have at least OperatorSpillContext::MIN_SPILL_THRESHOLD
    spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD - 1);
    ASSERT_TRUE(
        fine_grained_spill_context->getTotalRevocableMemory() == 2 * OperatorSpillContext::MIN_SPILL_THRESHOLD - 1);
    fine_grained_spill_context->triggerSpill(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(spill_context_1->needFinalSpill());
    ASSERT_FALSE(spill_context_3->needFinalSpill());
    spill_context_1->finishOneSpill();

    /// test the case that some operator spill context already finish spillable stage
    spill_context_1->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    spill_context_3->updateRevocableMemory(OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(fine_grained_spill_context->getTotalRevocableMemory() == 2 * OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(fine_grained_spill_context->supportFurtherSpill());
    spill_context_3->finishSpillableStage();
    ASSERT_TRUE(fine_grained_spill_context->getTotalRevocableMemory() == OperatorSpillContext::MIN_SPILL_THRESHOLD);
    ASSERT_TRUE(fine_grained_spill_context->supportFurtherSpill());
    spill_context_1->finishSpillableStage();
    ASSERT_TRUE(fine_grained_spill_context->getTotalRevocableMemory() == 0);
    ASSERT_FALSE(fine_grained_spill_context->supportFurtherSpill());
}
CATCH
} // namespace tests
} // namespace DB
