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

#include <Common/Logger.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Interpreters/AggSpillContext.h>
#include <Poco/File.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
class TestAggSpillContext : public ::testing::Test
{
protected:
    void SetUp() override
    {
        logger = Logger::get("agg_spill_context_test");
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

String TestAggSpillContext::spill_dir = DB::tests::TiFlashTestEnv::getTemporaryPath("agg_spill_context_test");

TEST_F(TestAggSpillContext, MarkSpill)
try
{
    auto agg_spill_context = std::make_shared<AggSpillContext>(1, *spill_config_ptr, 1000, logger);
    ASSERT_TRUE(agg_spill_context->isSpilled() == false);
    agg_spill_context->markSpill();
    ASSERT_TRUE(agg_spill_context->isSpilled() == true);
}
CATCH

TEST_F(TestAggSpillContext, TriggerSpill)
try
{
    std::vector<size_t> concurrency{1, 10};
    for (const auto & c : concurrency)
    {
        auto agg_spill_context = std::make_shared<AggSpillContext>(c, *spill_config_ptr, 1000, logger);
        if (c == 1)
            ASSERT_TRUE(agg_spill_context->updatePerThreadRevocableMemory(600, 0) == false);
        else
            ASSERT_TRUE(agg_spill_context->updatePerThreadRevocableMemory(600, 0) == true);
        ASSERT_TRUE(agg_spill_context->updatePerThreadRevocableMemory(1200, 0) == true);
    }
}
CATCH

TEST_F(TestAggSpillContext, ClearRevocableMemory)
try
{
    auto agg_spill_context = std::make_shared<AggSpillContext>(1, *spill_config_ptr, 1000, logger);
    agg_spill_context->updatePerThreadRevocableMemory(100, 0);
    ASSERT_TRUE(agg_spill_context->getTotalRevocableMemory() == 100);
    agg_spill_context->clearPerThreadRevocableMemory(0);
    ASSERT_TRUE(agg_spill_context->getTotalRevocableMemory() == 0);
    ASSERT_TRUE(agg_spill_context->updatePerThreadRevocableMemory(2000, 0) == false);
    ASSERT_TRUE(agg_spill_context->getTotalRevocableMemory() == 0);
}
CATCH

} // namespace tests
} // namespace DB
