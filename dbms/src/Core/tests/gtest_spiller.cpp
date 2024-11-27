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

#include <Common/FailPoint.h>
#include <Core/Defines.h>
#include <Core/Spiller.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class SpillerTest : public testing::Test
{
protected:
    void SetUp() override
    {
        logger = Logger::get("SpillerTest");
        Poco::File spiller_dir(spill_dir);
        /// remove spiller dir if exists
        if (spiller_dir.exists())
            spiller_dir.remove(true);
        spiller_dir.createDirectories();
        NamesAndTypes names_and_types;
        names_and_types.emplace_back("col0", DataTypeFactory::instance().get("Int64"));
        names_and_types.emplace_back("col1", DataTypeFactory::instance().get("UInt64"));
        spiller_test_header = Block(names_and_types);
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
    static Blocks generateBlocks(size_t block_num, const Block & schema)
    {
        Blocks ret;
        for (size_t i = 0; i < block_num; ++i)
        {
            ColumnsWithTypeAndName data;
            for (const auto & type_and_name : schema)
            {
                auto column = type_and_name.type->createColumn();
                for (size_t k = 0; k < 100; ++k)
                    column->insert(static_cast<UInt64>(k));
                data.push_back(ColumnWithTypeAndName(std::move(column), type_and_name.type, type_and_name.name));
            }
            ret.emplace_back(data);
        }
        return ret;
    }
    Blocks generateBlocks(size_t block_num) { return generateBlocks(block_num, spiller_test_header); }
    Blocks generateSortedBlocks(size_t block_num)
    {
        Blocks ret;
        for (size_t i = 0; i < block_num; ++i)
        {
            ColumnsWithTypeAndName data;
            for (const auto & type_and_name : spiller_test_header)
            {
                auto column = type_and_name.type->createColumn();
                for (size_t k = 0; k < 100; ++k)
                    column->insert(static_cast<UInt64>(k + i * 100));
                data.push_back(ColumnWithTypeAndName(std::move(column), type_and_name.type, type_and_name.name));
            }
            ret.emplace_back(data);
        }
        return ret;
    }
    static void verifyRestoreBlocks(
        Spiller & spiller,
        size_t restore_partition_id,
        size_t restore_max_stream_size,
        size_t expected_stream_size,
        const Blocks & expected_blocks,
        bool append_dummy_read_stream = false)
    {
        auto block_streams
            = spiller.restoreBlocks(restore_partition_id, restore_max_stream_size, append_dummy_read_stream);
        if (expected_stream_size > 0)
        {
            GTEST_ASSERT_EQ(block_streams.size(), expected_stream_size);
        }
        Blocks restored_blocks;
        for (auto & block_stream : block_streams)
        {
            for (Block block = block_stream->read(); block; block = block_stream->read())
                restored_blocks.push_back(block);
        }
        GTEST_ASSERT_EQ(expected_blocks.size(), restored_blocks.size());
        for (size_t i = 0; i < expected_blocks.size(); ++i)
        {
            blockEqual(expected_blocks[i], restored_blocks[i]);
        }
    }

    static String spill_dir;
    Block spiller_test_header;
    std::shared_ptr<SpillConfig> spill_config_ptr;
    LoggerPtr logger;
};

String SpillerTest::spill_dir = DB::tests::TiFlashTestEnv::getTemporaryPath("spiller_test");

TEST_F(SpillerTest, SpilledFileAutoRemove)
try
{
    auto file_name = spill_config_ptr->spill_dir + "spilled_file_auto_remove";
    {
        SpilledFile test_file(file_name, spill_config_ptr->file_provider);
        test_file.createFile();
        ASSERT(test_file.exists());
    }
    ASSERT(!Poco::File(file_name).exists());
}
CATCH

TEST_F(SpillerTest, InvalidPartitionIdInSpill)
try
{
    Spiller spiller(*spill_config_ptr, false, 20, spiller_test_header, logger);
    spiller.spillBlocks(generateBlocks(10), 30);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(
        e.message(),
        "Check partition_id < partition_num failed: test: partition id 30 exceeds partition num 20.");
}

TEST_F(SpillerTest, ExceptionDuringSpill)
try
{
    FailPointHelper::enableFailPoint("exception_during_spill");
    Spiller spiller(*spill_config_ptr, false, 1, spiller_test_header, logger);
    try
    {
        spiller.spillBlocks(generateBlocks(10), 0);
        GTEST_FAIL();
    }
    catch (Exception & e)
    {
        GTEST_ASSERT_EQ(std::strstr(e.message().c_str(), "exception_during_spill") != nullptr, true);
        GTEST_ASSERT_EQ(spiller.hasSpilledData(), false);
    }
    FailPointHelper::disableFailPoint("exception_during_spill");
}
CATCH

TEST_F(SpillerTest, SpillAfterFinish)
try
{
    Spiller spiller(*spill_config_ptr, false, 20, spiller_test_header, logger);
    spiller.finishSpill();
    spiller.spillBlocks(generateBlocks(10), 0);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check isSpillFinished() == false failed: test: spill after the spiller is finished.");
}

TEST_F(SpillerTest, InvalidPartitionIdInRestore)
try
{
    Spiller spiller(*spill_config_ptr, false, 20, spiller_test_header, logger);
    spiller.finishSpill();
    spiller.restoreBlocks(30, 20);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(
        e.message(),
        "Check partition_id < partition_num failed: test: partition id 30 exceeds partition num 20.");
}

TEST_F(SpillerTest, RestoreBeforeFinish)
try
{
    Spiller spiller(*spill_config_ptr, false, 20, spiller_test_header, logger);
    spiller.restoreBlocks(1, 1);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(e.message(), "Check isSpillFinished() failed: test: restore before the spiller is finished.");
}

TEST_F(SpillerTest, SpilledBlockDataSize)
try
{
    Spiller spiller(*spill_config_ptr, false, 1, spiller_test_header, logger);
    size_t spill_num = 5;
    size_t ref = 0;
    for (size_t spill_time = 0; spill_time < spill_num; ++spill_time)
    {
        auto blocks = generateBlocks(3);
        for (const auto & block : blocks)
            ref += block.rows();
        spiller.spillBlocks(std::move(blocks), 0);
    }
    spiller.finishSpill();
    GTEST_ASSERT_EQ(ref, spiller.spilledRows(0));
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreUnorderedBlocks)
try
{
    Spiller spiller(*spill_config_ptr, false, 2, spiller_test_header, logger);
    size_t partition_num = 2;
    size_t spill_num = 5;
    std::vector<Blocks> all_blocks(partition_num);
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        for (size_t spill_time = 0; spill_time < spill_num; ++spill_time)
        {
            auto blocks = generateBlocks(3);
            all_blocks[partition_id].insert(all_blocks[partition_id].end(), blocks.begin(), blocks.end());
            spiller.spillBlocks(std::move(blocks), partition_id);
        }
    }
    spiller.finishSpill();
    ASSERT_TRUE(spiller.hasSpilledData());
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        size_t max_restore_streams = 2 + partition_id * 10;
        size_t expected_streams = std::min(max_restore_streams, spill_num);
        verifyRestoreBlocks(spiller, partition_id, max_restore_streams, expected_streams, all_blocks[partition_id]);
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreUnorderedBlocksUsingBlockInputStream)
try
{
    std::vector<std::unique_ptr<Spiller>> spillers;
    spillers.push_back(std::make_unique<Spiller>(*spill_config_ptr, false, 2, spiller_test_header, logger));
    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_cached_data_bytes_in_spiller
        = spill_config_ptr->max_cached_data_bytes_in_spiller / 1000;
    spillers.push_back(
        std::make_unique<Spiller>(spiller_config_with_small_max_spill_size, false, 2, spiller_test_header, logger));

    for (auto & spiller : spillers)
    {
        size_t partition_num = 2;
        size_t spill_num = 5;
        std::vector<Blocks> all_blocks(partition_num);
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            for (size_t spill_time = 0; spill_time < spill_num; ++spill_time)
            {
                auto blocks = generateBlocks(50);
                BlocksList block_list;
                block_list.insert(block_list.end(), blocks.begin(), blocks.end());
                all_blocks[partition_id].insert(all_blocks[partition_id].end(), blocks.begin(), blocks.end());
                auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
                spiller->spillBlocksUsingBlockInputStream(block_input_stream, partition_id, []() { return false; });
            }
        }
        spiller->finishSpill();
        ASSERT_TRUE(spiller->hasSpilledData());
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            size_t max_restore_streams = 2 + partition_id * 10;
            size_t expected_streams = std::min(max_restore_streams, spill_num);
            verifyRestoreBlocks(
                *spiller,
                partition_id,
                max_restore_streams,
                expected_streams,
                all_blocks[partition_id]);
        }
    }
}
CATCH

TEST_F(SpillerTest, ReleaseFileOnRestore)
try
{
    std::vector<std::unique_ptr<Spiller>> spillers;
    spillers.push_back(std::make_unique<Spiller>(*spill_config_ptr, false, 1, spiller_test_header, logger, 1, false));
    auto new_spill_path = fmt::format("{}{}_{}", spill_config_ptr->spill_dir, "release_file_on_restore_test", rand());
    SpillConfig new_spill_config(
        new_spill_path,
        spill_config_ptr->spill_id,
        spill_config_ptr->max_cached_data_bytes_in_spiller,
        0,
        0,
        spill_config_ptr->file_provider);
    Poco::File new_spiller_dir(new_spill_config.spill_dir);
    /// remove spiller dir if exists
    if (new_spiller_dir.exists())
        new_spiller_dir.remove(true);
    new_spiller_dir.createDirectories();
    spillers.push_back(std::make_unique<Spiller>(new_spill_config, false, 1, spiller_test_header, logger));

    Blocks blocks = generateBlocks(50);
    for (auto & spiller : spillers)
    {
        auto blocks_to_spill = blocks;
        spiller->spillBlocks(std::move(blocks_to_spill), 0);
        spiller->finishSpill();
        ASSERT_TRUE(spiller->hasSpilledData());
        verifyRestoreBlocks(*spiller, 0, 0, 0, blocks);
        if (!spiller->releaseSpilledFileOnRestore())
            verifyRestoreBlocks(*spiller, 0, 0, 0, blocks);
        else
        {
            std::vector<String> files;
            new_spiller_dir.list(files);
            GTEST_ASSERT_EQ(files.size(), 0);
        }
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreOrderedBlocks)
try
{
    Spiller spiller(*spill_config_ptr, true, 2, spiller_test_header, logger);
    size_t partition_num = 2;
    size_t spill_num = 5;
    std::vector<Blocks> all_blocks(partition_num);
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        for (size_t spill_time = 0; spill_time < spill_num; ++spill_time)
        {
            auto blocks = generateSortedBlocks(3);
            all_blocks[partition_id].insert(all_blocks[partition_id].end(), blocks.begin(), blocks.end());
            spiller.spillBlocks(std::move(blocks), partition_id);
        }
    }
    spiller.finishSpill();
    ASSERT_TRUE(spiller.hasSpilledData());
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        size_t max_restore_streams = 2 + partition_id * 10;
        size_t expected_streams = spill_num;
        verifyRestoreBlocks(spiller, partition_id, max_restore_streams, expected_streams, all_blocks[partition_id]);
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreOrderedBlocksUsingBlockInputStream)
try
{
    std::vector<std::unique_ptr<Spiller>> spillers;
    spillers.push_back(std::make_unique<Spiller>(*spill_config_ptr, true, 2, spiller_test_header, logger));
    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_cached_data_bytes_in_spiller
        = spill_config_ptr->max_cached_data_bytes_in_spiller / 1000;
    spillers.push_back(
        std::make_unique<Spiller>(spiller_config_with_small_max_spill_size, true, 2, spiller_test_header, logger));

    for (auto & spiller : spillers)
    {
        size_t partition_num = 2;
        size_t spill_num = 5;
        std::vector<Blocks> all_blocks(partition_num);
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            for (size_t spill_time = 0; spill_time < spill_num; ++spill_time)
            {
                auto blocks = generateBlocks(50);
                BlocksList block_list;
                block_list.insert(block_list.end(), blocks.begin(), blocks.end());
                all_blocks[partition_id].insert(all_blocks[partition_id].end(), blocks.begin(), blocks.end());
                auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
                spiller->spillBlocksUsingBlockInputStream(block_input_stream, partition_id, []() { return false; });
            }
        }
        spiller->finishSpill();
        ASSERT_TRUE(spiller->hasSpilledData());
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            size_t max_restore_streams = 2 + partition_id * 10;
            /// for sorted spill, the restored stream num is always equal to the spill time
            size_t expected_streams = spill_num;
            verifyRestoreBlocks(
                *spiller,
                partition_id,
                max_restore_streams,
                expected_streams,
                all_blocks[partition_id]);
        }
    }
}
CATCH

TEST_F(SpillerTest, RestoreWithAppendDummyReadStream)
try
{
    auto spiller_config_for_append_write = *spill_config_ptr;

    /// append_dummy_read = false
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1000000000;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        Blocks all_blocks;
        auto blocks = generateBlocks(20);
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        auto blocks_copy = blocks;
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.spillBlocks(std::move(blocks_copy), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());
        verifyRestoreBlocks(spiller, 0, 20, 1, all_blocks, false);
    }
    /// append_dummy_read = true
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1000000000;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        Blocks all_blocks;
        auto blocks = generateBlocks(20);
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        auto blocks_copy = blocks;
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.spillBlocks(std::move(blocks_copy), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());
        verifyRestoreBlocks(spiller, 0, 20, 20, all_blocks, true);
    }
}
CATCH

TEST_F(SpillerTest, AppendWrite)
try
{
    auto spiller_config_for_append_write = *spill_config_ptr;

    /// case 1, multiple spill write to the same file
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1000000000;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        Blocks all_blocks;
        auto blocks = generateBlocks(50);
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
        auto blocks_copy = blocks;
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.spillBlocks(std::move(blocks_copy), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());
        verifyRestoreBlocks(spiller, 0, 2, 1, all_blocks);
    }
    /// case 2, one spill write to multiple files
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        auto all_blocks = generateBlocks(20);
        auto reference = all_blocks;
        spiller.spillBlocks(std::move(all_blocks), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());
        verifyRestoreBlocks(spiller, 0, 0, 20, reference);
    }
    /// case 3, spill empty blocks to existing spilled file
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1000000000;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        Blocks all_blocks = generateBlocks(20);
        auto reference = all_blocks;
        spiller.spillBlocks(std::move(all_blocks), 0);
        Blocks empty_blocks;
        spiller.spillBlocks(std::move(empty_blocks), 0);
        BlocksList empty_blocks_list;
        auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(empty_blocks_list));
        spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() { return false; });
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());
        verifyRestoreBlocks(spiller, 0, 2, 1, reference);
    }
    /// case 4, spill empty blocks to new spilled file
    {
        spiller_config_for_append_write.max_spilled_rows_per_file = 1000000000;
        Spiller spiller(spiller_config_for_append_write, false, 1, spiller_test_header, logger);
        Blocks empty_blocks;
        spiller.spillBlocks(std::move(empty_blocks), 0);
        BlocksList empty_blocks_list;
        auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(empty_blocks_list));
        spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() { return false; });
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData() == false);
    }
}
CATCH

TEST_F(SpillerTest, SpillFileNumberUsingBlockInputStream)
try
{
    auto blocks = generateBlocks(50);
    size_t total_block_size = 0;
    size_t total_block_rows = 0;
    for (const auto & block : blocks)
    {
        total_block_size += block.bytes();
        total_block_rows += block.rows();
    }

    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_cached_data_bytes_in_spiller = total_block_size / 50;
    spiller_config_with_small_max_spill_size.max_spilled_bytes_per_file
        = spiller_config_with_small_max_spill_size.max_cached_data_bytes_in_spiller;
    spiller_config_with_small_max_spill_size.max_spilled_rows_per_file = total_block_rows / 50;

    /// case 1, sorted spiller, only 1 file per spill
    Spiller sorted_spiller(spiller_config_with_small_max_spill_size, true, 1, spiller_test_header, logger);
    BlocksList block_list;
    block_list.insert(block_list.end(), blocks.begin(), blocks.end());
    auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
    sorted_spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() { return false; });
    sorted_spiller.finishSpill();
    auto restore_streams = sorted_spiller.restoreBlocks(0);
    ASSERT_TRUE(restore_streams.size() == 1);

    /// case 2, non-sorted spiller, multiple file
    Spiller non_sorted_spiller(spiller_config_with_small_max_spill_size, false, 1, spiller_test_header, logger);
    block_list.clear();
    block_list.insert(block_list.end(), blocks.begin(), blocks.end());
    block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
    non_sorted_spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() { return false; });
    non_sorted_spiller.finishSpill();
    restore_streams = non_sorted_spiller.restoreBlocks(0);
    ASSERT_TRUE(restore_streams.size() > 1);
}
CATCH

TEST_F(SpillerTest, SpillAndMeetCancelled)
try
{
    auto blocks = generateBlocks(50);
    size_t total_block_size = 0;
    for (const auto & block : blocks)
        total_block_size += block.bytes();

    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_cached_data_bytes_in_spiller = total_block_size / 50;
    Spiller spiller(spiller_config_with_small_max_spill_size, false, 1, spiller_test_header, logger);
    BlocksList block_list;
    block_list.insert(block_list.end(), blocks.begin(), blocks.end());
    auto block_input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
    spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() {
        static Int64 i = 0;
        return i++ >= 10;
    });
    ASSERT_EQ(spiller.hasSpilledData(), false);
}
CATCH

TEST_F(SpillerTest, SpillAllConstantBlock)
try
{
    auto constant_header = spiller_test_header;
    for (auto & type_and_name : constant_header)
        type_and_name.column = type_and_name.type->createColumnConst(1, Field(static_cast<Int64>(1)));

    {
        Blocks blocks;
        size_t rows = 10;
        size_t block_num = 10;
        for (size_t i = 0; i < block_num; ++i)
        {
            Block block = constant_header;
            for (auto & col : block)
                col.column = col.column->cloneResized(rows);
            blocks.push_back(std::move(block));
        }
        auto new_spill_path = fmt::format("{}{}_{}", spill_config_ptr->spill_dir, "SpillAllConstantBlock1", rand());
        SpillConfig new_spill_config(
            new_spill_path,
            spill_config_ptr->spill_id,
            spill_config_ptr->max_cached_data_bytes_in_spiller,
            0,
            0,
            spill_config_ptr->file_provider,
            100,
            DEFAULT_BLOCK_SIZE);
        Spiller spiller(new_spill_config, false, 1, constant_header, logger);
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());

        Block expected_block = constant_header;
        for (auto & col : expected_block)
            col.column = col.column->cloneResized(rows * block_num);
        verifyRestoreBlocks(spiller, 0, 100, 1, {expected_block});
    }

    {
        Blocks blocks;
        size_t block_num = DEFAULT_BLOCK_SIZE + 1;
        for (size_t i = 0; i < block_num; ++i)
        {
            Block block = constant_header;
            for (auto & col : block)
                col.column = col.column->cloneResized(1);
            blocks.push_back(std::move(block));
        }
        auto new_spill_path = fmt::format("{}{}_{}", spill_config_ptr->spill_dir, "SpillAllConstantBlock2", rand());
        SpillConfig new_spill_config(
            new_spill_path,
            spill_config_ptr->spill_id,
            spill_config_ptr->max_cached_data_bytes_in_spiller,
            0,
            0,
            spill_config_ptr->file_provider,
            100,
            DEFAULT_BLOCK_SIZE);
        Spiller spiller(new_spill_config, false, 1, constant_header, logger);
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());

        Blocks expected_blocks;
        Block block1 = constant_header;
        for (auto & col : block1)
            col.column = col.column->cloneResized(new_spill_config.for_all_constant_block_size);
        expected_blocks.push_back(std::move(block1));
        Block block2 = constant_header;
        for (auto & col : block2)
            col.column = col.column->cloneResized(1);
        expected_blocks.push_back(std::move(block2));
        verifyRestoreBlocks(spiller, 0, 100, 2, expected_blocks);
    }

    {
        Blocks blocks;
        size_t block_num = DEFAULT_BLOCK_SIZE + 1;
        for (size_t i = 0; i < block_num; ++i)
        {
            Block block = constant_header;
            for (auto & col : block)
                col.column = col.column->cloneResized(1);
            blocks.push_back(std::move(block));
        }
        auto new_spill_path = fmt::format("{}{}_{}", spill_config_ptr->spill_dir, "SpillAllConstantBlock3", rand());
        SpillConfig new_spill_config(
            new_spill_path,
            spill_config_ptr->spill_id,
            spill_config_ptr->max_cached_data_bytes_in_spiller,
            0,
            0,
            spill_config_ptr->file_provider,
            100,
            DEFAULT_BLOCK_SIZE);
        Spiller spiller(new_spill_config, false, 1, constant_header, logger);
        spiller.spillBlocks(std::move(blocks), 0);
        spiller.finishSpill();
        ASSERT_TRUE(spiller.hasSpilledData());

        Blocks expected_blocks;
        Block block1 = constant_header;
        for (auto & col : block1)
            col.column = col.column->cloneResized(new_spill_config.for_all_constant_block_size);
        expected_blocks.push_back(std::move(block1));
        Block block2 = constant_header;
        for (auto & col : block2)
            col.column = col.column->cloneResized(1);
        expected_blocks.push_back(std::move(block2));
        verifyRestoreBlocks(spiller, 0, 1, 1, expected_blocks);
    }
}
CATCH

TEST_F(SpillerTest, SpillWithConstantSchemaAndNonConstantData)
try
{
    NamesAndTypes names_and_types;
    names_and_types.emplace_back("col0", DataTypeFactory::instance().get("Int64"));
    names_and_types.emplace_back("col1", DataTypeFactory::instance().get("UInt64"));

    std::vector<bool> const_columns_flag = {
        true,
        false,
    };


    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < names_and_types.size(); i++)
    {
        if (const_columns_flag[i])
        {
            /// const column
            columns.emplace_back(
                names_and_types[i].type->createColumnConst(1, Field(static_cast<Int64>(1))),
                names_and_types[i].type,
                names_and_types[i].name);
        }
        else
        {
            /// normal column
            columns.emplace_back(
                names_and_types[i].type->createColumn(),
                names_and_types[i].type,
                names_and_types[i].name);
        }
    }
    Block header(columns);
    Spiller spiller(*spill_config_ptr, false, 1, header, logger);
    auto all_blocks = generateBlocks(20, header);
    spiller.spillBlocks(std::move(all_blocks), 0);
    GTEST_FAIL();
}
catch (Exception & e)
{
    GTEST_ASSERT_EQ(
        e.message().find("Check block.getByPosition(*it).column->isColumnConst() failed: The 0-th column in block must "
                         "be constant column")
            != std::string::npos,
        true);
}

TEST_F(SpillerTest, SpillAndRestoreConstantData)
try
{
    NamesAndTypes names_and_types;
    names_and_types.emplace_back("col0", DataTypeFactory::instance().get("Int64"));
    names_and_types.emplace_back("col1", DataTypeFactory::instance().get("UInt64"));
    names_and_types.emplace_back("col2", DataTypeFactory::instance().get("Nullable(Int64)"));
    names_and_types.emplace_back("col3", DataTypeFactory::instance().get("Nullable(UInt64)"));
    names_and_types.emplace_back("col4", DataTypeFactory::instance().get("Int64"));
    names_and_types.emplace_back("col5", DataTypeFactory::instance().get("UInt64"));

    std::vector<std::vector<bool>> const_columns_flags = {
        {false, false, false, false, false, true},
        {false, true, true, true, true, true},
        {true, false, false, false, false, false},
        {true, true, true, true, true, false},
        {true, false, true, false, true, false},
        {false, true, false, true, false, true},
        {false, true, false, true, false, false},
        {true, false, true, false, true, true},
    };

    for (const auto & const_columns_flag : const_columns_flags)
    {
        ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < names_and_types.size(); i++)
        {
            if (const_columns_flag[i])
            {
                /// const column
                columns.emplace_back(
                    names_and_types[i].type->createColumnConst(1, Field(static_cast<Int64>(1))),
                    names_and_types[i].type,
                    names_and_types[i].name);
            }
            else
            {
                /// normal column
                columns.emplace_back(
                    names_and_types[i].type->createColumn(),
                    names_and_types[i].type,
                    names_and_types[i].name);
            }
        }
        Block header(columns);
        Spiller spiller(*spill_config_ptr, false, 1, header, logger);
        auto all_blocks = generateBlocks(20, header);
        for (auto & block : all_blocks)
        {
            for (size_t i = 0; i < const_columns_flag.size(); i++)
            {
                if (header.getByPosition(i).column->isColumnConst())
                {
                    Field constant_field;
                    header.getByPosition(i).column->get(0, constant_field);
                    block.getByPosition(i).column
                        = header.getByPosition(i).type->createColumnConst(block.rows(), constant_field);
                }
            }
        }
        auto reference = all_blocks;
        spiller.spillBlocks(std::move(all_blocks), 0);
        spiller.finishSpill();
        auto block_streams = spiller.restoreBlocks(0, 1);
        GTEST_ASSERT_EQ(block_streams.size(), 1);
        Blocks restored_blocks;
        for (auto & block_stream : block_streams)
        {
            for (Block block = block_stream->read(); block; block = block_stream->read())
                restored_blocks.push_back(block);
        }
        GTEST_ASSERT_EQ(reference.size(), restored_blocks.size());
        for (size_t i = 0; i < reference.size(); ++i)
        {
            blockEqual(materializeBlock(reference[i]), restored_blocks[i]);
        }
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreNumericData)
try
{
    NamesAndTypes spiller_schema;
    spiller_schema.emplace_back("col0", DataTypeFactory::instance().get("Int8"));
    spiller_schema.emplace_back("col1", DataTypeFactory::instance().get("Nullable(Int8)"));
    spiller_schema.emplace_back("col2", DataTypeFactory::instance().get("Int16"));
    spiller_schema.emplace_back("col3", DataTypeFactory::instance().get("Nullable(Int16)"));
    spiller_schema.emplace_back("col4", DataTypeFactory::instance().get("Int32"));
    spiller_schema.emplace_back("col5", DataTypeFactory::instance().get("Nullable(Int32)"));
    spiller_schema.emplace_back("col6", DataTypeFactory::instance().get("Int64"));
    spiller_schema.emplace_back("col7", DataTypeFactory::instance().get("Nullable(Int64)"));
    spiller_schema.emplace_back("col8", DataTypeFactory::instance().get("UInt8"));
    spiller_schema.emplace_back("col9", DataTypeFactory::instance().get("Nullable(UInt8)"));
    spiller_schema.emplace_back("col10", DataTypeFactory::instance().get("UInt16"));
    spiller_schema.emplace_back("col11", DataTypeFactory::instance().get("Nullable(UInt16)"));
    spiller_schema.emplace_back("col12", DataTypeFactory::instance().get("UInt32"));
    spiller_schema.emplace_back("col13", DataTypeFactory::instance().get("Nullable(UInt32)"));
    spiller_schema.emplace_back("col14", DataTypeFactory::instance().get("UInt64"));
    spiller_schema.emplace_back("col15", DataTypeFactory::instance().get("Nullable(UInt64)"));
    spiller_schema.emplace_back("col16", DataTypeFactory::instance().get("Float32"));
    spiller_schema.emplace_back("col17", DataTypeFactory::instance().get("Nullable(Float32)"));
    spiller_schema.emplace_back("col18", DataTypeFactory::instance().get("Float64"));
    spiller_schema.emplace_back("col19", DataTypeFactory::instance().get("Nullable(Float64)"));
    auto spiller_header = Block(spiller_schema);

    Spiller spiller(*spill_config_ptr, false, 1, spiller_header, logger);

    Blocks ret;
    ColumnsWithTypeAndName data;
    size_t rows = 100;
    for (const auto & type_and_name : spiller_schema)
    {
        auto column = ColumnGenerator::instance().generate(
            {rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    auto reference = ret;
    spiller.spillBlocks(std::move(ret), 0);
    spiller.finishSpill();
    verifyRestoreBlocks(spiller, 0, 2, 1, reference);
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreDecimalData)
try
{
    NamesAndTypes spiller_schema;
    /// For Decimal32
    spiller_schema.emplace_back("col0", DataTypeFactory::instance().get("Decimal(5,2)"));
    spiller_schema.emplace_back("col1", DataTypeFactory::instance().get("Nullable(Decimal(5,2))"));
    /// For Decimal64
    spiller_schema.emplace_back("col2", DataTypeFactory::instance().get("Decimal(15,2)"));
    spiller_schema.emplace_back("col3", DataTypeFactory::instance().get("Nullable(Decimal(15,2))"));
    /// For Decimal128
    spiller_schema.emplace_back("col4", DataTypeFactory::instance().get("Decimal(25,2)"));
    spiller_schema.emplace_back("col5", DataTypeFactory::instance().get("Nullable(Decimal(25,2))"));
    /// For Decimal256
    spiller_schema.emplace_back("col6", DataTypeFactory::instance().get("Decimal(45,2)"));
    spiller_schema.emplace_back("col7", DataTypeFactory::instance().get("Nullable(Decimal(45,2))"));

    auto spiller_header = Block(spiller_schema);

    Spiller spiller(*spill_config_ptr, false, 1, spiller_header, logger);

    Blocks ret;
    ColumnsWithTypeAndName data;
    size_t rows = 100;
    for (const auto & type_and_name : spiller_schema)
    {
        auto column = ColumnGenerator::instance().generate(
            {rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    auto reference = ret;
    spiller.spillBlocks(std::move(ret), 0);
    spiller.finishSpill();
    verifyRestoreBlocks(spiller, 0, 2, 1, reference);
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreDateTimeData)
try
{
    NamesAndTypes spiller_schema;
    spiller_schema.emplace_back("col0", DataTypeFactory::instance().get("MyDate"));
    spiller_schema.emplace_back("col1", DataTypeFactory::instance().get("Nullable(MyDate)"));
    spiller_schema.emplace_back("col2", DataTypeFactory::instance().get("MyDatetime(3)"));
    spiller_schema.emplace_back("col3", DataTypeFactory::instance().get("Nullable(MyDatetime(3))"));
    spiller_schema.emplace_back("col4", DataTypeFactory::instance().get("MyDatetime(6)"));
    spiller_schema.emplace_back("col5", DataTypeFactory::instance().get("Nullable(MyDatetime(6))"));
    spiller_schema.emplace_back("col6", DataTypeFactory::instance().get("MyDuration(3)"));
    spiller_schema.emplace_back("col7", DataTypeFactory::instance().get("Nullable(MyDuration(3))"));
    spiller_schema.emplace_back("col8", DataTypeFactory::instance().get("MyDuration(6)"));
    spiller_schema.emplace_back("col9", DataTypeFactory::instance().get("Nullable(MyDuration(6))"));

    auto spiller_header = Block(spiller_schema);

    Spiller spiller(*spill_config_ptr, false, 1, spiller_header, logger);

    Blocks ret;
    ColumnsWithTypeAndName data;
    size_t rows = 100;
    for (const auto & type_and_name : spiller_schema)
    {
        auto column = ColumnGenerator::instance().generate(
            {rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    auto reference = ret;
    spiller.spillBlocks(std::move(ret), 0);
    spiller.finishSpill();
    verifyRestoreBlocks(spiller, 0, 2, 1, reference);
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreStringEnumData)
try
{
    NamesAndTypes spiller_schema;
    spiller_schema.emplace_back("col0", DataTypeFactory::instance().get("StringV1"));
    spiller_schema.emplace_back("col1", DataTypeFactory::instance().get("Nullable(StringV1)"));
    spiller_schema.emplace_back("col2", DataTypeFactory::instance().get("Enum8('a' = 0,'b' = 1,'c' = 2)"));
    spiller_schema.emplace_back("col3", DataTypeFactory::instance().get("Nullable(Enum8('a' = 0,'b' = 1,'c' = 2))"));
    spiller_schema.emplace_back("col4", DataTypeFactory::instance().get("Enum16('a' = 0,'b' = 1,'c' = 2)"));
    spiller_schema.emplace_back("col5", DataTypeFactory::instance().get("Nullable(Enum16('a' = 0,'b' = 1,'c' = 2))"));

    auto spiller_header = Block(spiller_schema);

    Spiller spiller(*spill_config_ptr, false, 1, spiller_header, logger);

    Blocks ret;
    ColumnsWithTypeAndName data;
    size_t rows = 100;
    for (const auto & type_and_name : spiller_schema)
    {
        auto column = ColumnGenerator::instance().generate(
            {rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    auto reference = ret;
    spiller.spillBlocks(std::move(ret), 0);
    spiller.finishSpill();
    verifyRestoreBlocks(spiller, 0, 2, 1, reference);
}
CATCH

} // namespace tests
} // namespace DB
