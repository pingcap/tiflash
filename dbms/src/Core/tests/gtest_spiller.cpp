// Copyright 2023 PingCAP, Ltd.
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

#include <Core/Spiller.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Encryption/MockKeyManager.h>
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
        spill_config_ptr = std::make_shared<SpillConfig>(spill_dir, "test", 1024ULL * 1024 * 1024, file_provider);
    }
    void TearDown() override
    {
        Poco::File spiller_dir(spill_dir);
        /// remove spiller dir if exists
        if (spiller_dir.exists())
            spiller_dir.remove(true);
    }
    Blocks generateBlocks(size_t block_num)
    {
        Blocks ret;
        for (size_t i = 0; i < block_num; ++i)
        {
            ColumnsWithTypeAndName data;
            for (const auto & type_and_name : spiller_test_header)
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
    GTEST_ASSERT_EQ(e.message(), "Check partition_id < partition_num failed: test: partition id 30 exceeds partition num 20.");
}

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
    GTEST_ASSERT_EQ(e.message(), "Check spill_finished == false failed: test: spill after the spiller is finished.");
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
    GTEST_ASSERT_EQ(e.message(), "Check partition_id < partition_num failed: test: partition id 30 exceeds partition num 20.");
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
    GTEST_ASSERT_EQ(e.message(), "Check spill_finished failed: test: restore before the spiller is finished.");
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
        spiller.spillBlocks(blocks, 0);
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
            spiller.spillBlocks(blocks, partition_id);
        }
    }
    spiller.finishSpill();
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        size_t max_restore_streams = 2 + partition_id * 10;
        auto restore_block_streams = spiller.restoreBlocks(partition_id, max_restore_streams);
        size_t expected_streams = std::min(max_restore_streams, spill_num);
        GTEST_ASSERT_EQ(restore_block_streams.size(), expected_streams);
        Blocks all_restored_blocks;
        for (const auto & block_stream : restore_block_streams)
        {
            for (Block block = block_stream->read(); block; block = block_stream->read())
            {
                all_restored_blocks.push_back(block);
            }
        }
        GTEST_ASSERT_EQ(all_restored_blocks.size(), all_blocks[partition_id].size());
        for (size_t i = 0; i < all_restored_blocks.size(); ++i)
        {
            blockEqual(all_blocks[partition_id][i], all_restored_blocks[i]);
        }
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreUnorderedBlocksUsingBlockInputStream)
try
{
    std::vector<std::unique_ptr<Spiller>> spillers;
    spillers.push_back(std::make_unique<Spiller>(*spill_config_ptr, false, 2, spiller_test_header, logger));
    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_spilled_size_per_spill = spill_config_ptr->max_spilled_size_per_spill / 1000;
    spillers.push_back(std::make_unique<Spiller>(spiller_config_with_small_max_spill_size, false, 2, spiller_test_header, logger));

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
                BlocksListBlockInputStream block_input_stream(std::move(block_list));
                spiller->spillBlocksUsingBlockInputStream(block_input_stream, partition_id, []() { return false; });
            }
        }
        spiller->finishSpill();
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            size_t max_restore_streams = 2 + partition_id * 10;
            auto restore_block_streams = spiller->restoreBlocks(partition_id, max_restore_streams);
            size_t expected_streams = std::min(max_restore_streams, spill_num);
            GTEST_ASSERT_EQ(restore_block_streams.size(), expected_streams);
            Blocks all_restored_blocks;
            for (const auto & block_stream : restore_block_streams)
            {
                for (Block block = block_stream->read(); block; block = block_stream->read())
                {
                    all_restored_blocks.push_back(block);
                }
            }
            GTEST_ASSERT_EQ(all_restored_blocks.size(), all_blocks[partition_id].size());
            for (size_t i = 0; i < all_restored_blocks.size(); ++i)
            {
                blockEqual(all_blocks[partition_id][i], all_restored_blocks[i]);
            }
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
            spiller.spillBlocks(blocks, partition_id);
        }
    }
    spiller.finishSpill();
    for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
    {
        size_t max_restore_streams = 2 + partition_id * 10;
        auto restore_block_streams = spiller.restoreBlocks(partition_id, max_restore_streams);
        /// for sorted spill, the restored stream num is always equal to the spill time
        size_t expected_streams = spill_num;
        GTEST_ASSERT_EQ(restore_block_streams.size(), expected_streams);
        Blocks all_restored_blocks;
        for (const auto & block_stream : restore_block_streams)
        {
            for (Block block = block_stream->read(); block; block = block_stream->read())
            {
                all_restored_blocks.push_back(block);
            }
        }
        GTEST_ASSERT_EQ(all_restored_blocks.size(), all_blocks[partition_id].size());
        for (size_t i = 0; i < all_restored_blocks.size(); ++i)
        {
            blockEqual(all_blocks[partition_id][i], all_restored_blocks[i]);
        }
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreOrderedBlocksUsingBlockInputStream)
try
{
    std::vector<std::unique_ptr<Spiller>> spillers;
    spillers.push_back(std::make_unique<Spiller>(*spill_config_ptr, true, 2, spiller_test_header, logger));
    auto spiller_config_with_small_max_spill_size = *spill_config_ptr;
    spiller_config_with_small_max_spill_size.max_spilled_size_per_spill = spill_config_ptr->max_spilled_size_per_spill / 1000;
    spillers.push_back(std::make_unique<Spiller>(spiller_config_with_small_max_spill_size, true, 2, spiller_test_header, logger));

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
                BlocksListBlockInputStream block_input_stream(std::move(block_list));
                spiller->spillBlocksUsingBlockInputStream(block_input_stream, partition_id, []() { return false; });
            }
        }
        spiller->finishSpill();
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            size_t max_restore_streams = 2 + partition_id * 10;
            auto restore_block_streams = spiller->restoreBlocks(partition_id, max_restore_streams);
            /// for sorted spill, the restored stream num is always equal to the spill time
            size_t expected_streams = spill_num;
            GTEST_ASSERT_EQ(restore_block_streams.size(), expected_streams);
            Blocks all_restored_blocks;
            for (const auto & block_stream : restore_block_streams)
            {
                for (Block block = block_stream->read(); block; block = block_stream->read())
                {
                    all_restored_blocks.push_back(block);
                }
            }
            GTEST_ASSERT_EQ(all_restored_blocks.size(), all_blocks[partition_id].size());
            for (size_t i = 0; i < all_restored_blocks.size(); ++i)
            {
                blockEqual(all_blocks[partition_id][i], all_restored_blocks[i]);
            }
        }
    }
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
    spiller_config_with_small_max_spill_size.max_spilled_size_per_spill = total_block_size / 50;
    Spiller spiller(spiller_config_with_small_max_spill_size, false, 1, spiller_test_header, logger);
    BlocksList block_list;
    block_list.insert(block_list.end(), blocks.begin(), blocks.end());
    BlocksListBlockInputStream block_input_stream(std::move(block_list));
    spiller.spillBlocksUsingBlockInputStream(block_input_stream, 0, []() {
        static Int64 i = 0;
        return i++ >= 10;
    });
    ASSERT_EQ(spiller.hasSpilledData(), false);
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreConstantData)
try
{
    Spiller spiller(*spill_config_ptr, false, 1, spiller_test_header, logger);
    Blocks ret;
    ColumnsWithTypeAndName data;
    for (const auto & type_and_name : spiller_test_header)
    {
        auto column = type_and_name.type->createColumnConst(100, Field(static_cast<Int64>(1)));
        data.push_back(ColumnWithTypeAndName(std::move(column), type_and_name.type, type_and_name.name));
    }
    ret.emplace_back(data);
    spiller.spillBlocks(ret, 0);
    spiller.finishSpill();
    auto block_streams = spiller.restoreBlocks(0, 2);
    GTEST_ASSERT_EQ(block_streams.size(), 1);
    Blocks restored_blocks;
    for (auto & block_stream : block_streams)
    {
        for (Block block = block_stream->read(); block; block = block_stream->read())
            restored_blocks.push_back(block);
    }
    GTEST_ASSERT_EQ(ret.size(), restored_blocks.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
        blockEqual(materializeBlock(ret[i]), restored_blocks[i]);
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
        auto column = ColumnGenerator::instance().generate({rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    spiller.spillBlocks(ret, 0);
    spiller.finishSpill();
    auto block_streams = spiller.restoreBlocks(0, 2);
    GTEST_ASSERT_EQ(block_streams.size(), 1);
    Blocks restored_blocks;
    for (auto & block_stream : block_streams)
    {
        for (Block block = block_stream->read(); block; block = block_stream->read())
            restored_blocks.push_back(block);
    }
    GTEST_ASSERT_EQ(ret.size(), restored_blocks.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
        blockEqual(ret[i], restored_blocks[i]);
    }
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
        auto column = ColumnGenerator::instance().generate({rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    spiller.spillBlocks(ret, 0);
    spiller.finishSpill();
    auto block_streams = spiller.restoreBlocks(0, 2);
    GTEST_ASSERT_EQ(block_streams.size(), 1);
    Blocks restored_blocks;
    for (auto & block_stream : block_streams)
    {
        for (Block block = block_stream->read(); block; block = block_stream->read())
            restored_blocks.push_back(block);
    }
    GTEST_ASSERT_EQ(ret.size(), restored_blocks.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
        blockEqual(ret[i], restored_blocks[i]);
    }
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
        auto column = ColumnGenerator::instance().generate({rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    spiller.spillBlocks(ret, 0);
    spiller.finishSpill();
    auto block_streams = spiller.restoreBlocks(0, 2);
    GTEST_ASSERT_EQ(block_streams.size(), 1);
    Blocks restored_blocks;
    for (auto & block_stream : block_streams)
    {
        for (Block block = block_stream->read(); block; block = block_stream->read())
            restored_blocks.push_back(block);
    }
    GTEST_ASSERT_EQ(ret.size(), restored_blocks.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
        blockEqual(ret[i], restored_blocks[i]);
    }
}
CATCH

TEST_F(SpillerTest, SpillAndRestoreStringEnumData)
try
{
    NamesAndTypes spiller_schema;
    spiller_schema.emplace_back("col0", DataTypeFactory::instance().get("String"));
    spiller_schema.emplace_back("col1", DataTypeFactory::instance().get("Nullable(String)"));
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
        auto column = ColumnGenerator::instance().generate({rows, type_and_name.type->getName(), RANDOM, type_and_name.name, 128});
        data.push_back(column);
    }
    ret.emplace_back(data);
    spiller.spillBlocks(ret, 0);
    spiller.finishSpill();
    auto block_streams = spiller.restoreBlocks(0, 2);
    GTEST_ASSERT_EQ(block_streams.size(), 1);
    Blocks restored_blocks;
    for (auto & block_stream : block_streams)
    {
        for (Block block = block_stream->read(); block; block = block_stream->read())
            restored_blocks.push_back(block);
    }
    GTEST_ASSERT_EQ(ret.size(), restored_blocks.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
        blockEqual(ret[i], restored_blocks[i]);
    }
}
CATCH

} // namespace tests
} // namespace DB
