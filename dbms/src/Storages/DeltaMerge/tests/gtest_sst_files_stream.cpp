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

#include <Common/SyncPoint/SyncPoint.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToDTFilesOutputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <Storages/PathPool.h>
#include <Storages/StorageDeltaMerge.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <magic_enum.hpp>

namespace DB
{
namespace DM
{
namespace tests
{

class SSTFilesToDTFilesOutputStreamTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        mock_region = makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1000));

        TiFlashStorageTestBasic::SetUp();
        setupStorage();
    }

    void TearDown() override
    {
        storage->drop();
        db_context->getTMTContext().getStorages().remove(NullspaceID, /* table id */ 100);
    }

    void setupStorage()
    {
        auto columns = DM::tests::DMTestEnv::getDefaultTableColumns(pk_type);
        auto table_info = DM::tests::DMTestEnv::getMinimalTableInfo(/* table id */ 100, pk_type);
        auto astptr = DM::tests::DMTestEnv::getPrimaryKeyExpr("test_table", pk_type);

        storage = StorageDeltaMerge::create(
            "TiFlash",
            "default" /* db_name */,
            "test_table" /* table_name */,
            table_info,
            ColumnsDescription{columns},
            astptr,
            0,
            db_context->getGlobalContext());
        storage->startup();
    }

    std::vector<Block> prepareBlocks(Int64 start_key, Int64 end_key, UInt64 block_size)
    {
        RUNTIME_CHECK(block_size > 0);
        RUNTIME_CHECK(block_size < std::numeric_limits<Int64>::max());

        std::vector<Block> blocks;
        while (true)
        {
            if (start_key >= end_key)
                break;
            auto this_block_size = std::min(static_cast<UInt64>(end_key - start_key), block_size);
            auto block = DMTestEnv::prepareSimpleWriteBlock(
                start_key,
                start_key + static_cast<Int64>(this_block_size),
                false,
                pk_type,
                2);
            blocks.push_back(block);
            start_key += static_cast<Int64>(this_block_size);
        }

        return blocks;
    }

    std::shared_ptr<DM::MockSSTFilesToDTFilesOutputStreamChild> makeMockChild(std::vector<Block> blocks)
    {
        BlocksList blocks_list(blocks.begin(), blocks.end());
        auto is = std::make_shared<BlocksListBlockInputStream>(std::move(blocks_list));
        return std::make_shared<DM::MockSSTFilesToDTFilesOutputStreamChild>(is, mock_region);
    }

protected:
    StorageDeltaMergePtr storage;
    RegionPtr mock_region;
    DMTestEnv::PkType pk_type = DMTestEnv::PkType::HiddenTiDBRowID;
};


TEST_F(SSTFilesToDTFilesOutputStreamTest, PrepareBlock)
try
{
    {
        auto blocks = prepareBlocks(5, 5, 1000);
        ASSERT_EQ(blocks.size(), 0);
    }
    {
        auto blocks = prepareBlocks(-10, 5, 1000);
        ASSERT_EQ(blocks.size(), 1);
        auto block = blocks[0];
        auto col = block.getByName(MutableSupport::tidb_pk_column_name);
        ASSERT_COLUMN_EQ(col, createColumn<Int64>({-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4}));
    }
    {
        auto blocks = prepareBlocks(1, 14, 3);
        ASSERT_EQ(blocks.size(), 5);
        {
            auto block = blocks[0];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({1, 2, 3}));
        }
        {
            auto block = blocks[1];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({4, 5, 6}));
        }
        {
            auto block = blocks[2];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({7, 8, 9}));
        }
        {
            auto block = blocks[3];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({10, 11, 12}));
        }
        {
            auto block = blocks[4];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({13}));
        }
    }
    {
        auto blocks = prepareBlocks(1, 4, 1);
        ASSERT_EQ(blocks.size(), 3);
        {
            auto block = blocks[0];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({1}));
        }
        {
            auto block = blocks[1];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({2}));
        }
        {
            auto block = blocks[2];
            auto col = block.getByName(MutableSupport::tidb_pk_column_name);
            ASSERT_COLUMN_EQ(col, createColumn<Int64>({3}));
        }
    }
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputNoDTFile)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(100, 100, /*block_size=*/5));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 0,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();

    auto files = stream->outputFiles();
    ASSERT_EQ(0, files.size());
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputSingleDTFile)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/5));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 0,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();

    auto files = stream->outputFiles();
    ASSERT_EQ(1, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 100);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputSingleDTFileWithOneBlock)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/1000));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 1,
        /* split_after_size */ 1,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();

    // We expect to have only 1 DTFile, as there is only one block.
    auto files = stream->outputFiles();
    ASSERT_EQ(1, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 100);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputMultipleDTFile)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/1));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto files = stream->outputFiles();
    ASSERT_EQ(5, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 60);
    ASSERT_EQ(files[1].range.getStart().int_value, 60);
    ASSERT_EQ(files[1].range.getEnd().int_value, 70);
    ASSERT_EQ(files[2].range.getStart().int_value, 70);
    ASSERT_EQ(files[2].range.getEnd().int_value, 80);
    ASSERT_EQ(files[3].range.getStart().int_value, 80);
    ASSERT_EQ(files[3].range.getEnd().int_value, 90);
    ASSERT_EQ(files[4].range.getStart().int_value, 90);
    ASSERT_EQ(files[4].range.getEnd().int_value, 100);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, SplitAtBlockBoundary)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/20));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto files = stream->outputFiles();
    ASSERT_EQ(3, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 70);
    ASSERT_EQ(files[1].range.getStart().int_value, 70);
    ASSERT_EQ(files[1].range.getEnd().int_value, 90);
    ASSERT_EQ(files[2].range.getStart().int_value, 90);
    ASSERT_EQ(files[2].range.getEnd().int_value, 100);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, VeryLargeSplitThreshold)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/20));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10000,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();

    auto files = stream->outputFiles();
    ASSERT_EQ(1, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 100);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, NonContinuousBlock)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto blocks1 = prepareBlocks(50, 100, /*block_size=*/20);
    auto blocks2 = prepareBlocks(130, 150, /*block_size=*/10);
    blocks1.insert(blocks1.end(), blocks2.begin(), blocks2.end());
    auto mock_stream = makeMockChild(blocks1);
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();

    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 20,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto files = stream->outputFiles();

    ASSERT_EQ(4, files.size());
    ASSERT_EQ(files[0].range.getStart().int_value, 50);
    ASSERT_EQ(files[0].range.getEnd().int_value, 70);
    ASSERT_EQ(files[1].range.getStart().int_value, 70);
    ASSERT_EQ(files[1].range.getEnd().int_value, 90);
    ASSERT_EQ(files[2].range.getStart().int_value, 90);
    ASSERT_EQ(files[2].range.getEnd().int_value, 140);
    ASSERT_EQ(files[3].range.getStart().int_value, 140);
    ASSERT_EQ(files[3].range.getEnd().int_value, 150);
}
CATCH


TEST_F(SSTFilesToDTFilesOutputStreamTest, BrokenChild)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto blocks1 = prepareBlocks(50, 100, /*block_size=*/20);
    auto blocks2 = prepareBlocks(0, 30, /*block_size=*/20);
    blocks1.insert(blocks1.end(), blocks2.begin(), blocks2.end());
    auto mock_stream = makeMockChild(blocks1);
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();

    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 20,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    EXPECT_THROW(
        {
            stream->writePrefix();
            stream->write();
            stream->writeSuffix();
        },
        DB::Exception);

    stream->cancel();
}
CATCH

TEST_F(SSTFilesToDTFilesOutputStreamTest, Cancel)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/1));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto files = stream->outputFiles();
    ASSERT_EQ(5, files.size());

    auto delegator = storage->getAndMaybeInitStore()->path_pool->getStableDiskDelegator();
    std::unordered_map<UInt64, String> file_id_to_path;
    for (const auto & file : files)
    {
        auto parent_path = delegator.getDTFilePath(file.id);
        auto file_path = DM::getPathByStatus(parent_path, file.id, DM::DMFileStatus::READABLE);
        file_id_to_path.emplace(file.id, file_path);
        ASSERT_TRUE(Poco::File(file_path).exists());
    }
    stream->cancel(); // remove all data
    for (const auto & file : files)
    {
        ASSERT_TRUE(delegator.getDTFilePath(file.id, /*throw_on_not_exists*/ false).empty());
        auto file_path = file_id_to_path[file.id];
        ASSERT_FALSE(Poco::File(file_path).exists());
    }

    // It should be empty
    auto output_files = stream->outputFiles();
    ASSERT_TRUE(output_files.empty());
}
CATCH

TEST_F(SSTFilesToDTFilesOutputStreamTest, UpperLayerCancel)
try
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false, true);

    auto mock_stream = makeMockChild(prepareBlocks(50, 100, /*block_size=*/1));
    auto prehandle_task = std::make_shared<PreHandlingTrace::Item>();
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        /* log_prefix */ "",
        mock_stream,
        storage,
        schema_snapshot,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10,
        /* split_after_size */ 0,
        0,
        prehandle_task,
        *db_context);

    auto sp = SyncPointCtl::enableInScope("before_SSTFilesToDTFilesOutputStream::handle_one");
    stream->writePrefix();
    auto t = std::thread([&]() { stream->write(); });
    sp.waitAndPause();
    prehandle_task->abortFor(PrehandleTransformStatus::Aborted);
    sp.next();
    sp.disable();
    t.join();
    stream->writeSuffix();
    auto files = stream->outputFiles();
    ASSERT_EQ(true, prehandle_task->isAbort());
    ASSERT_EQ(1, files.size());
    auto delegator = storage->getAndMaybeInitStore()->path_pool->getStableDiskDelegator();
    std::vector<std::string> fps;
    for (const auto & file : files)
    {
        auto parent_path = delegator.getDTFilePath(file.id);
        auto file_path = DM::getPathByStatus(parent_path, file.id, DM::DMFileStatus::READABLE);
        fps.push_back(file_path);
    }
    storage->cleanPreIngestFiles(files, db_context->getSettingsRef());

    for (const auto & file : files)
    {
        ASSERT_TRUE(delegator.getDTFilePath(file.id, /*throw_on_not_exists*/ false).empty());
    }
    for (const auto & f : fps)
    {
        ASSERT_FALSE(Poco::File(f).exists());
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
