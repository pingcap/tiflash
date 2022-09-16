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

#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/SSTFilesToDTFilesOutputStream.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <magic_enum.hpp>

namespace DB
{
namespace DM
{
namespace tests
{

class SSTFilesToDTFilesOutputStreamTest
    : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        pk_type = DMTestEnv::PkType::HiddenTiDBRowID;
        mock_region = makeRegion(1, RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 1000));

        TiFlashStorageTestBasic::SetUp();
        setupStorage();
    }

    void TearDown() override
    {
        storage->drop();
        db_context->getTMTContext().getStorages().remove(/* table id */ 100);
    }

    void setupStorage()
    {
        auto columns = DM::tests::DMTestEnv::getDefaultTableColumns(pk_type);
        auto table_info = DM::tests::DMTestEnv::getMinimalTableInfo(/* table id */ 100, pk_type);
        auto astptr = DM::tests::DMTestEnv::getPrimaryKeyExpr("test_table", pk_type);

        storage = StorageDeltaMerge::create("TiFlash",
                                            "default" /* db_name */,
                                            "test_table" /* table_name */,
                                            table_info,
                                            ColumnsDescription{columns},
                                            astptr,
                                            0,
                                            db_context->getGlobalContext());
        storage->startup();
    }

    BlockInputStreamPtr prepareBlocks(size_t begin, size_t end, size_t block_size)
    {
        RUNTIME_CHECK(end > begin);

        BlocksList list{};
        while (true)
        {
            if (begin >= end)
                break;
            auto this_block_size = std::min(end - begin, block_size);
            auto block = DMTestEnv::prepareSimpleWriteBlock(begin, begin + this_block_size, false, pk_type, 2);
            list.push_back(block);
            begin += this_block_size;
        }

        BlockInputStreamPtr stream = std::make_shared<BlocksListBlockInputStream>(std::move(list));
        return stream;
    }

protected:
    StorageDeltaMergePtr storage;
    RegionPtr mock_region;
    DMTestEnv::PkType pk_type;
};


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputSingleDTFile)
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false);

    auto mock_stream = std::make_shared<DM::MockSSTFilesToDTFilesOutputStreamChild>(prepareBlocks(50, 100, /*block_size=*/5), mock_region);
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        mock_stream,
        storage,
        schema_snapshot,
        TiDB::SnapshotApplyMethod::DTFile_Directory,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 0,
        /* split_after_size */ 0,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto ids = stream->ingestIds();
    ASSERT_EQ(1, ids.size());
}


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputSingleDTFileWithOneBlock)
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false);

    auto mock_stream = std::make_shared<DM::MockSSTFilesToDTFilesOutputStreamChild>(prepareBlocks(50, 100, /*block_size=*/1000), mock_region);
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        mock_stream,
        storage,
        schema_snapshot,
        TiDB::SnapshotApplyMethod::DTFile_Directory,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 1,
        /* split_after_size */ 1,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto ids = stream->ingestIds();

    // We expect to have only 1 DTFile, as there is only one block.
    ASSERT_EQ(1, ids.size());
}


TEST_F(SSTFilesToDTFilesOutputStreamTest, OutputMultipleDTFile)
{
    auto table_lock = storage->lockStructureForShare("foo_query_id");
    auto [schema_snapshot, unused] = storage->getSchemaSnapshotAndBlockForDecoding(table_lock, false);

    auto mock_stream = std::make_shared<DM::MockSSTFilesToDTFilesOutputStreamChild>(prepareBlocks(50, 100, /*block_size=*/1), mock_region);
    auto stream = std::make_shared<DM::SSTFilesToDTFilesOutputStream<DM::MockSSTFilesToDTFilesOutputStreamChildPtr>>(
        mock_stream,
        storage,
        schema_snapshot,
        TiDB::SnapshotApplyMethod::DTFile_Directory,
        FileConvertJobType::ApplySnapshot,
        /* split_after_rows */ 10,
        /* split_after_size */ 0,
        *db_context);

    stream->writePrefix();
    stream->write();
    stream->writeSuffix();
    auto ids = stream->ingestIds();
    ASSERT_EQ(5, ids.size());
}


} // namespace tests
} // namespace DM
} // namespace DB