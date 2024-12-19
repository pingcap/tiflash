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
#include <Core/ColumnWithTypeAndName.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <Interpreters/Context.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/FormatVersion.h>
#include <Storages/PathPool.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <magic_enum.hpp>
#include <vector>
namespace DB
{
namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}
namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
} // namespace FailPoints
} // namespace DB

namespace DB::DM::tests
{
enum class DMFileMode
{
    DirectoryLegacy,
    DirectoryChecksum,
    DirectoryMetaV2,
};

String paramToString(const ::testing::TestParamInfo<DMFileMode> & info)
{
    return String{magic_enum::enum_name(info.param)};
}

static inline std::optional<DMChecksumConfig> createConfiguration(DMFileMode mode)
{
    return (mode != DMFileMode::DirectoryLegacy ? std::make_optional<DMChecksumConfig>() : std::nullopt);
}

inline static DMFileFormat::Version modeToVersion(DMFileMode mode)
{
    switch (mode)
    {
    case DMFileMode::DirectoryLegacy:
        return DMFileFormat::V1;
    case DMFileMode::DirectoryChecksum:
        return DMFileFormat::V2;
    case DMFileMode::DirectoryMetaV2: // here we just test without s3, for s3 we will test in gtest_s3file.cpp
        return DMFileFormat::V3;
    }
}

using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

class DMFileMetaV2Test : public DB::base::TiFlashStorageTestBasic
{
public:
    DMFileMetaV2Test()
        : dm_file(nullptr)
    {}

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        auto configuration = createConfiguration(DMFileMode::DirectoryMetaV2);
        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "DMFileTest", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");
        dm_file = DMFile::create(
            1,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            modeToVersion(DMFileMode::DirectoryMetaV2));
        table_columns = std::make_shared<ColumnDefines>();
        column_cache = std::make_shared<ColumnCache>();

        DB::tests::TiFlashTestEnv::disableS3Config();

        reload();
    }

    // Update dm_context.
    void reload(const ColumnDefinesPtr & cols = DMTestEnv::getDefaultColumns())
    {
        TiFlashStorageTestBasic::reload();
        if (table_columns != cols)
            *table_columns = *cols;
        *path_pool = db_context->getPathPool().withTable("test", "t1", false);
        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMFilePtr restoreDMFile()
    {
        auto file_id = dm_file->fileId();
        auto page_id = dm_file->pageId();
        auto parent_path = dm_file->parentPath();
        auto file_provider = dbContext().getFileProvider();
        return DMFile::restore(file_provider, file_id, page_id, parent_path, DMFileMeta::ReadMode::all());
    }

    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

    static void checkMergedFile(
        const DMFilePtr & dmfile,
        UInt64 merged_number,
        const std::set<String> & not_uploaded_files,
        std::set<String> & checked_fnames);
    static void checkColumnStats(const DMFilePtr & dmfile1, const DMFilePtr & dmfile2);

    static void breakFileMetaV2File(const DMFilePtr & dmfile)
    {
        PosixWritableFile file(dmfile->metav2Path(/* meta_version= */ 0), false, -1, 0666);
        String s = "hello";
        auto n = file.pwrite(s.data(), s.size(), 0);
        ASSERT_EQ(n, s.size());
    }

    static void skipFirstPack(DMFileBlockInputStreamPtr & stream)
    {
        const auto & pack_stats = stream->reader.dmfile->getPackStats();
        auto [pack_id, pack_count, res, rows] = stream->reader.read_block_infos.front();
        stream->reader.read_block_infos.pop_front();
        stream->reader.read_block_infos
            .emplace_front(pack_id + 1, pack_count - 1, res, rows - pack_stats[pack_id].rows);
        stream->reader.read_block_infos.emplace_front(pack_id, 1, res, pack_stats[pack_id].rows);
        ASSERT_EQ(stream->skipNextBlock(), pack_stats[pack_id].rows);
    }

protected:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DeltaMergeStore::Settings settings;

protected:
    String parent_path;
    DMFilePtr dm_file;
    ColumnCachePtr column_cache;
};

void DMFileMetaV2Test::checkMergedFile(
    const DMFilePtr & dmfile,
    UInt64 merged_number,
    const std::set<String> & not_uploaded_files,
    std::set<String> & checked_fnames)
{
    const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
    ASSERT_TRUE(dmfile_meta != nullptr);
    auto merged_filename = dmfile_meta->mergedPath(merged_number);
    auto merged_file = PosixRandomAccessFile::create(merged_filename);

    for (const auto & fname : not_uploaded_files)
    {
        auto itr = dmfile_meta->merged_sub_file_infos.find(fname);
        ASSERT_NE(itr, dmfile_meta->merged_sub_file_infos.end());
        if (itr->second.number != merged_number)
        {
            continue;
        }
        checked_fnames.insert(fname);

        String merged_data;
        {
            merged_data.resize(itr->second.size);
            auto n = merged_file->pread(merged_data.data(), itr->second.size, itr->second.offset);
            ASSERT_EQ(n, itr->second.size);
        }

        String sub_data;
        {
            auto sub_path = fmt::format("{}/{}", dmfile->path(), fname);
            auto sub_fsize = std::filesystem::file_size(sub_path);
            ASSERT_EQ(sub_fsize, itr->second.size);
            auto sub_file = PosixRandomAccessFile::create(sub_path);
            sub_data.resize(sub_fsize);
            auto n = sub_file->pread(sub_data.data(), sub_fsize, 0);
            ASSERT_EQ(n, sub_fsize);
        }
        ASSERT_EQ(merged_data, sub_data);
    }
}

void DMFileMetaV2Test::checkColumnStats(const DMFilePtr & dmfile1, const DMFilePtr & dmfile2)
{
    const auto & col_defs = dmfile1->getColumnDefines();
    for (const auto & col_def : col_defs)
    {
        const auto & col_stat1 = dmfile1->getColumnStat(col_def.id);
        const auto & col_stat2 = dmfile2->getColumnStat(col_def.id);
        ASSERT_DOUBLE_EQ(col_stat1.avg_size, col_stat2.avg_size);
        ASSERT_EQ(col_stat1.col_id, col_stat2.col_id);
        ASSERT_EQ(col_stat1.type->getName(), col_stat2.type->getName());
        ASSERT_GT(col_stat1.serialized_bytes, col_stat2.serialized_bytes); // v3 without mrk

        ASSERT_EQ(col_stat2.serialized_bytes, col_stat2.data_bytes + col_stat2.nullmap_data_bytes) << fmt::format(
            "data_bytes={} nullmap_data_bytes={} col_id={} type={}",
            col_stat2.data_bytes,
            col_stat2.nullmap_data_bytes,
            col_stat2.col_id,
            col_stat2.type->getName());

        ASSERT_EQ(
            dmfile1->colDataSize(col_def.id, DMFile::ColDataType::Elements),
            dmfile2->colDataSize(col_def.id, DMFile::ColDataType::Elements));
        ASSERT_EQ(dmfile1->isColIndexExist(col_def.id), dmfile2->isColIndexExist(col_def.id));
        if (dmfile1->isColIndexExist(col_def.id))
        {
            ASSERT_EQ(dmfile1->colIndexSize(col_def.id), dmfile2->colIndexSize(col_def.id));
        }
        if (col_def.type->isNullable())
        {
            ASSERT_EQ(
                dmfile1->colDataSize(col_def.id, DMFile::ColDataType::NullMap),
                dmfile2->colDataSize(col_def.id, DMFile::ColDataType::NullMap));
        }
        if (col_def.type->getTypeId() == TypeIndex::Array)
        {
            ASSERT_EQ(
                dmfile1->colDataSize(col_def.id, DMFile::ColDataType::ArraySizes),
                dmfile2->colDataSize(col_def.id, DMFile::ColDataType::ArraySizes));
        }
    }
}

// test tiny data into v3, and read it
// check all data is in 0.merged and meta
TEST_F(DMFileMetaV2Test, CheckDMFileV3WithTinyData)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 1);
    }


    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }

    {
        // Test file number
        auto dir = dm_file->path();

        std::vector<String> files;
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator itr(dir); itr != end; ++itr)
        {
            if (itr.name() != "NGC" && itr.name() != "meta")
            {
                files.emplace_back(itr.name());
            }
        }
        ASSERT_EQ(files.size(), 1);
        ASSERT(files[0] == "0.merged");
    }

    /// Test restore the file from disk and read
    {
        dm_file = restoreDMFile();

        // Test dt property read success
        auto propertys = dm_file->getPackProperties();
        ASSERT_EQ(propertys.property_size(), 1);
        for (int i = 0; i < propertys.property_size(); i++)
        {
            const auto & property = propertys.property(i);
            ASSERT_EQ((size_t)property.num_rows(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.gc_hint_version(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.deleted_rows(), (size_t)block_propertys[i].deleted_rows);
        }
    }

    {
        // Test read after restore
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }
}
CATCH

// test multiple data  into v3, and read it
TEST_F(DMFileMetaV2Test, CheckDMFileV3WithMultiData)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write * 2000);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 1);
    }

    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write * 2000)),
            }));
    }

    {
        // Test file number
        auto dir = dm_file->path();

        std::set<String> files;
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator itr(dir); itr != end; ++itr)
        {
            if (itr.name() != "NGC" && itr.name() != "meta")
            {
                files.insert(itr.name());
            }
        }
        ASSERT_EQ(files.size(), 3); // handle, col data and merged file
        ASSERT(files.find("0.merged") != files.end());
        ASSERT(files.find("%2D1.dat") != files.end());
        ASSERT(files.find("1.dat") != files.end());
    }

    /// Test restore the file from disk and read
    {
        dm_file = restoreDMFile();

        // Test dt property read success
        auto propertys = dm_file->getPackProperties();
        ASSERT_EQ(propertys.property_size(), 1);
        for (int i = 0; i < propertys.property_size(); i++)
        {
            const auto & property = propertys.property(i);
            ASSERT_EQ((size_t)property.num_rows(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.gc_hint_version(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.deleted_rows(), (size_t)block_propertys[i].deleted_rows);
        }
    }

    {
        // Test read after restore
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write * 2000)),
            }));
    }
}
CATCH

TEST_F(DMFileMetaV2Test, MetaV2)
try
{
    auto check_pack_stats = [](const DMFilePtr & dmfile1, const DMFilePtr & dmfile2) {
        const auto & pack_stats1 = dmfile1->getPackStats();
        const auto & pack_stats2 = dmfile2->getPackStats();
        ASSERT_EQ(pack_stats1.size(), pack_stats2.size());
        for (size_t i = 0; i < pack_stats1.size(); ++i)
        {
            const auto & stat1 = pack_stats1[i];
            const auto & stat2 = pack_stats2[i];
            ASSERT_EQ(stat1.toDebugString(), stat2.toDebugString());
        }
    };

    auto check_pack_properties = [](const DMFilePtr & dmfile1, const DMFilePtr & dmfile2) {
        const auto & properties1 = dmfile1->getPackProperties();
        const auto & properties2 = dmfile2->getPackProperties();
        ASSERT_EQ(properties1.property_size(), properties2.property_size());
        for (int i = 0; i < properties1.property_size(); ++i)
        {
            const auto & p1 = properties1.property(i);
            const auto & p2 = properties2.property(i);
            ASSERT_EQ(p1.ShortDebugString(), p2.ShortDebugString());
        }
    };

    auto check_meta = [&](const DMFilePtr & dmfile1, const DMFilePtr & dmfile2) {
        ASSERT_FALSE(dmfile1->useMetaV2());
        ASSERT_TRUE(dmfile2->useMetaV2());
        check_pack_stats(dmfile1, dmfile2);
        check_pack_properties(dmfile1, dmfile2);
        checkColumnStats(dmfile1, dmfile2);
    };
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    block_property2.deleted_rows = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    DMFilePtr dmfile1, dmfile2;
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);
        auto mode = DMFileMode::DirectoryChecksum;
        auto configuration = createConfiguration(mode);
        dmfile1 = DMFile::create(
            1,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            DMFileFormat::V2);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile1, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();
    }
    {
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);
        auto mode = DMFileMode::DirectoryMetaV2;
        auto configuration = createConfiguration(mode);
        dmfile2 = DMFile::create(
            2,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            DMFileFormat::V3);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile2, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();
    }

    LOG_DEBUG(Logger::get(), "check dmfile1 dmfile2");
    check_meta(dmfile1, dmfile2);

    // Restore MetaV2
    auto file_provider = dbContext().getFileProvider();
    auto dmfile3 = DMFile::restore(
        file_provider,
        dmfile2->fileId(),
        dmfile2->pageId(),
        dmfile2->parentPath(),
        DMFileMeta::ReadMode::all());
    LOG_DEBUG(Logger::get(), "check dmfile1 dmfile3");
    check_meta(dmfile1, dmfile3);

    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dmfile2, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        Block block;
        while ((block = stream->read())) {}
    }
}
CATCH

TEST_F(DMFileMetaV2Test, MetaV2Broken)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    block_property2.deleted_rows = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    DMFilePtr dmfile;

    Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
    Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);
    auto mode = DMFileMode::DirectoryChecksum;
    auto configuration = createConfiguration(mode);
    dmfile = DMFile::create(
        1,
        parent_path,
        std::move(configuration),
        128 * 1024,
        16 * 1024 * 1024,
        NullspaceID,
        DMFileFormat::V3);
    auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile, *cols);
    stream->writePrefix();
    stream->write(block1, block_property1);
    stream->write(block2, block_property2);
    stream->writeSuffix();

    breakFileMetaV2File(dmfile);

    try
    {
        auto file_provider = dbContext().getFileProvider();
        auto t = DMFile::restore(
            file_provider,
            dmfile->fileId(),
            dmfile->pageId(),
            dmfile->parentPath(),
            DMFileMeta::ReadMode::all());
        FAIL(); // Should not come here.
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA) << e.message();
    }
}
CATCH

TEST_F(DMFileMetaV2Test, WriteReadWithArray)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto array_cd = ColumnDefine(100, "array", tests::typeFromString("Array(Float32)"));
    cols->emplace_back(array_cd);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1{
        .not_clean_rows = 0,
        .deleted_rows = 1,
        .effective_num_rows = 1,
        .gc_hint_version = 1,
    };
    DMFileBlockOutputStream::BlockProperty block_property2{
        .not_clean_rows = 0,
        .deleted_rows = 2,
        .effective_num_rows = 2,
        .gc_hint_version = 2,
    };
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys{block_property1, block_property2};

    Array vec1{Field(1.0), Field(2.0), Field(3.0)};
    Array vec2{Field(11.0), Field(12.0), Field(13.0)};
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        block1.insert(
            createVecFloat32Column<Array>(std::vector<Array>(num_rows_write / 2, vec1), array_cd.name, array_cd.id));
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);
        block2.insert(
            createVecFloat32Column<Array>(std::vector<Array>(num_rows_write / 2, vec2), array_cd.name, array_cd.id));
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 2);
    }

    std::vector<Array> expect_arr_values;
    for (size_t i = 0; i < num_rows_write; ++i)
    {
        if (i < num_rows_write / 2)
            expect_arr_values.emplace_back(vec1);
        else
            expect_arr_values.emplace_back(vec2);
    }
    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createVecFloat32Column<Array>(expect_arr_values),
            }));
    }

    /// Test restore the file from disk and read
    {
        dm_file = restoreDMFile();

        // Test dt property read success
        auto propertys = dm_file->getPackProperties();
        ASSERT_EQ(propertys.property_size(), 2);
        for (int i = 0; i < propertys.property_size(); i++)
        {
            const auto & property = propertys.property(i);
            ASSERT_EQ((size_t)property.num_rows(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.gc_hint_version(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.deleted_rows(), (size_t)block_propertys[i].deleted_rows);
        }
    }
    {
        // Test read after restore
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createVecFloat32Column<Array>(expect_arr_values),
            }));
    }
}
CATCH

TEST_F(DMFileMetaV2Test, SeekWithArray)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto array_cd = ColumnDefine(100, "array", tests::typeFromString("Array(Float32)"));
    cols->emplace_back(array_cd);

    const size_t num_rows_write = 8192;

    DMFileBlockOutputStream::BlockProperty block_property1{
        .not_clean_rows = 0,
        .deleted_rows = 1,
        .effective_num_rows = 1,
        .gc_hint_version = 1,
    };
    DMFileBlockOutputStream::BlockProperty block_property2{
        .not_clean_rows = 0,
        .deleted_rows = 2,
        .effective_num_rows = 2,
        .gc_hint_version = 2,
    };
    DMFileBlockOutputStream::BlockProperty block_property3{
        .not_clean_rows = 0,
        .deleted_rows = 3,
        .effective_num_rows = 3,
        .gc_hint_version = 3,
    };
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys{
        block_property1,
        block_property2,
        block_property3,
    };

    Array vec1{Field(1.0), Field(2.0), Field(3.0)};
    Array vec2{Field(11.0), Field(12.0), Field(13.0), Field(14.0)};
    Array vec3{Field(111.0), Field(222.0), Field(333.0), Field(444.0), Field(555.0)};
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 3);
        block1.insert(
            createVecFloat32Column<Array>(std::vector<Array>(num_rows_write / 3, vec1), array_cd.name, array_cd.id));
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 3, num_rows_write * 2 / 3);
        block2.insert(
            createVecFloat32Column<Array>(std::vector<Array>(block2.rows(), vec2), array_cd.name, array_cd.id));
        // Block 3: [128, 192)
        Block block3 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write * 2 / 3, num_rows_write);
        block3.insert(
            createVecFloat32Column<Array>(std::vector<Array>(block3.rows(), vec3), array_cd.name, array_cd.id));
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->write(block3, block_property3);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 3);
    }

    dm_file = restoreDMFile();

    std::vector<Array> expect_arr_values;
    for (size_t i = 0; i < num_rows_write; ++i)
    {
        if (i < num_rows_write / 3)
            expect_arr_values.emplace_back(vec1);
        else if (i < num_rows_write * 2 / 3)
            expect_arr_values.emplace_back(vec2);
        else
            expect_arr_values.emplace_back(vec3);
    }
    {
        // Read all packs
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createVecFloat32Column<Array>(expect_arr_values),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        // Only read pack[0]
        auto read_ids = std::make_shared<IdSet>(IdSet{0});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        std::vector<Array> partial_expect_arr_values;
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin(),
            expect_arr_values.begin() + num_rows_write / 3);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write / 3)),
                createVecFloat32Column<Array>(partial_expect_arr_values),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        // Only read pack[1]
        auto read_ids = std::make_shared<IdSet>(IdSet{1});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        std::vector<Array> partial_expect_arr_values;
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin() + num_rows_write / 3,
            expect_arr_values.begin() + num_rows_write * 2 / 3);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(num_rows_write / 3, num_rows_write * 2 / 3)),
                createVecFloat32Column<Array>(partial_expect_arr_values),
            }));
    }
    {
        /// Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        // Only read pack[2]
        auto read_ids = std::make_shared<IdSet>(IdSet{2});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        std::vector<Array> partial_expect_arr_values;
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin() + num_rows_write * 2 / 3,
            expect_arr_values.end());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(num_rows_write * 2 / 3, num_rows_write)),
                createVecFloat32Column<Array>(partial_expect_arr_values),
            }));
    }
    {
        /// Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        // Only read pack[0, 2]
        auto read_ids = std::make_shared<IdSet>(IdSet{0, 2});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        std::vector<Int64> partial_pk = createNumbers<Int64>(0, num_rows_write / 3);
        std::vector<Int64> pk_pack2 = createNumbers<Int64>(num_rows_write * 2 / 3, num_rows_write);
        partial_pk.insert(partial_pk.end(), pk_pack2.begin(), pk_pack2.end());
        std::vector<Array> partial_expect_arr_values;
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin(),
            expect_arr_values.begin() + num_rows_write / 3);
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin() + num_rows_write * 2 / 3,
            expect_arr_values.end());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(partial_pk),
                createVecFloat32Column<Array>(partial_expect_arr_values),
            }));
    }
    {
        /// Test read after skip
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        skipFirstPack(stream);
        std::vector<Array> partial_expect_arr_values;
        partial_expect_arr_values.insert(
            partial_expect_arr_values.cend(),
            expect_arr_values.begin() + num_rows_write / 3,
            expect_arr_values.end());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, array_cd.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(num_rows_write / 3, num_rows_write)),
                createVecFloat32Column<Array>(partial_expect_arr_values),
            }));
    }
}
CATCH

class DMFileTest
    : public DMFileMetaV2Test
    , public testing::WithParamInterface<DMFileMode>
{
public:
    DMFileTest() = default;

    void SetUp() override
    {
        DB::DM::tests::DMFileMetaV2Test::SetUp();

        auto mode = GetParam();
        auto configuration = createConfiguration(mode);
        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "DMFileTest", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");
        dm_file = DMFile::create(
            1,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            modeToVersion(mode));
        table_columns = std::make_shared<ColumnDefines>();
        column_cache = std::make_shared<ColumnCache>();

        DB::tests::TiFlashTestEnv::disableS3Config();

        reload();
    }
};

TEST_P(DMFileTest, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    block_property2.deleted_rows = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 2);
    }


    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }

    /// Test restore the file from disk and read
    {
        dm_file = restoreDMFile();

        // Test dt property read success
        auto propertys = dm_file->getPackProperties();
        ASSERT_EQ(propertys.property_size(), 2);
        for (int i = 0; i < propertys.property_size(); i++)
        {
            const auto & property = propertys.property(i);
            ASSERT_EQ((size_t)property.num_rows(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.gc_hint_version(), (size_t)block_propertys[i].effective_num_rows);
            ASSERT_EQ((size_t)property.deleted_rows(), (size_t)block_propertys[i].deleted_rows);
        }
    }
    {
        // Test read after restore
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }
}
CATCH

// test seek
TEST_P(DMFileTest, Seek)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 8192;

    DMFileBlockOutputStream::BlockProperty block_property1{
        .not_clean_rows = 0,
        .deleted_rows = 1,
        .effective_num_rows = 1,
        .gc_hint_version = 1,
    };
    DMFileBlockOutputStream::BlockProperty block_property2{
        .not_clean_rows = 0,
        .deleted_rows = 2,
        .effective_num_rows = 2,
        .gc_hint_version = 2,
    };
    DMFileBlockOutputStream::BlockProperty block_property3{
        .not_clean_rows = 0,
        .deleted_rows = 3,
        .effective_num_rows = 3,
        .gc_hint_version = 3,
    };
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    block_propertys.push_back(block_property3);
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 3);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 3, num_rows_write * 2 / 3);
        // Block 3: [128, 192)
        Block block3 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write * 2 / 3, num_rows_write);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->write(block3, block_property3);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 3);
    }
    {
        /// Test read all packs
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        // no pack are used, nothing to be skipped
        auto read_ids = std::make_shared<IdSet>(IdSet{});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_EQ(stream->skipNextBlock(), 0);
        // nothing to be read
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 0)),
            }));
    }
    {
        /// Test read after skip
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        skipFirstPack(stream);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(num_rows_write / 3, num_rows_write)),
            }));
    }
}
CATCH

TEST_P(DMFileTest, GetSkippedRows)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 1024;

    DMFileBlockOutputStream::BlockProperty block_property1{
        .not_clean_rows = 0,
        .deleted_rows = 1,
        .effective_num_rows = 1,
        .gc_hint_version = 1,
    };
    DMFileBlockOutputStream::BlockProperty block_property2{
        .not_clean_rows = 0,
        .deleted_rows = 2,
        .effective_num_rows = 2,
        .gc_hint_version = 2,
    };
    DMFileBlockOutputStream::BlockProperty block_property3{
        .not_clean_rows = 0,
        .deleted_rows = 3,
        .effective_num_rows = 3,
        .gc_hint_version = 3,
    };
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    block_propertys.push_back(block_property3);
    {
        // Prepare for write
        // Block 1: [0, 341)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 3);
        // Block 2: [342, 682)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 3, num_rows_write * 2 / 3);
        // Block 3: [683, 1024)
        Block block3 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write * 2 / 3, num_rows_write);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->write(block3, block_property3);
        stream->writeSuffix();

        ASSERT_EQ(dm_file->getPackProperties().property_size(), 3);
    }
    {
        /// Test read all packs
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        // nothing is skipped
        size_t skip_rows;
        ASSERT_TRUE(stream->getSkippedRows(skip_rows));
        ASSERT_EQ(skip_rows, 0);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto read_ids = std::make_shared<IdSet>(IdSet{});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        // pack[0, 1, 2] are skipped
        size_t skip_rows;
        ASSERT_FALSE(stream->getSkippedRows(skip_rows));
        ASSERT_EQ(skip_rows, num_rows_write);
        // nothing to be read
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, 0)),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto read_ids = std::make_shared<IdSet>(IdSet{2});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        // pack[0, 1] are skipped
        size_t skip_rows;
        ASSERT_TRUE(stream->getSkippedRows(skip_rows));
        ASSERT_EQ(skip_rows, num_rows_write * 2 / 3);
        // pack[2] is read
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(num_rows_write * 2 / 3, num_rows_write)),
            }));
    }
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto read_ids = std::make_shared<IdSet>(IdSet{0, 2});
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(read_ids)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        // nothing is skipped
        size_t skip_rows;
        ASSERT_TRUE(stream->getSkippedRows(skip_rows));
        ASSERT_EQ(skip_rows, 0);
        // pack[0, 2] is read
        std::vector<Int64> expect_values = createNumbers<Int64>(0, num_rows_write / 3);
        std::vector<Int64> pack2_values = createNumbers<Int64>(num_rows_write * 2 / 3, num_rows_write);
        expect_values.insert(expect_values.end(), pack2_values.begin(), pack2_values.end());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(expect_values),
            }));
    }
}
CATCH

TEST_P(DMFileTest, GcFlag)
try
{
    // clean
    auto file_provider = dbContext().getFileProvider();
    auto id = dm_file->fileId();
    dm_file->remove(file_provider);
    dm_file.reset();

    auto mode = GetParam();
    auto configuration = createConfiguration(mode);

    dm_file = DMFile::create(
        id,
        parent_path,
        std::move(configuration),
        128 * 1024,
        16 * 1024 * 1024,
        NullspaceID,
        modeToVersion(mode));
    // Right after created, the fil is not abled to GC and it is ignored by `listAllInPath`
    EXPECT_FALSE(dm_file->canGC());
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto scan_ids = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_TRUE(scan_ids.empty());

    {
        // Write some data and finialize the file
        auto cols = DMTestEnv::getDefaultColumns();
        auto num_rows_write = 128UL;
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }

    // The file remains not able to GC
    ASSERT_FALSE(dm_file->canGC());
    options.only_list_can_gc = false;
    // Now the file can be scaned
    scan_ids = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scan_ids.size(), 1UL);
    EXPECT_EQ(*scan_ids.begin(), id);
    options.only_list_can_gc = true;
    scan_ids = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(scan_ids.empty());

    // After enable GC, the file can be scaned with `can_gc=true`
    dm_file->enableGC();
    ASSERT_TRUE(dm_file->canGC());
    options.only_list_can_gc = false;
    scan_ids = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scan_ids.size(), 1UL);
    EXPECT_EQ(*scan_ids.begin(), id);
    options.only_list_can_gc = true;
    scan_ids = DMFile::listAllInPath(file_provider, parent_path, options);
    ASSERT_EQ(scan_ids.size(), 1UL);
    EXPECT_EQ(*scan_ids.begin(), id);
}
CATCH

/// DMFileTest.InterruptedDrop_0 and InterruptedDrop_1 test that if deleting file
/// is interrupted by accident, we can safely ignore those broken files.

TEST_P(DMFileTest, InterruptedDrop0)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }

    FailPointHelper::enableFailPoint(FailPoints::exception_before_dmfile_remove_encryption);
    auto file_provider = dbContext().getFileProvider();
    try
    {
        dm_file->remove(file_provider);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
            throw;
    }

    // The broken file is ignored
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto res = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(res.empty());
}
CATCH

TEST_P(DMFileTest, InterruptedDrop1)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }

    FailPointHelper::enableFailPoint(FailPoints::exception_before_dmfile_remove_from_disk);
    auto file_provider = dbContext().getFileProvider();
    try
    {
        dm_file->remove(file_provider);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != ErrorCodes::FAIL_POINT_ERROR)
            throw;
    }

    // The broken file is ignored
    DMFile::ListOptions options;
    options.only_list_can_gc = true;
    auto res = DMFile::listAllInPath(file_provider, parent_path, options);
    EXPECT_TRUE(res.empty());
}
CATCH

/// Test reading rows with some filters

TEST_P(DMFileTest, ReadFilteredByHandle)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        size_t pk_beg = 0;

        DMFileBlockOutputStream::BlockProperty block_property;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges{
        HandleRange{0, span_per_part}, // only first part
        HandleRange{800, num_rows_write},
        HandleRange{256, 700}, //
        HandleRange::newNone(), // none
        HandleRange{0, num_rows_write}, // full range
        HandleRange::newAll(), // full range
    };
    auto test_read_range = [&](const HandleRange & range) {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols,
                              RowKeyRanges{RowKeyRange::fromHandleRange(range)},
                              std::make_shared<ScanContext>()); // Filtered by read_range

        Int64 expect_first_pk = static_cast<int>(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(
            num_rows_write, //
            static_cast<int>(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                + (range.end % span_per_part ? span_per_part : 0));
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(expect_first_pk, expect_last_pk)),
            }))
            << fmt::format("range: {}, first: {}, last: {}", range.toDebugString(), expect_first_pk, expect_last_pk);
    };

    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with range:" + range.toDebugString());
        test_read_range(range);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with range:" + range.toDebugString() + " after restoring DTFile");
        test_read_range(range);
    }
}
CATCH

namespace
{
RSOperatorPtr toRSFilter(const ColumnDefine & cd, const HandleRange & range)
{
    Attr attr = {cd.name, cd.id, cd.type};
    auto left = createGreaterEqual(attr, Field(range.start));
    auto right = createLess(attr, Field(range.end));
    return createAnd({left, right});
}
} // namespace

TEST_P(DMFileTest, ReadFilteredByRoughSetFilter)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            size_t pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            block.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(pk_beg, pk_end), i64_cd.name, i64_cd.id));
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    HandleRanges ranges{
        HandleRange{0, span_per_part}, // only first part
        HandleRange{800, num_rows_write},
        HandleRange{256, 700}, //
        HandleRange::newNone(), // none
        HandleRange{0, num_rows_write}, // full range
        HandleRange::newAll(), // full range
    };
    auto test_read_filter = [&](const HandleRange & range) {
        // Filtered by rough set filter
        auto filter = toRSFilter(i64_cd, range);
        const auto read_ranges = RowKeyRanges{RowKeyRange::newAll(false, 1)};
        auto pack_result = std::make_shared<DMFilePackFilterResult>(
            DMFilePackFilter::loadFrom(dmContext(), dm_file, false, read_ranges, filter, {}));
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .setDMFilePackFilterResult(pack_result)
                          .build(dm_file, *cols, read_ranges, std::make_shared<ScanContext>());

        Int64 expect_first_pk = static_cast<int>(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(
            num_rows_write, //
            static_cast<int>(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                + (range.end % span_per_part ? span_per_part : 0));
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(expect_first_pk, expect_last_pk)),
            }))
            << fmt::format("range: {}, first: {}, last: {}", range.toDebugString(), expect_first_pk, expect_last_pk);
    };

    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with filter range:" + range.toDebugString());
        test_read_filter(range);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (const auto & range : ranges)
    {
        SCOPED_TRACE("Test reading with filter range:" + range.toDebugString() + " after restoring DTFile");
        test_read_filter(range);
    }
}
CATCH

// Test rough filter with some unsupported operations
TEST_P(DMFileTest, ReadFilteredByRoughSetFilterWithUnsupportedOperation)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_cd(2, "i64", typeFromString("Int64"));
    cols->push_back(i64_cd);

    reload(cols);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            size_t pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            block.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(pk_beg, pk_end), i64_cd.name, i64_cd.id));
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    std::vector<std::pair<DM::RSOperatorPtr, size_t>> filters;
    DM::RSOperatorPtr one_part_filter = toRSFilter(i64_cd, HandleRange{0, span_per_part});
    // <filter, num_rows_should_read>
    filters.emplace_back(one_part_filter, span_per_part); // only first part
    // <filter, num_rows_should_read>
    // (first range) And (Unsuppported) -> should filter some chunks by range
    filters.emplace_back(createAnd({one_part_filter, createUnsupported("test")}), span_per_part);
    // <filter, num_rows_should_read>
    // (first range) Or (Unsupported) -> should NOT filter any chunk
    filters.emplace_back(createOr({one_part_filter, createUnsupported("test")}), num_rows_write);
    auto test_read_filter = [&](const DM::RSOperatorPtr & filter, const size_t num_rows_should_read) {
        const auto read_ranges = RowKeyRanges{RowKeyRange::newAll(false, 1)};
        auto pack_result = std::make_shared<DMFilePackFilterResult>(
            DMFilePackFilter::loadFrom(dmContext(), dm_file, false, read_ranges, filter, {}));
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .setDMFilePackFilterResult(pack_result)
                          .build(dm_file, *cols, read_ranges, std::make_shared<ScanContext>());

        Int64 expect_first_pk = 0;
        Int64 expect_last_pk = num_rows_should_read;
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(expect_first_pk, expect_last_pk)),
            }))
            << fmt::format("first: {}, last: {}", expect_first_pk, expect_last_pk);
    };

    for (size_t i = 0; i < filters.size(); ++i)
    {
        const auto & filter = filters[i].first;
        const auto num_rows_should_read = filters[i].second;
        SCOPED_TRACE("Test reading with idx: " + DB::toString(i) + ", filter range:" + filter->toDebugString());
        test_read_filter(filter, num_rows_should_read);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (size_t i = 0; i < filters.size(); ++i)
    {
        const auto & filter = filters[i].first;
        const auto num_rows_should_read = filters[i].second;
        SCOPED_TRACE(
            "Test reading with idx: " + DB::toString(i) + ", filter range:" + filter->toDebugString()
            + " after restoring DTFile");
        test_read_filter(filter, num_rows_should_read);
    }
}
CATCH

TEST_P(DMFileTest, ReadFilteredByPackIndices)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(pk_beg, pk_end, false);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    std::vector<IdSet> test_sets;
    test_sets.emplace_back(IdSet{0});
    test_sets.emplace_back(IdSet{nparts - 1});
    test_sets.emplace_back(IdSet{nparts - 2, nparts - 1});
    test_sets.emplace_back(IdSet{1, 2});
    test_sets.emplace_back(IdSet{}); // filter all packs
    auto test_with_case_index = [&](const size_t test_index) {
        IdSetPtr id_set_ptr = nullptr; // Keep for not filter test
        if (test_index < test_sets.size())
            id_set_ptr = std::make_shared<IdSet>(test_sets[test_index]);

        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .setReadPacks(id_set_ptr) // filter by pack index
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());

        Int64 expect_first_pk = 0;
        Int64 expect_last_pk = 0;
        if (id_set_ptr && !id_set_ptr->empty())
        {
            expect_first_pk = *(id_set_ptr->begin()) * span_per_part;
            auto last_id = *(id_set_ptr->rbegin());
            expect_last_pk = (last_id == nparts - 1) ? num_rows_write : (last_id + 1) * span_per_part;
        }
        else if (!id_set_ptr)
        {
            // not filter if it is nullptr
            expect_last_pk = num_rows_write;
        }
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(expect_first_pk, expect_last_pk)),
            }))
            << fmt::format("test index: {}, first: {}, last: {}", test_index, expect_first_pk, expect_last_pk);
    };
    for (size_t test_index = 0; test_index <= test_sets.size(); test_index++)
    {
        SCOPED_TRACE("Test reading with index:" + DB::toString(test_index));
        test_with_case_index(test_index);
    }

    // Restore file from disk and read again
    dm_file = restoreDMFile();
    for (size_t test_index = 0; test_index <= test_sets.size(); test_index++)
    {
        SCOPED_TRACE("Test reading with index:" + DB::toString(test_index) + " after restoring DTFile");
        test_with_case_index(test_index);
    }
}
CATCH

/// Test reading different column types

TEST_P(DMFileTest, NumberTypes)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine i64_col(2, "i64", typeFromString("Int64"));
    ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
    cols->push_back(i64_col);
    cols->push_back(f64_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        block.insert(DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows_write), i64_col.name, i64_col.id));
        block.insert(
            DB::tests::createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125), f64_col.name, f64_col.id));

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test Read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, i64_col.name, f64_col.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125)),
            }));
    }
}
CATCH

TEST_P(DMFileTest, StringType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    // Prepare columns
    ColumnDefine fixed_str_col(2, "str", typeFromString(DataTypeString::getDefaultName()));
    cols->push_back(fixed_str_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        block.insert(ColumnWithTypeAndName{
            DB::tests::makeColumn<String>(fixed_str_col.type, Strings(num_rows_write, "hello")),
            fixed_str_col.type,
            fixed_str_col.name,
            fixed_str_col.id});

        auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test Read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, fixed_str_col.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<String>(std::vector<String>(num_rows_write, "hello")),
            }));
    }
}
CATCH

TEST_P(DMFileTest, NullableType)
try
{
    auto cols = DMTestEnv::getDefaultColumns();
    ColumnDefine nullable_col(2, "i32_null", typeFromString("Nullable(Int32)"));
    // Prepare columns
    cols->emplace_back(nullable_col);

    reload(cols);

    const size_t num_rows_write = 128;
    {
        // Prepare write
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // Half of the column are filled by NULL
        auto col = nullable_col.type->createColumn();
        for (size_t i = 0; i < num_rows_write / 2; i++)
            col->insert(toField(static_cast<Int64>(i)));
        for (size_t i = 64; i < num_rows_write; i++)
            col->insertDefault();
        block.insert(ColumnWithTypeAndName{std::move(col), nullable_col.type, nullable_col.name, nullable_col.id});

        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block, block_property);
        stream->writeSuffix();
    }

    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.setColumnCache(column_cache)
                  .build(dm_file, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        std::vector<Int64> nullable_coldata = createNumbers<Int64>(0, num_rows_write / 2);
        nullable_coldata.resize(num_rows_write);
        std::vector<Int32> null_map(num_rows_write, 0);
        std::fill(null_map.begin() + num_rows_write / 2, null_map.end(), 1);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, nullable_col.name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createNullableColumn<Int32>(nullable_coldata, null_map),
            }));
    }
}
CATCH


INSTANTIATE_TEST_CASE_P(
    DTFileMode, //
    DMFileTest,
    testing::Values(DMFileMode::DirectoryLegacy, DMFileMode::DirectoryChecksum, DMFileMode::DirectoryMetaV2),
    paramToString);


/// DMFile test for clustered index
class DMFileClusteredIndexTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<DMFileMode>
{
public:
    DMFileClusteredIndexTest()
        : dm_file(nullptr)
    {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        path = TiFlashStorageTestBasic::getTemporaryPath();

        auto mode = GetParam();
        auto configuration = createConfiguration(mode);

        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, table_id, *path_pool, "test.t1");
        dm_file = DMFile::create(
            0,
            path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            modeToVersion(mode));
        table_columns = std::make_shared<ColumnDefines>();
        column_cache = std::make_shared<ColumnCache>();

        reload();
    }

    // Update dm_context.
    void reload(ColumnDefinesPtr cols = {})
    {
        TiFlashStorageTestBasic::reload();
        if (!cols)
            cols = DMTestEnv::getDefaultColumns(
                is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

        *table_columns = *cols;

        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            is_common_handle,
            rowkey_column_size,
            db_context->getSettingsRef());
    }


    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    String path;
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DeltaMergeStore::Settings settings;

protected:
    DMFilePtr dm_file;
    ColumnCachePtr column_cache;
    TableID table_id = 1;
    bool is_common_handle = true;
    size_t rowkey_column_size = 2;
};

TEST_P(DMFileClusteredIndexTest, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns(
        is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

    const size_t num_rows_write = 128;

    {
        // Prepare for write
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(
            0,
            num_rows_write / 2,
            false,
            2,
            EXTRA_HANDLE_COLUMN_NAME,
            EXTRA_HANDLE_COLUMN_ID,
            EXTRA_HANDLE_COLUMN_STRING_TYPE,
            is_common_handle,
            rowkey_column_size);
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(
            num_rows_write / 2,
            num_rows_write,
            false,
            2,
            EXTRA_HANDLE_COLUMN_NAME,
            EXTRA_HANDLE_COLUMN_ID,
            EXTRA_HANDLE_COLUMN_STRING_TYPE,
            is_common_handle,
            rowkey_column_size);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        stream->write(block1, block_property);
        stream->write(block2, block_property);
        stream->writeSuffix();
    }


    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols,
                              RowKeyRanges{RowKeyRange::newAll(is_common_handle, rowkey_column_size)},
                              std::make_shared<ScanContext>());
        // mock common handle
        auto common_handle_coldata = [this]() {
            std::vector<Int64> int_coldata = createNumbers<Int64>(0, num_rows_write);
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) {
                return genMockCommonHandle(v, rowkey_column_size);
            });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), num_rows_write);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

TEST_P(DMFileClusteredIndexTest, ReadFilteredByHandle)
try
{
    auto cols = DMTestEnv::getDefaultColumns(
        is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);

    const Int64 num_rows_write = 1024;
    const Int64 nparts = 5;
    const Int64 span_per_part = num_rows_write / nparts;

    {
        // Prepare some packs in DMFile
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);

        DMFileBlockOutputStream::BlockProperty block_property;
        stream->writePrefix();
        size_t pk_beg = 0;
        for (size_t i = 0; i < nparts; ++i)
        {
            auto pk_end = (i == nparts - 1) ? num_rows_write : (pk_beg + num_rows_write / nparts);
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                pk_beg,
                pk_end,
                false,
                2,
                EXTRA_HANDLE_COLUMN_NAME,
                EXTRA_HANDLE_COLUMN_ID,
                EXTRA_HANDLE_COLUMN_STRING_TYPE,
                is_common_handle,
                rowkey_column_size);
            stream->write(block, block_property);
            pk_beg += num_rows_write / nparts;
        }
        stream->writeSuffix();
    }

    struct QueryRangeInfo
    {
        QueryRangeInfo(const RowKeyRange & range_, Int64 start_, Int64 end_)
            : range(range_)
            , start(start_)
            , end(end_)
        {}
        RowKeyRange range;
        Int64 start, end;
    };
    std::vector<QueryRangeInfo> ranges{
        QueryRangeInfo{
            DMTestEnv::getRowKeyRangeForClusteredIndex(0, span_per_part, rowkey_column_size),
            0,
            span_per_part // only first part
        },
        QueryRangeInfo{
            DMTestEnv::getRowKeyRangeForClusteredIndex(800, num_rows_write, rowkey_column_size),
            800,
            num_rows_write},
        QueryRangeInfo{DMTestEnv::getRowKeyRangeForClusteredIndex(256, 700, rowkey_column_size), 256, 700},
        QueryRangeInfo{DMTestEnv::getRowKeyRangeForClusteredIndex(0, 0, rowkey_column_size), 0, 0}, //none
        QueryRangeInfo{
            DMTestEnv::getRowKeyRangeForClusteredIndex(0, num_rows_write, rowkey_column_size),
            0,
            num_rows_write,
        }, // full range
        QueryRangeInfo{
            DMTestEnv::getRowKeyRangeForClusteredIndex(
                std::numeric_limits<Int64>::min(),
                std::numeric_limits<Int64>::max(),
                rowkey_column_size),
            std::numeric_limits<Int64>::min(),
            std::numeric_limits<Int64>::max(),
        }, // full range
    };

    for (const auto & range : ranges)
    {
        // Test read
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols,
                              RowKeyRanges{range.range},
                              std::make_shared<ScanContext>()); // Filtered by read_range
        Int64 expect_first_pk = static_cast<int>(std::floor(std::max(0, range.start) / span_per_part)) * span_per_part;
        Int64 expect_last_pk = std::min(
            num_rows_write, //
            static_cast<int>(std::ceil(std::min(num_rows_write, range.end) / span_per_part)) * span_per_part
                + (range.end % span_per_part ? span_per_part : 0));
        // mock common handle
        auto common_handle_coldata = [this, expect_first_pk, expect_last_pk]() {
            std::vector<Int64> int_coldata = createNumbers<Int64>(expect_first_pk, expect_last_pk);
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) {
                return genMockCommonHandle(v, rowkey_column_size);
            });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), expect_last_pk - expect_first_pk);
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }))
            << fmt::format(
                   "range: {}, first: {}, last: {}",
                   range.range.toDebugString(),
                   expect_first_pk,
                   expect_last_pk);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    DTFileMode, //
    DMFileClusteredIndexTest,
    testing::Values(DMFileMode::DirectoryLegacy, DMFileMode::DirectoryChecksum, DMFileMode::DirectoryMetaV2),
    paramToString);

/// DDL test cases
class DMFileDDLTest : public DMFileTest
{
public:
    /// Write some data into DMFile.
    /// return rows write, schema
    std::pair<size_t, ColumnDefines> prepareSomeDataToDMFile(bool i8_is_nullable = false)
    {
        size_t num_rows_write = 128;
        auto cols_before_ddl = DMTestEnv::getDefaultColumns();

        ColumnDefine i8_col(2, "i8", i8_is_nullable ? typeFromString("Nullable(Int8)") : typeFromString("Int8"));
        ColumnDefine f64_col(3, "f64", typeFromString("Float64"));
        cols_before_ddl->push_back(i8_col);
        cols_before_ddl->push_back(f64_col);

        reload(cols_before_ddl);

        {
            // Prepare write
            Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
            if (!i8_is_nullable)
            {
                auto i8_coldata = createSignedNumbers(0, num_rows_write);
                block.insert(createColumn<Int8>(i8_coldata, i8_col.name, i8_col.id));
            }
            else
            {
                auto c = getExpectedI8Column(num_rows_write);
                c.name = i8_col.name;
                c.column_id = i8_col.id;
                block.insert(c);
            }

            block.insert(DB::tests::createColumn<Float64>(
                std::vector<Float64>(num_rows_write, 0.125),
                f64_col.name,
                f64_col.id));

            auto stream = std::make_unique<DMFileBlockOutputStream>(dbContext(), dm_file, *cols_before_ddl);
            DMFileBlockOutputStream::BlockProperty block_property;
            stream->writePrefix();
            stream->write(block, block_property);
            stream->writeSuffix();

            return {num_rows_write, *cols_before_ddl};
        }
    }

    static ColumnWithTypeAndName getExpectedI8Column(size_t num_rows_write)
    {
        auto i8_coldata = createSignedNumbers(0, num_rows_write);
        std::vector<Int32> nullmap(num_rows_write, 0);
        for (size_t i = 0; i < num_rows_write / 2; ++i)
        {
            i8_coldata[i] = 0;
            nullmap[i] = 1;
        }
        return createNullableColumn<Int8>(i8_coldata, nullmap);
    }
};

TEST_P(DMFileDDLTest, AddColumn)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we add new column after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    // A new string column
    ColumnDefine new_s_col(100, "s", typeFromString(DataTypeString::getDefaultName()));
    cols_after_ddl->emplace_back(new_s_col);
    // A new int64 column with default value 5
    ColumnDefine new_i_col_with_default(101, "i", typeFromString("Int64"));
    new_i_col_with_default.default_value = Field(static_cast<Int64>(5));
    cols_after_ddl->emplace_back(new_i_col_with_default);

    {
        // Test read with new columns after ddl
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols_after_ddl,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({"i8", "f64", new_s_col.name, new_i_col_with_default.name}),
            createColumns({
                // old cols
                createColumn<Int8>(createSignedNumbers(0, num_rows_write)),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125)),
                // new cols
                createColumn<String>(Strings(num_rows_write, "")), // filled with empty
                createColumn<Int64>(std::vector<Int64>(num_rows_write, 5)), // filled with default value
            }));
    }
}
CATCH

TEST_P(DMFileDDLTest, UpcastColumnType)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> int32, and its name to "i8_new" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Int32");
    new_col.name = "i32_new";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols_after_ddl,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({new_col.name, "f64"}),
            createColumns({
                createColumn<Int32>(createSignedNumbers(0, num_rows_write)),
                // old cols
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125)),
            }));
    }
}
CATCH

TEST_P(DMFileDDLTest, NotNullToNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile();

    // Mock that we achange a column type from int8 -> Nullable(int32), and its name to "i8_new" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>(cols_before_ddl);
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Int8")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Nullable(Int32)");
    new_col.name = "i32_nullable";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols_after_ddl,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({new_col.name, "f64"}),
            createColumns({
                createNullableColumn<Int32>(
                    createSignedNumbers(0, num_rows_write),
                    /*null_map=*/std::vector<Int32>(num_rows_write, 0)),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125)),
            }));
    }
}
CATCH

TEST_P(DMFileDDLTest, NullToNotNull)
try
{
    // Prepare some data before ddl
    const auto [num_rows_write, cols_before_ddl] = prepareSomeDataToDMFile(true);

    // Mock that we achange a column type from Nullable(int8) -> int32, and its name to "i32" after ddl
    auto cols_after_ddl = std::make_shared<ColumnDefines>();
    *cols_after_ddl = cols_before_ddl;
    const ColumnDefine old_col = cols_before_ddl[3];
    ASSERT_TRUE(old_col.type->equals(*typeFromString("Nullable(Int8)")));
    ColumnDefine new_col = old_col;
    new_col.type = typeFromString("Int32");
    new_col.name = "i32";
    (*cols_after_ddl)[3] = new_col;

    {
        // Test read with new columns after ddl
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.setColumnCache(column_cache)
                          .build(
                              dm_file,
                              *cols_after_ddl,
                              RowKeyRanges{RowKeyRange::newAll(false, 1)},
                              std::make_shared<ScanContext>());

        auto i32_coldata = createSignedNumbers(0, num_rows_write);
        for (size_t i = 0; i < num_rows_write / 2; ++i)
            i32_coldata[i] = 0;
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name, new_col.name, "f64"}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                createColumn<Int32>(i32_coldata),
                createColumn<Float64>(std::vector<Float64>(num_rows_write, 0.125)),
            }));
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    DTFileMode, //
    DMFileDDLTest,
    testing::Values(DMFileMode::DirectoryLegacy, DMFileMode::DirectoryChecksum, DMFileMode::DirectoryMetaV2),
    paramToString);

} // namespace DB::DM::tests
