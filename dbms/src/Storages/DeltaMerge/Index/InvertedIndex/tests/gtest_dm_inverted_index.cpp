// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyLocalIndexWriter.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileLocalIndexWriter.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromDMFile.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <gtest/gtest.h>


namespace DB::DM::tests
{

class InvertedIndexDMFileTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");
        auto delegator = path_pool->getStableDiskDelegator();
        auto paths = delegator.listPaths();
        RUNTIME_CHECK(paths.size() == 1);
        dm_file = DMFile::create(
            1,
            paths[0],
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            DMFileFormat::V3);

        DB::tests::TiFlashTestEnv::disableS3Config();

        reload();
    }

    // Update dm_context.
    void reload()
    {
        TiFlashStorageTestBasic::reload();

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
        auto dmfile_parent_path = dm_file->parentPath();
        auto dmfile = DMFile::restore(
            dbContext().getFileProvider(),
            dm_file->fileId(),
            dm_file->pageId(),
            dmfile_parent_path,
            DMFileMeta::ReadMode::all(),
            /* meta_version= */ 0);
        auto delegator = path_pool->getStableDiskDelegator();
        delegator.addDTFile(dm_file->fileId(), dmfile->getBytesOnDisk(), dmfile_parent_path);
        return dmfile;
    }

    DMFilePtr buildIndex(const LocalIndexInfosSnapshot & index_infos)
    {
        auto build_info = DMFileLocalIndexWriter::getLocalIndexBuildInfo(index_infos, {dm_file});
        DMFileLocalIndexWriter iw(DMFileLocalIndexWriter::Options{
            .path_pool = path_pool,
            .index_infos = build_info.indexes_to_build,
            .dm_files = {dm_file},
            .dm_context = *dm_context,
        });
        auto new_dmfiles = iw.build();
        assert(new_dmfiles.size() == 1);
        return new_dmfiles[0];
    }

    Context & dbContext() { return *db_context; }

protected:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    DMFilePtr dm_file = nullptr;

public:
    InvertedIndexDMFileTest() = default;

protected:
    static LocalIndexInfosSnapshot indexInfo(
        ColumnID col_id,
        TiDB::InvertedIndexDefinition definition = TiDB::InvertedIndexDefinition{
            .is_signed = false,
            .type_size = 8,
        })
    {
        const LocalIndexInfos index_infos = LocalIndexInfos{
            LocalIndexInfo{
                1,
                col_id,
                std::make_shared<TiDB::InvertedIndexDefinition>(definition),
            },
        };
        return std::make_shared<const LocalIndexInfos>(index_infos);
    }
};

TEST_F(InvertedIndexDMFileTest, SingleIndex)
try
{
    const ColumnID integer_column_id = 100;
    const String integer_column_name = "integer_column";
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto integer_cd = ColumnDefine(integer_column_id, integer_column_name, tests::typeFromString("UInt64"));
    auto definition = std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
        .is_signed = false,
        .type_size = 8,
    });
    const LocalIndexInfos index_infos = LocalIndexInfos{LocalIndexInfo{1, integer_column_id, definition}};
    const auto snapshot = std::make_shared<const LocalIndexInfos>(index_infos);
    cols->emplace_back(integer_cd);

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(integer_column_id);
        used_indexes.Add(std::move(columnar_info));
    }

    // Prepare DMFile
    {
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
            block.insert(createColumn<UInt64>({1, 2, 3}, integer_cd.name, integer_cd.id));
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
            block.insert(createColumn<UInt64>({4, 5, 6}, integer_cd.name, integer_cd.id));
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(6, 9);
            block.insert(createColumn<UInt64>({7, 8, 9}, integer_cd.name, integer_cd.id));
            // delete the 7th row, but InvertedIndexReaderFromDMFile should not care about it
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 1, 0, 0});
        }
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();
    dm_file = buildIndex(snapshot);

    // test col > 1
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "011111111");
    }
    // test col < 7
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createLess(attr, Field(static_cast<UInt64>(7)));
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "111111000");
    }
    // test col = 6
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createEqual(attr, Field(static_cast<UInt64>(6)));
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "000001000");
    }
    // test col > 1 and col < 10
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createLess(attr, Field(static_cast<UInt64>(10)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "011111111");
    }
    // test col > 5 and unsupported
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(5)));
        auto rs_operator2 = createUnsupported("unsupported");
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "000001111");
    }
    // test col > 3 or unsupported
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createUnsupported("unsupported");
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "111111111");
    }
}
CATCH

TEST_F(InvertedIndexDMFileTest, MultipleIndex)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto integer_cd_1 = ColumnDefine(100, "UInt64", tests::typeFromString("UInt64"));
    auto integer_cd_2 = ColumnDefine(101, "Int64", tests::typeFromString("Int64"));
    cols->emplace_back(integer_cd_1);
    cols->emplace_back(integer_cd_2);
    const LocalIndexInfos index_infos = LocalIndexInfos{
        LocalIndexInfo{
            1,
            integer_cd_1.id,
            std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
                .is_signed = false,
                .type_size = 8,
            }),
        },
        LocalIndexInfo{
            2,
            integer_cd_2.id,
            std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
                .is_signed = true,
                .type_size = 8,
            }),
        },
    };
    const auto snapshot = std::make_shared<const LocalIndexInfos>(index_infos);

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(integer_cd_1.id);
        used_indexes.Add(std::move(columnar_info));
    }
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(2);
        inverted->set_column_id(integer_cd_2.id);
        used_indexes.Add(std::move(columnar_info));
    }

    // Prepare DMFile
    {
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, *cols);
        stream->writePrefix();
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
            block.insert(createColumn<UInt64>({1, 2, 3}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({1, 2, 3}, integer_cd_2.name, integer_cd_2.id));
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
            block.insert(createColumn<UInt64>({4, 5, 6}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({4, 5, 6}, integer_cd_2.name, integer_cd_2.id));
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(6, 9);
            block.insert(createColumn<UInt64>({7, 8, 9}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({7, 8, 9}, integer_cd_2.name, integer_cd_2.id));
            // delete the 7th row, but InvertedIndexReaderFromDMFile should not care about it
            stream->write(block, DMFileBlockOutputStream::BlockProperty{0, 1, 0, 0});
        }
        stream->writeSuffix();
    }

    dm_file = restoreDMFile();
    dm_file = buildIndex(snapshot);

    // test col_1 > 1 and col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "011111000");
    }
    // test col_1 > 5 or col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(5)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "111111111");
    }
    // test col_1 > 10 and col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(10)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "000000000");
    }
    // test col_1 < 1 or col_2 > 1
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createLess(attr_1, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createGreater(attr_2, Field(static_cast<Int64>(1)));
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "011111111");
    }
    // test col_1 < 8 and col_2 = 6 and unsupported
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createLess(attr_1, Field(static_cast<UInt64>(8)));
        auto rs_operator2 = createEqual(attr_2, Field(static_cast<UInt64>(6)));
        auto rs_operator3 = createUnsupported("unsupported");
        auto rs_operator = createAnd({rs_operator1, rs_operator2, rs_operator3});
        auto column_range = rs_operator->buildSets(used_indexes);

        InvertedIndexReaderFromDMFile reader(column_range, dm_file, dbContext().getLightLocalIndexCache());
        auto bitmap = reader.load();
        ASSERT_EQ(bitmap->size(), 9);
        ASSERT_EQ(bitmap->toDebugString(), "000001000");
    }
}
CATCH

class InvertedIndexColumnFileTinyTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        TiFlashStorageTestBasic::SetUp();

        auto & global_context = TiFlashTestEnv::getGlobalContext();
        global_context.getTMTContext().initS3GCManager(nullptr);

        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store == nullptr);
        global_context.getSharedContextDisagg()->initRemoteDataStore(
            global_context.getFileProvider(),
            /*s3_enabled*/ true);
        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store != nullptr);

        ASSERT_TRUE(global_context.tryGetWriteNodePageStorage() == nullptr);
        orig_mode = global_context.getPageStorageRunMode();
        global_context.setPageStorageRunMode(PageStorageRunMode::UNI_PS);
        global_context.tryReleaseWriteNodePageStorageForTest();
        global_context.initializeWriteNodePageStorageIfNeed(global_context.getPathPool());

        reload();
    }

    // Update dm_context.
    void reload()
    {
        TiFlashStorageTestBasic::reload();

        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        page_id_allocator = std::make_shared<GlobalPageIdAllocator>();
        storage_pool
            = std::make_shared<StoragePool>(*db_context, NullspaceID, ns_id, *path_pool, page_id_allocator, "test.t1");
        storage_pool->restore();

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

    void TearDown() override
    {
        auto & global_context = TiFlashTestEnv::getGlobalContext();
        global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        global_context.setPageStorageRunMode(orig_mode);

        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

    ColumnFileSetSnapshotPtr getSnapshot()
    {
        auto storage_snap = std::make_shared<StorageSnapshot>(
            *storage_pool,
            dm_context->getReadLimiter(),
            dm_context->tracing_id,
            /*snapshot_read*/ true);
        auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);
        return persisted_set->createSnapshot(data_from_storage_snap);
    }

    ColumnFileTinys buildIndex(
        const ColumnFileSetSnapshotPtr & persisted_files_snap,
        const LocalIndexInfosSnapshot & index_infos)
    {
        auto build_info = ColumnFileTinyLocalIndexWriter::getLocalIndexBuildInfo(index_infos, {persisted_set});
        WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());
        ColumnFileTinyLocalIndexWriter iw(ColumnFileTinyLocalIndexWriter::Options{
            .storage_pool = storage_pool,
            .write_limiter = dm_context->global_context.getWriteLimiter(),
            .files = persisted_files_snap->getColumnFiles(),
            .data_provider = persisted_files_snap->getDataProvider(),
            .index_infos = build_info.indexes_to_build,
            .wbs = wbs,
        });
        auto new_files = iw.build([]() { return true; });
        wbs.writeAll();
        return new_files;
    }

    Context & dbContext() { return *db_context; }

protected:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;

    NamespaceID ns_id = 100;

    ColumnFilePersistedSetPtr persisted_set = nullptr;

    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;

public:
    InvertedIndexColumnFileTinyTest() = default;

protected:
    static LocalIndexInfosSnapshot indexInfo(
        ColumnID col_id,
        TiDB::InvertedIndexDefinition definition = TiDB::InvertedIndexDefinition{
            .is_signed = false,
            .type_size = 8,
        })
    {
        const LocalIndexInfos index_infos = LocalIndexInfos{
            LocalIndexInfo{
                1,
                col_id,
                std::make_shared<TiDB::InvertedIndexDefinition>(definition),
            },
        };
        return std::make_shared<const LocalIndexInfos>(index_infos);
    }
};

TEST_F(InvertedIndexColumnFileTinyTest, SingleIndex)
try
{
    const ColumnID integer_column_id = 100;
    const String integer_column_name = "integer_column";
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto integer_cd = ColumnDefine(integer_column_id, integer_column_name, tests::typeFromString("UInt64"));
    auto definition = std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
        .is_signed = false,
        .type_size = 8,
    });
    const LocalIndexInfos index_infos = LocalIndexInfos{LocalIndexInfo{1, integer_column_id, definition}};
    const auto snapshot = std::make_shared<const LocalIndexInfos>(index_infos);
    cols->emplace_back(integer_cd);

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(integer_column_id);
        used_indexes.Add(std::move(columnar_info));
    }

    // Prepare ColumnFilePersistedSet
    ColumnFilePersisteds persisted_files;
    {
        WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
            block.insert(createColumn<UInt64>({1, 2, 3}, integer_cd.name, integer_cd.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
            block.insert(createColumn<UInt64>({4, 5, 6}, integer_cd.name, integer_cd.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(6, 9);
            block.insert(createColumn<UInt64>({7, 8, 9}, integer_cd.name, integer_cd.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        wbs.writeAll();
    }

    persisted_set = std::make_shared<ColumnFilePersistedSet>(1, persisted_files);
    auto persisted_files_snap = getSnapshot();
    auto tiny_files = buildIndex(persisted_files_snap, snapshot);
    // New pages are created, so we need to get a new snapshot.
    persisted_files_snap = getSnapshot();

    // test col > 1
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "011111111");
    }
    // test col < 7
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createLess(attr, Field(static_cast<UInt64>(7)));
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "111111000");
    }
    // test col = 6
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator = createEqual(attr, Field(static_cast<UInt64>(6)));
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "000001000");
    }
    // test col > 1 and col < 10
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createLess(attr, Field(static_cast<UInt64>(10)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "011111111");
    }
    // test col > 5 and unsupported
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(5)));
        auto rs_operator2 = createUnsupported("unsupported");
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "000001111");
    }
    // test col > 3 or unsupported
    {
        Attr attr{.col_name = integer_cd.name, .col_id = integer_cd.id, .type = integer_cd.type};
        auto rs_operator1 = createGreater(attr, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createUnsupported("unsupported");
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "111111111");
    }
}
CATCH

TEST_F(InvertedIndexColumnFileTinyTest, MultipleIndex)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto integer_cd_1 = ColumnDefine(100, "UInt64", tests::typeFromString("UInt64"));
    auto integer_cd_2 = ColumnDefine(101, "Int64", tests::typeFromString("Int64"));
    cols->emplace_back(integer_cd_1);
    cols->emplace_back(integer_cd_2);
    const LocalIndexInfos index_infos = LocalIndexInfos{
        LocalIndexInfo{
            1,
            integer_cd_1.id,
            std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
                .is_signed = false,
                .type_size = 8,
            }),
        },
        LocalIndexInfo{
            2,
            integer_cd_2.id,
            std::make_shared<TiDB::InvertedIndexDefinition>(TiDB::InvertedIndexDefinition{
                .is_signed = true,
                .type_size = 8,
            }),
        },
    };
    const auto snapshot = std::make_shared<const LocalIndexInfos>(index_infos);

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(1);
        inverted->set_column_id(integer_cd_1.id);
        used_indexes.Add(std::move(columnar_info));
    }
    {
        auto columnar_info = tipb::ColumnarIndexInfo();
        columnar_info.set_index_type(tipb::ColumnarIndexType::TypeInverted);
        auto * inverted = columnar_info.mutable_invert_query_info();
        inverted->set_index_id(2);
        inverted->set_column_id(integer_cd_2.id);
        used_indexes.Add(std::move(columnar_info));
    }

    // Prepare ColumnFilePersistedSet
    ColumnFilePersisteds persisted_files;
    {
        WriteBatches wbs(*storage_pool, dm_context->getWriteLimiter());
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);
            block.insert(createColumn<UInt64>({1, 2, 3}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({1, 2, 3}, integer_cd_2.name, integer_cd_2.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(3, 6);
            block.insert(createColumn<UInt64>({4, 5, 6}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({4, 5, 6}, integer_cd_2.name, integer_cd_2.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        {
            Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(6, 9);
            block.insert(createColumn<UInt64>({7, 8, 9}, integer_cd_1.name, integer_cd_1.id));
            block.insert(createColumn<Int64>({7, 8, 9}, integer_cd_2.name, integer_cd_2.id));
            auto file = ColumnFileTiny::writeColumnFile(*dm_context, block, 0, 3, wbs);
            persisted_files.emplace_back(std::move(file));
        }
        wbs.writeAll();
    }

    persisted_set = std::make_shared<ColumnFilePersistedSet>(1, persisted_files);
    auto persisted_files_snap = getSnapshot();
    auto tiny_files = buildIndex(persisted_files_snap, snapshot);
    // New pages are created, so we need to get a new snapshot.
    persisted_files_snap = getSnapshot();

    // test col_1 > 1 and col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "011111000");
    }
    // test col_1 > 5 or col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(5)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "111111111");
    }
    // test col_1 > 10 and col_2 < 7
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createGreater(attr_1, Field(static_cast<UInt64>(10)));
        auto rs_operator2 = createLess(attr_2, Field(static_cast<Int64>(7)));
        auto rs_operator = createAnd({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "000000000");
    }
    // test col_1 < 1 or col_2 > 1
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createLess(attr_1, Field(static_cast<UInt64>(1)));
        auto rs_operator2 = createGreater(attr_2, Field(static_cast<Int64>(1)));
        auto rs_operator = createOr({rs_operator1, rs_operator2});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "011111111");
    }
    // test col_1 < 8 and col_2 = 6 and unsupported
    {
        Attr attr_1{.col_name = integer_cd_1.name, .col_id = integer_cd_1.id, .type = integer_cd_1.type};
        Attr attr_2{.col_name = integer_cd_2.name, .col_id = integer_cd_2.id, .type = integer_cd_2.type};
        auto rs_operator1 = createLess(attr_1, Field(static_cast<UInt64>(8)));
        auto rs_operator2 = createEqual(attr_2, Field(static_cast<UInt64>(6)));
        auto rs_operator3 = createUnsupported("unsupported");
        auto rs_operator = createAnd({rs_operator1, rs_operator2, rs_operator3});
        auto column_range = rs_operator->buildSets(used_indexes);

        BitmapFilter bitmap_filter(0, false);
        for (const auto & tiny_file : tiny_files)
        {
            InvertedIndexReaderFromColumnFileTiny reader(
                column_range,
                *tiny_file,
                persisted_files_snap->getDataProvider(),
                dbContext().getLightLocalIndexCache());
            auto bitmap = reader.load();
            ASSERT_TRUE(bitmap != nullptr);
            bitmap_filter.append(*bitmap);
        }
        ASSERT_EQ(bitmap_filter.size(), 9);
        ASSERT_EQ(bitmap_filter.toDebugString(), "000001000");
    }
}
CATCH

} // namespace DB::DM::tests
