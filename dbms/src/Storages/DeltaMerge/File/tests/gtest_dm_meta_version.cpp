// Copyright 2024 PingCAP, Inc.
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

#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileLocalStaging.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <Common/TiFlashMetrics.h>
#include <gtest/gtest.h>

#include <memory>
#include <unordered_set>


namespace DB::DM::tests
{

class DMFileMetaVersionTestBase : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        if (enable_encryption)
        {
            KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(true);
            file_provider_maybe_encrypted = std::make_shared<FileProvider>(key_manager, true);
        }
        else
        {
            file_provider_maybe_encrypted = db_context->getFileProvider();
        }

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        db_context->setFileProvider(file_provider_maybe_encrypted);
        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
    }

protected:
    DMFilePtr prepareDMFile(UInt64 file_id)
    {
        auto dm_file = DMFile::create(
            file_id,
            parent_path,
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            DMFileFormat::V3);

        auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
        Block block = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, 3);

        auto writer = DMFileWriter(
            dm_file,
            *cols,
            file_provider_maybe_encrypted,
            db_context->getWriteLimiter(),
            DMFileWriter::Options());
        writer.write(block, DMFileBlockOutputStream::BlockProperty{0, 0, 0, 0});
        writer.finalize();

        return dm_file;
    }

    bool enable_encryption = true;

    const KeyspaceID keyspace_id = NullspaceID;
    const TableID table_id = 100;

    std::shared_ptr<StoragePathPool> path_pool{};
    FileProviderPtr file_provider_maybe_encrypted{};
    String parent_path;
};

class LocalDMFile
    : public DMFileMetaVersionTestBase
    , public testing::WithParamInterface<bool>
{
public:
    LocalDMFile() { enable_encryption = GetParam(); }
};

INSTANTIATE_TEST_CASE_P( //
    DMFileMetaVersion,
    LocalDMFile,
    /* enable_encryption */ ::testing::Bool());

TEST_P(LocalDMFile, WriteWithOldMetaVersion)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);
    ASSERT_EQ(0, dm_file->metaVersion());

    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    ASSERT_THROW({ iw->finalize(); }, DB::Exception);
}
CATCH

TEST_P(LocalDMFile, RestoreInvalidMetaVersion)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);
    ASSERT_EQ(0, dm_file->metaVersion());

    ASSERT_THROW(
        {
            DMFile::restore(
                file_provider_maybe_encrypted,
                1,
                1,
                parent_path,
                DMFileMeta::ReadMode::all(),
                /* meta_version= */ 1);
        },
        DB::Exception);
}
CATCH

TEST_P(LocalDMFile, RestoreWithMetaVersion)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);
    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion({}));
    iw->finalize();

    // Read out meta version = 0
    dm_file = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 0);

    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());

    // Read out meta version = 1
    dm_file = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 1);

    ASSERT_EQ(1, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("test", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());
}
CATCH

TEST_P(LocalDMFile, RestoreWithMultipleMetaVersion)
try
{
    auto dm_file_for_write = prepareDMFile(/* file_id= */ 1);

    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file_for_write,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file_for_write->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file_for_write->meta->bumpMetaVersion({}));
    iw->finalize();

    auto dm_file_for_read_v1 = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 1);
    ASSERT_STREQ(
        "test",
        dm_file_for_read_v1->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());

    // Write a new meta with a new version = 2
    iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file_for_write,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file_for_write->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test2";
    ASSERT_EQ(2, dm_file_for_write->meta->bumpMetaVersion({}));
    iw->finalize();

    // Current DMFile instance does not affect
    ASSERT_STREQ(
        "test",
        dm_file_for_read_v1->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());

    // Read out meta version = 2
    auto dm_file_for_read_v2 = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 2);
    ASSERT_STREQ(
        "test2",
        dm_file_for_read_v2->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());
}
CATCH

TEST_P(LocalDMFile, OverrideMetaVersion)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);

    // Write meta v1.
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion({}));
    iw->finalize();

    // Overwrite meta v1.
    // To overwrite meta v1, we restore a v0 instance, and then bump meta version again.
    auto dm_file_2 = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 0);
    iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file_2,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file_2->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test_overwrite";
    ASSERT_EQ(1, dm_file_2->meta->bumpMetaVersion({}));
    ASSERT_NO_THROW({
        iw->finalize();
    }); // No exception should be thrown because it may be a file left by previous writes but segment failed to update meta version.

    // Read out meta v1 again.
    auto dm_file_for_read = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 1);
    ASSERT_STREQ(
        "test_overwrite",
        dm_file_for_read->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());
}
CATCH

TEST_P(LocalDMFile, FinalizeMultipleTimes)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);
    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    dm_file->meta->bumpMetaVersion({});
    iw->finalize();

    ASSERT_THROW({ iw->finalize(); }, DB::Exception);

    dm_file->meta->bumpMetaVersion({});
    ASSERT_THROW({ iw->finalize(); }, DB::Exception);
}
CATCH

class S3DMFile
    : public DMFileMetaVersionTestBase
    , public testing::WithParamInterface<bool>
{
public:
    S3DMFile() { enable_encryption = GetParam(); }

    void SetUp() override
    {
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));

        DMFileMetaVersionTestBase::SetUp();

        auto & global_context = db_context->getGlobalContext();
        ASSERT_TRUE(!global_context.getSharedContextDisagg()->remote_data_store);
        global_context.getSharedContextDisagg()->initRemoteDataStore(
            file_provider_maybe_encrypted,
            /* s3_enabled= */ true);
        ASSERT_TRUE(global_context.getSharedContextDisagg()->remote_data_store);
    }

    void TearDown() override
    {
        DMFileMetaVersionTestBase::TearDown();

        auto & global_context = db_context->getGlobalContext();
        global_context.getSharedContextDisagg()->remote_data_store = nullptr;
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

protected:
    Remote::IDataStorePtr dataStore()
    {
        auto data_store = db_context->getSharedContextDisagg()->remote_data_store;
        RUNTIME_CHECK(data_store != nullptr);
        return data_store;
    }

    DMFilePtr prepareDMFileRemote(UInt64 file_id)
    {
        auto dm_file = prepareDMFile(file_id);
        dataStore()->putDMFile(
            dm_file,
            S3::DMFileOID{
                .store_id = store_id,
                .keyspace_id = keyspace_id,
                .table_id = table_id,
                .file_id = dm_file->fileId(),
            },
            true);
        return dm_file;
    }

protected:
    const StoreID store_id = 17;

    // DeltaMergeStorePtr store;
    bool already_initialize_data_store = false;
    bool already_initialize_write_ps = false;
    DB::PageStorageRunMode orig_mode = PageStorageRunMode::ONLY_V3;
};

INSTANTIATE_TEST_CASE_P( //
    DMFileMetaVersion,
    S3DMFile,
    /* enable_encryption */ ::testing::Bool());

TEST_P(S3DMFile, Basic)
try
{
    // This test case just test DMFileMetaVersionTestForS3 is working.

    auto dm_file = prepareDMFileRemote(/* file_id= */ 1);
    ASSERT_TRUE(dm_file->path().starts_with("s3://"));
    ASSERT_EQ(0, dm_file->metaVersion());

    auto token = dataStore()->prepareDMFile(
        S3::DMFileOID{
            .store_id = store_id,
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .file_id = 1,
        },
        /* page_id= */ 0);
    auto cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 0);
    ASSERT_EQ(0, cn_dmf->metaVersion());

    auto cn_dmf_2 = token->restore(DMFileMeta::ReadMode::all(), 0);
    ASSERT_EQ(0, cn_dmf_2->metaVersion());
}
CATCH

TEST_P(S3DMFile, WriteRemoteDMFile)
try
{
    auto dm_file = prepareDMFileRemote(/* file_id= */ 1);
    ASSERT_TRUE(dm_file->path().starts_with("s3://"));

    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion({}));
    iw->finalize();

    // Read out meta version = 0
    auto token = dataStore()->prepareDMFile(
        S3::DMFileOID{
            .store_id = store_id,
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .file_id = 1,
        },
        /* page_id= */ 0);
    auto cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 0);
    ASSERT_EQ(0, cn_dmf->metaVersion());
    ASSERT_STREQ("", cn_dmf->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());

    // Read out meta version = 1
    cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 1);
    ASSERT_EQ(1, cn_dmf->metaVersion());
    ASSERT_STREQ("test", cn_dmf->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());
}
CATCH

TEST_P(S3DMFile, WithFileCache)
try
{
    StorageRemoteCacheConfig file_cache_config{
        .dir = fmt::format("{}/fs_cache", getTemporaryPath()),
        .capacity = 1 * 1000 * 1000 * 1000,
    };
    UInt16 vcores = 8;
    FileCache::initialize(
        db_context->getGlobalContext().getPathCapacity(),
        file_cache_config,
        vcores,
        db_context->getGlobalContext().getIORateLimiter());

    auto dm_file = prepareDMFileRemote(/* file_id= */ 1);
    ASSERT_TRUE(dm_file->path().starts_with("s3://"));

    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(MutSup::extra_handle_id).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion({}));
    iw->finalize();

    {
        auto * file_cache = FileCache::instance();
        ASSERT_TRUE(file_cache->getAll().empty());
    }

    // Read out meta version = 0
    auto token = dataStore()->prepareDMFile(
        S3::DMFileOID{
            .store_id = store_id,
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .file_id = 1,
        },
        /* page_id= */ 0);
    auto cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 0);
    ASSERT_EQ(0, cn_dmf->metaVersion());
    ASSERT_STREQ("", cn_dmf->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());

    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
    }

    // Read out meta version = 1
    cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 1);
    ASSERT_EQ(1, cn_dmf->metaVersion());
    ASSERT_STREQ("test", cn_dmf->meta->getColumnStats()[MutSup::extra_handle_id].additional_data_for_test.c_str());

    SCOPE_EXIT({ FileCache::shutdown(); });
}
CATCH

namespace
{
std::unordered_set<String> collectObjectKeys(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const LoggerPtr & log)
{
    const auto objects = collectMetaV2MergedFilesForLocalRead(dmfile, read_columns, log, "DMFileLocalStagingTest");
    std::unordered_set<String> keys;
    for (const auto & object : objects)
        keys.insert(object.s3_key);
    return keys;
}
} // namespace

TEST_P(LocalDMFile, LocalStagingNonMetaV2Noop)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = DMFile::create(
        /*file_id=*/2,
        parent_path,
        std::make_optional<DMChecksumConfig>(),
        /*small_file_size_threshold=*/0,
        16 * 1024 * 1024,
        DMFileFormat::V2);
    ASSERT_FALSE(dm_file->useMetaV2());
    ASSERT_TRUE(collectMetaV2MergedFilesForLocalRead(dm_file, *cols, log, "DMFileLocalStagingTest").empty());
}
CATCH

TEST_P(LocalDMFile, LocalStagingInvalidS3PathNoop)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFile(/* file_id= */ 3);
    ASSERT_TRUE(dm_file->useMetaV2());
    ASSERT_TRUE(collectMetaV2MergedFilesForLocalRead(dm_file, *cols, log, "DMFileLocalStagingTest").empty());
}
CATCH

TEST_P(S3DMFile, LocalStagingDedupMergedFiles)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 10);
    const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(dmfile_meta, nullptr);

    const auto objects = collectMetaV2MergedFilesForLocalRead(dm_file, *cols, log, "DMFileLocalStagingTest");
    ASSERT_FALSE(objects.empty());
    ASSERT_EQ(objects.size(), dmfile_meta->merged_files.size());

    std::unordered_set<String> keys;
    for (const auto & object : objects)
    {
        EXPECT_TRUE(S3::S3FilenameView::fromKey(object.s3_key).isValid());
        EXPECT_TRUE(object.s3_key.ends_with(".merged"));
        EXPECT_GT(object.file_size, 0);
        EXPECT_TRUE(keys.insert(object.s3_key).second);
    }
}
CATCH

TEST_P(S3DMFile, LocalStagingOnlyReadColumns)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto all_cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 11);

    const ColumnDefines handle_only{getExtraHandleColumnDefine(/*is_common_handle=*/false)};
    const ColumnDefines nullable_only{ColumnDefine{
        1,
        "Nullable(UInt64)",
        DataTypeFactory::instance().get("Nullable(UInt64)"),
    }};

    const auto all_keys = collectObjectKeys(dm_file, *all_cols, log);
    const auto handle_keys = collectObjectKeys(dm_file, handle_only, log);
    const auto nullable_keys = collectObjectKeys(dm_file, nullable_only, log);

    ASSERT_FALSE(all_keys.empty());
    ASSERT_FALSE(handle_keys.empty());
    ASSERT_FALSE(nullable_keys.empty());
    for (const auto & key : handle_keys)
        ASSERT_TRUE(all_keys.contains(key));
    for (const auto & key : nullable_keys)
        ASSERT_TRUE(all_keys.contains(key));

    ColumnDefines missing_column{ColumnDefine{
        9999,
        "missing",
        DataTypeFactory::instance().get("Int64"),
    }};
    ASSERT_TRUE(collectObjectKeys(dm_file, missing_column, log).empty());
}
CATCH

TEST_P(S3DMFile, LocalStagingDownloadDisabled)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 12);
    ASSERT_TRUE(tryDownloadMetaV2MergedFilesForLocalRead(dm_file, *cols, /*enable=*/false, log, "DMFileLocalStagingTest")
                    .empty());
}
CATCH

TEST_P(S3DMFile, LocalStagingDownloadWithoutFileCache)
try
{
    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 13);
    ASSERT_EQ(FileCache::instance(), nullptr);
    ASSERT_TRUE(tryDownloadMetaV2MergedFilesForLocalRead(dm_file, *cols, /*enable=*/true, log, "DMFileLocalStagingTest")
                    .empty());
}
CATCH

TEST_P(S3DMFile, LocalStagingDownloadMergedFiles)
try
{
    StorageRemoteCacheConfig file_cache_config{
        .dir = fmt::format("{}/write_filecache_staging", getTemporaryPath()),
        .capacity = 1 * 1000 * 1000 * 1000,
    };
    UInt16 vcores = 8;
    FileCache::initialize(
        db_context->getGlobalContext().getPathCapacity(),
        file_cache_config,
        vcores,
        db_context->getGlobalContext().getIORateLimiter());
    SCOPE_EXIT({ FileCache::shutdown(); });

    auto log = Logger::get("DMFileLocalStagingTest");
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 14);

    const auto attempt_before = GET_METRIC(tiflash_storage_write_filecache_staging, type_attempt).Value();
    const auto local_read_files
        = tryDownloadMetaV2MergedFilesForLocalRead(dm_file, *cols, /*enable=*/true, log, "DMFileLocalStagingTest");
    ASSERT_FALSE(local_read_files.empty());
    ASSERT_EQ(
        attempt_before + 1,
        GET_METRIC(tiflash_storage_write_filecache_staging, type_attempt).Value());
    ASSERT_GE(
        GET_METRIC(tiflash_storage_write_filecache_staging, type_download_ok).Value(),
        local_read_files.size());
    ASSERT_GT(
        GET_METRIC(tiflash_storage_write_filecache_staging_bytes, type_staged).Value(),
        0);

    auto * file_cache = FileCache::instance();
    ASSERT_NE(file_cache, nullptr);
    ASSERT_FALSE(file_cache->getAll().empty());
}
CATCH

TEST_P(S3DMFile, LocalStagingBuildWithWriteFileCache)
try
{
    StorageRemoteCacheConfig file_cache_config{
        .dir = fmt::format("{}/write_filecache_build", getTemporaryPath()),
        .capacity = 1 * 1000 * 1000 * 1000,
    };
    UInt16 vcores = 8;
    FileCache::initialize(
        db_context->getGlobalContext().getPathCapacity(),
        file_cache_config,
        vcores,
        db_context->getGlobalContext().getIORateLimiter());
    SCOPE_EXIT({ FileCache::shutdown(); });

    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);
    auto dm_file = prepareDMFileRemote(/* file_id= */ 15);
    ASSERT_TRUE(FileCache::instance()->getAll().empty());

    DMFileBlockInputStreamBuilder builder(*db_context);
    auto stream = builder.enableWriteFileCacheLocalRead(true).build(
        dm_file,
        *cols,
        {RowKeyRange::newAll(false, 1)},
        std::make_shared<ScanContext>());
    ASSERT_FALSE(FileCache::instance()->getAll().empty());

    auto block = stream->read();
    ASSERT_GT(block.rows(), 0);
}
CATCH

} // namespace DB::DM::tests
