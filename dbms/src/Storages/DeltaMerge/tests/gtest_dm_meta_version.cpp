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
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>

#include <memory>


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
    ASSERT_STREQ("", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion());
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
    ASSERT_STREQ("", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());

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
    ASSERT_STREQ("test", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());
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
    dm_file_for_write->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file_for_write->meta->bumpMetaVersion());
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
        dm_file_for_read_v1->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());

    // Write a new meta with a new version = 2
    iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file_for_write,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file_for_write->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test2";
    ASSERT_EQ(2, dm_file_for_write->meta->bumpMetaVersion());
    iw->finalize();

    // Current DMFile instance does not affect
    ASSERT_STREQ(
        "test",
        dm_file_for_read_v1->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());

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
        dm_file_for_read_v2->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
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
    dm_file->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion());
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
    dm_file_2->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test_overwrite";
    ASSERT_EQ(1, dm_file_2->meta->bumpMetaVersion());
    ASSERT_THROW({ iw->finalize(); }, DB::Exception);

    // Read out meta v1 again.
    auto dm_file_for_read = DMFile::restore(
        file_provider_maybe_encrypted,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /* meta_version= */ 1);
    ASSERT_STREQ(
        "test",
        dm_file_for_read->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
}
CATCH

TEST_P(LocalDMFile, FinalizeMultipleTimes)
try
{
    auto dm_file = prepareDMFile(/* file_id= */ 1);
    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    dm_file->meta->bumpMetaVersion();
    iw->finalize();

    ASSERT_THROW({ iw->finalize(); }, DB::Exception);

    dm_file->meta->bumpMetaVersion();
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
    ASSERT_STREQ("", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion());
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
    ASSERT_STREQ("", cn_dmf->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());

    // Read out meta version = 1
    cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 1);
    ASSERT_EQ(1, cn_dmf->metaVersion());
    ASSERT_STREQ("test", cn_dmf->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());
}
CATCH

TEST_P(S3DMFile, WithFileCache)
try
{
    StorageRemoteCacheConfig file_cache_config{
        .dir = fmt::format("{}/fs_cache", getTemporaryPath()),
        .capacity = 1 * 1000 * 1000 * 1000,
    };
    FileCache::initialize(db_context->getGlobalContext().getPathCapacity(), file_cache_config);

    auto dm_file = prepareDMFileRemote(/* file_id= */ 1);
    ASSERT_TRUE(dm_file->path().starts_with("s3://"));

    ASSERT_EQ(0, dm_file->metaVersion());
    ASSERT_EQ(4, dm_file->meta->getColumnStats().size());
    ASSERT_STREQ("", dm_file->getColumnStat(::DB::TiDBPkColumnID).additional_data_for_test.c_str());

    // Write new metadata
    auto iw = DMFileV3IncrementWriter::create(DMFileV3IncrementWriter::Options{
        .dm_file = dm_file,
        .file_provider = file_provider_maybe_encrypted,
        .write_limiter = db_context->getWriteLimiter(),
        .path_pool = path_pool,
        .disagg_ctx = db_context->getSharedContextDisagg(),
    });
    dm_file->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test = "test";
    ASSERT_EQ(1, dm_file->meta->bumpMetaVersion());
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
    ASSERT_STREQ("", cn_dmf->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());

    {
        auto * file_cache = FileCache::instance();
        ASSERT_FALSE(file_cache->getAll().empty());
    }

    // Read out meta version = 1
    cn_dmf = token->restore(DMFileMeta::ReadMode::all(), 1);
    ASSERT_EQ(1, cn_dmf->metaVersion());
    ASSERT_STREQ("test", cn_dmf->meta->getColumnStats()[::DB::TiDBPkColumnID].additional_data_for_test.c_str());

    SCOPE_EXIT({ FileCache::shutdown(); });
}
CATCH

} // namespace DB::DM::tests
