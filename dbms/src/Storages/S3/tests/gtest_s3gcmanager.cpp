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
#include <Common/Logger.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/OwnerManager.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/GetObjectTaggingRequest.h>
#include <gtest/gtest.h>
#include <pingcap/pd/MockPDClient.h>

#include <chrono>
#include <memory>
#include <unordered_set>

namespace DB::FailPoints
{
extern const char force_set_mocked_s3_object_mtime[];
} // namespace DB::FailPoints
namespace DB::S3::tests
{
class S3GCManagerTest : public DB::base::TiFlashStorageTestBasic
{
public:
    S3GCManagerTest()
        : log(Logger::get())
    {}

    static void SetUpTestCase() {}

    virtual S3GCConfig getConfig()
    {
        return S3GCConfig{
            .method = S3GCMethod::Lifecycle,
        };
    }

    void SetUp() override
    {
        DB::base::TiFlashStorageTestBasic::SetUp();

        auto config = getConfig();
        mock_s3_client = ClientFactory::instance().sharedTiFlashClient();
        auto mock_gc_owner = OwnerManager::createMockOwner("owner_0");
        auto mock_lock_client = std::make_shared<MockS3LockClient>(mock_s3_client);
        auto mock_pd_client = std::make_shared<pingcap::pd::MockPDClient>();
        auto data_store = std::make_shared<DM::Remote::DataStoreS3>(::DB::tests::TiFlashTestEnv::getMockFileProvider());
        gc_mgr = std::make_unique<S3GCManager>(mock_pd_client, mock_gc_owner, mock_lock_client, data_store, config);

        ::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*mock_s3_client);

        tmp_dir = getTemporaryPath();
        data_file_path_pattern1 = tmp_dir + "/data1_{index}";
        data_file_id_pattern1 = "data1_{index}";
        manifest_file_path1 = tmp_dir + "/manifest_foo1";
        manifest_file_id1 = "manifest_foo1";
        data_file_path_pattern2 = tmp_dir + "/data2_{index}";
        data_file_id_pattern2 = "data2_{index}";
        manifest_file_path2 = tmp_dir + "/manifest_foo2";
        manifest_file_id2 = "manifest_foo2";
        dropDataOnDisk(tmp_dir);
        createIfNotExist(tmp_dir);
    }

    void TearDown() override { ::DB::tests::TiFlashTestEnv::deleteBucket(*mock_s3_client); }

protected:
    String tmp_dir;

    std::shared_ptr<TiFlashS3Client> mock_s3_client;
    std::unique_ptr<S3GCManager> gc_mgr;
    LoggerPtr log;

    String data_file_id_pattern1;
    String data_file_path_pattern1;
    String manifest_file_id1;
    String manifest_file_path1;

    String data_file_id_pattern2;
    String data_file_path_pattern2;
    String manifest_file_id2;
    String manifest_file_path2;
};

class S3GCManagerByScanDeleteTest : public S3GCManagerTest
{
public:
    S3GCConfig getConfig() override
    {
        return S3GCConfig{
            .method = S3GCMethod::ScanThenDelete,
            .delmark_expired_hour = 1,
        };
    }
};

TEST_F(S3GCManagerTest, RemoveManifest)
try
{
    StoreID store_id = 100;
    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    // <upload_seq, create_seconds to timepoint>
    std::vector<std::pair<UInt64, Int64>> mfs = {
        {4, -7201},
        {5, -3601},
        {70, -3600},
        {80, -3599},
        {81, 3601},
    };

    std::vector<CheckpointManifestS3Object> objs;
    {
        objs.reserve(mfs.size());
        for (const auto & [seq, diff_sec] : mfs)
        {
            auto key = S3Filename::newCheckpointManifest(store_id, seq).toFullKey();
            uploadEmptyFile(*mock_s3_client, key);
            objs.emplace_back(CheckpointManifestS3Object{
                .key = key,
                .last_modification = timepoint + std::chrono::milliseconds(diff_sec * 1000),
            });
        }
    }
    CheckpointManifestS3Set set = CheckpointManifestS3Set::create(objs);
    ASSERT_EQ(set.latestUploadSequence(), 81);
    ASSERT_EQ(set.latestManifestKey(), S3Filename::newCheckpointManifest(store_id, 81).toFullKey());
    {
        auto preserved = set.preservedManifests(4, 1, timepoint);
        ASSERT_EQ(preserved.size(), 3);
        EXPECT_EQ(preserved[0], S3Filename::newCheckpointManifest(store_id, 81).toFullKey());
        EXPECT_EQ(preserved[1], S3Filename::newCheckpointManifest(store_id, 80).toFullKey());
        EXPECT_EQ(preserved[2], S3Filename::newCheckpointManifest(store_id, 70).toFullKey());
        auto outdated = set.outdatedObjects(4, 1, timepoint);
        ASSERT_EQ(outdated.size(), 2);
        EXPECT_EQ(outdated[0].key, S3Filename::newCheckpointManifest(store_id, 4).toFullKey());
        EXPECT_EQ(outdated[1].key, S3Filename::newCheckpointManifest(store_id, 5).toFullKey());
    }
    {
        auto preserved = set.preservedManifests(3, 1, timepoint);
        ASSERT_EQ(preserved.size(), 3);
        EXPECT_EQ(preserved[0], S3Filename::newCheckpointManifest(store_id, 81).toFullKey());
        EXPECT_EQ(preserved[1], S3Filename::newCheckpointManifest(store_id, 80).toFullKey());
        EXPECT_EQ(preserved[2], S3Filename::newCheckpointManifest(store_id, 70).toFullKey());
        auto outdated = set.outdatedObjects(3, 1, timepoint);
        ASSERT_EQ(outdated.size(), 2);
        EXPECT_EQ(outdated[0].key, S3Filename::newCheckpointManifest(store_id, 4).toFullKey());
        EXPECT_EQ(outdated[1].key, S3Filename::newCheckpointManifest(store_id, 5).toFullKey());
    }
    {
        auto preserved = set.preservedManifests(2, 1, timepoint);
        ASSERT_EQ(preserved.size(), 2);
        EXPECT_EQ(preserved[0], S3Filename::newCheckpointManifest(store_id, 81).toFullKey());
        EXPECT_EQ(preserved[1], S3Filename::newCheckpointManifest(store_id, 80).toFullKey());
        auto outdated = set.outdatedObjects(2, 1, timepoint);
        ASSERT_EQ(outdated.size(), 3);
        EXPECT_EQ(outdated[0].key, S3Filename::newCheckpointManifest(store_id, 4).toFullKey());
        EXPECT_EQ(outdated[1].key, S3Filename::newCheckpointManifest(store_id, 5).toFullKey());
        EXPECT_EQ(outdated[2].key, S3Filename::newCheckpointManifest(store_id, 70).toFullKey());
    }

    gc_mgr->removeOutdatedManifest(set, &timepoint);

    for (const auto & [seq, obj] : set.objects())
    {
        if (seq == 4 || seq == 5)
        {
            // deleted
            ASSERT_FALSE(S3::objectExists(*mock_s3_client, obj.key));
        }
        else
        {
            ASSERT_TRUE(S3::objectExists(*mock_s3_client, obj.key));
        }
    }
}
CATCH


TEST_F(S3GCManagerByScanDeleteTest, RemoveCheckpointData)
try
{
    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    StoreID store_id = 100;
    UInt64 upload_seq = 99;
    UInt64 file_idx = 5;

    // test for checkpoint data remove
    {
        const auto cp_data = S3Filename::newCheckpointData(store_id, upload_seq, file_idx);
        const auto df_key = cp_data.toFullKey();
        const auto delmark_key = cp_data.toView().getDelMarkKey();
        uploadEmptyFile(*mock_s3_client, df_key);
        uploadEmptyFile(*mock_s3_client, delmark_key);

        // delmark expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3601 * 1000);
        gc_mgr->removeDataFileIfDelmarkExpired(df_key, delmark_key, timepoint, delmark_mtime, log);

        // removed
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, df_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
    }
    {
        const auto cp_data = S3Filename::newCheckpointData(store_id, upload_seq, file_idx);
        const auto df_key = cp_data.toFullKey();
        const auto delmark_key = cp_data.toView().getDelMarkKey();
        uploadEmptyFile(*mock_s3_client, df_key);
        uploadEmptyFile(*mock_s3_client, delmark_key);

        // delmark not expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3599 * 1000);
        gc_mgr->removeDataFileIfDelmarkExpired(df_key, delmark_key, timepoint, delmark_mtime, log);

        // not removed
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
    }
}
CATCH

TEST_F(S3GCManagerByScanDeleteTest, RemoveDMFile)
try
{
    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    // test for dmfile remove
    StoreID store_id = 100;
    TableID table_id = 1000;
    UInt64 file_id2 = 2;
    UInt64 file_id27 = 27;
    {
        const auto cp_dmf2
            = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id2});
        const auto df2_key = cp_dmf2.toFullKey();
        const auto delmark_key = cp_dmf2.toView().getDelMarkKey();
        uploadEmptyFile(*mock_s3_client, df2_key + "/meta");
        uploadEmptyFile(*mock_s3_client, delmark_key);

        const auto cp_dmf27
            = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id27});
        const auto df27_key = cp_dmf27.toFullKey();
        uploadEmptyFile(*mock_s3_client, df27_key + "/meta");

        // delmark expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3601 * 1000);
        gc_mgr->removeDataFileIfDelmarkExpired(df2_key, delmark_key, timepoint, delmark_mtime, log);

        // removed
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, df2_key + "/meta"));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
        // dmf_27 is not removed
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df27_key + "/meta"));
    }
    {
        const auto cp_dmf2
            = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id2});
        const auto df2_key = cp_dmf2.toFullKey();
        const auto delmark_key = cp_dmf2.toView().getDelMarkKey();
        uploadEmptyFile(*mock_s3_client, df2_key + "/meta");
        uploadEmptyFile(*mock_s3_client, delmark_key);

        const auto cp_dmf27
            = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id27});
        const auto df27_key = cp_dmf27.toFullKey();
        uploadEmptyFile(*mock_s3_client, df27_key + "/meta");

        // delmark not expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3599 * 1000);
        gc_mgr->removeDataFileIfDelmarkExpired(df2_key, delmark_key, timepoint, delmark_mtime, log);

        // not removed
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df2_key + "/meta"));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
        // dmf_27 is not removed
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df27_key + "/meta"));
    }
}
CATCH

TEST_F(S3GCManagerByScanDeleteTest, RemoveLock)
try
{
    StoreID store_id = 20;
    auto df = S3Filename::newCheckpointData(store_id, 300, 1);

    auto lock_key = df.toView().getLockKey(store_id, 400);
    auto lock_view = S3FilenameView::fromKey(lock_key);

    auto delmark_key = df.toView().getDelMarkKey();

    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    auto clear_bucket = [&] {
        DB::tests::TiFlashTestEnv::deleteBucket(*mock_s3_client);
        DB::tests::TiFlashTestEnv::createBucketIfNotExist(*mock_s3_client);
    };

    {
        clear_bucket();
        // delmark not exist, and no more lockfile
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, lock_key);

        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));

        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted and delmark is created
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df.toFullKey()));
    }
    {
        clear_bucket();
        // delmark not exist, but still locked by another lockfile
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, lock_key);
        // another lock
        auto another_lock_key = df.toView().getLockKey(store_id + 1, 450);
        S3::uploadEmptyFile(*mock_s3_client, another_lock_key);
        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted but delmark is not created
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, another_lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df.toFullKey()));
    }
    {
        clear_bucket();
        // delmark exist, not expired
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, delmark_key);
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3599 * 1000);
        FailPointHelper::enableFailPoint(
            FailPoints::force_set_mocked_s3_object_mtime,
            std::map<String, Aws::Utils::DateTime>{{delmark_key, delmark_mtime}});
        SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_mocked_s3_object_mtime); });
        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted, datafile and delmark remain
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df.toFullKey()));
    }
    {
        clear_bucket();
        // delmark exist, expired
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, delmark_key);
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3601 * 1000);
        FailPointHelper::enableFailPoint(
            FailPoints::force_set_mocked_s3_object_mtime,
            std::map<String, Aws::Utils::DateTime>{{delmark_key, delmark_mtime}});
        SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_mocked_s3_object_mtime); });
        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock datafile and delmark are deleted
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, df.toFullKey()));
    }
}
CATCH


TEST_F(S3GCManagerTest, RemoveLockOfCheckpointData)
try
{
    StoreID store_id = 20;
    auto df = S3Filename::newCheckpointData(store_id, 300, 1);

    auto lock_key = df.toView().getLockKey(store_id, 400);
    auto lock_view = S3FilenameView::fromKey(lock_key);

    auto delmark_key = df.toView().getDelMarkKey();

    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    auto clear_bucket = [&] {
        DB::tests::TiFlashTestEnv::deleteBucket(*mock_s3_client);
        DB::tests::TiFlashTestEnv::createBucketIfNotExist(*mock_s3_client);
    };

    {
        clear_bucket();
        // delmark not exist, and no more lockfile
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, lock_key);

        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));

        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted, delmark is created, object is rewrite with tagging
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df.toFullKey()));

        auto req = Aws::S3::Model::GetObjectTaggingRequest(); //
        mock_s3_client->setBucketAndKeyWithRoot(req, df.toFullKey());
        const auto res = mock_s3_client->GetObjectTagging(req);

        auto tags = res.GetResult().GetTagSet();
        ASSERT_EQ(tags.size(), 1);
        EXPECT_EQ(tags[0].GetKey(), "tiflash_deleted");
        EXPECT_EQ(tags[0].GetValue(), "true");
    }
    {
        clear_bucket();
        // delmark not exist, but still locked by another lockfile
        S3::uploadEmptyFile(*mock_s3_client, df.toFullKey());
        S3::uploadEmptyFile(*mock_s3_client, lock_key);
        // another lock
        auto another_lock_key = df.toView().getLockKey(store_id + 1, 450);
        S3::uploadEmptyFile(*mock_s3_client, another_lock_key);
        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted but delmark is not created
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, another_lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, df.toFullKey()));
    }
}
CATCH

TEST_F(S3GCManagerTest, RemoveLockOfDMFile)
try
{
    StoreID store_id = 20;
    TableID table_id = 1000;
    UInt64 file_id2 = 2;
    UInt64 file_id27 = 27;

    const auto cp_dmf2
        = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id2});
    const auto dmf2_key = cp_dmf2.toFullKey();
    auto lock_key = cp_dmf2.toView().getLockKey(store_id, 400);
    auto lock_view = S3FilenameView::fromKey(lock_key);
    auto delmark_key = cp_dmf2.toView().getDelMarkKey();

    const auto cp_dmf27
        = S3Filename::fromDMFileOID(DMFileOID{.store_id = store_id, .table_id = table_id, .file_id = file_id27});
    const auto dmf27_key = cp_dmf27.toFullKey();

    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    auto clear_bucket = [&] {
        DB::tests::TiFlashTestEnv::deleteBucket(*mock_s3_client);
        DB::tests::TiFlashTestEnv::createBucketIfNotExist(*mock_s3_client);
    };

    {
        clear_bucket();
        // delmark not exist, and no more lockfile
        S3::uploadEmptyFile(*mock_s3_client, dmf2_key + "/meta");
        S3::uploadEmptyFile(*mock_s3_client, dmf27_key + "/meta");
        S3::uploadEmptyFile(*mock_s3_client, lock_key);

        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));

        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted, delmark is created, object is rewrite with tagging
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, dmf2_key + "/meta"));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, dmf27_key + "/meta"));

        // dmf2 is rewritten
        {
            auto req = Aws::S3::Model::GetObjectTaggingRequest(); //
            mock_s3_client->setBucketAndKeyWithRoot(req, dmf2_key + "/meta");
            const auto res = mock_s3_client->GetObjectTagging(req);

            auto tags = res.GetResult().GetTagSet();
            ASSERT_EQ(tags.size(), 1);
            EXPECT_EQ(tags[0].GetKey(), "tiflash_deleted");
            EXPECT_EQ(tags[0].GetValue(), "true");
        }

        // dmf27 is not rewritten
        {
            auto req = Aws::S3::Model::GetObjectTaggingRequest(); //
            mock_s3_client->setBucketAndKeyWithRoot(req, dmf27_key + "/meta");
            const auto res = mock_s3_client->GetObjectTagging(req);
            ASSERT_TRUE(res.GetResult().GetTagSet().empty());
        }
    }
    {
        clear_bucket();
        // delmark not exist, but still locked by another lockfile
        S3::uploadEmptyFile(*mock_s3_client, dmf2_key + "/meta");
        S3::uploadEmptyFile(*mock_s3_client, dmf27_key + "/meta");
        S3::uploadEmptyFile(*mock_s3_client, lock_key);
        // another lock
        auto another_lock_key = cp_dmf2.toView().getLockKey(store_id + 1, 450);
        S3::uploadEmptyFile(*mock_s3_client, another_lock_key);
        gc_mgr->cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted but delmark is not created
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, lock_key));
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, delmark_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, another_lock_key));
        // dmf2 is not rewritten
        {
            auto req = Aws::S3::Model::GetObjectTaggingRequest(); //
            mock_s3_client->setBucketAndKeyWithRoot(req, dmf2_key + "/meta");
            const auto res = mock_s3_client->GetObjectTagging(req);
            ASSERT_TRUE(res.GetResult().GetTagSet().empty());
        }
        // dmf27 is not rewritten
        {
            auto req = Aws::S3::Model::GetObjectTaggingRequest(); //
            mock_s3_client->setBucketAndKeyWithRoot(req, dmf27_key + "/meta");
            const auto res = mock_s3_client->GetObjectTagging(req);
            ASSERT_TRUE(res.GetResult().GetTagSet().empty());
        }
    }
}
CATCH

TEST_F(S3GCManagerTest, ScanLocks)
try
{
    StoreID store_id = 20;
    StoreID lock_store_id = 21;
    UInt64 safe_sequence = 100;
    std::unordered_set<String> valid_lock_files;
    String expected_deleted_lock_key;
    String expected_created_delmark;

    // prepare and set test keys to mock client
    {
        Strings keys;
        {
            // not managed by lock_store_id
            auto df = S3Filename::newCheckpointData(store_id, 300, 1);
            keys.emplace_back(df.toFullKey());
            auto lock_key = df.toView().getLockKey(store_id, safe_sequence + 1);
            keys.emplace_back(lock_key);

            // not managed by the latest manifest yet
            df = S3Filename::newCheckpointData(store_id, 300, 1);
            keys.emplace_back(df.toFullKey());
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence + 1);
            keys.emplace_back(lock_key);

            // still valid in latest manifest
            df = S3Filename::newCheckpointData(store_id, 300, 1);
            keys.emplace_back(df.toFullKey());
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence - 1);
            valid_lock_files.emplace(lock_key);
            keys.emplace_back(lock_key);

            // not valid in latest manfiest, should be delete
            df = S3Filename::newCheckpointData(store_id, 300, 2);
            keys.emplace_back(df.toFullKey());
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence - 1);
            expected_deleted_lock_key = lock_key;
            expected_created_delmark = df.toView().getDelMarkKey();
            keys.emplace_back(lock_key);
        }

        // prepare for `LIST`
        for (const auto & k : keys)
        {
            uploadEmptyFile(*mock_s3_client, k);
        }
    }

    {
        auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
        gc_mgr
            ->cleanUnusedLocks(lock_store_id, S3Filename::getLockPrefix(), safe_sequence, valid_lock_files, timepoint);

        // lock is deleted and delmark is created
        ASSERT_FALSE(S3::objectExists(*mock_s3_client, expected_deleted_lock_key));
        ASSERT_TRUE(S3::objectExists(*mock_s3_client, expected_created_delmark));
    }
}
CATCH


TEST_F(S3GCManagerTest, ReadManifestFromS3)
try
{
    using namespace ::DB::PS::V3;
    { // prepare the manifest on S3
        const String entry_data = "apple_value";
        auto writer = CPFilesWriter::create({
            .data_file_path_pattern = data_file_path_pattern1,
            .data_file_id_pattern = data_file_id_pattern1,
            .manifest_file_path = manifest_file_path1,
            .manifest_file_id = manifest_file_id1,
            .data_source = CPWriteDataSourceFixture::create({{0, entry_data}, {entry_data.size(), entry_data}}),
        });

        writer->writePrefix({
            .writer = {},
            .sequence = 5,
            .last_sequence = 3,
        });
        {
            auto edits = universal::PageEntriesEdit{};
            // remote entry ingest from other node
            edits.varEntry(
                "apple",
                PageVersion(2),
                PageEntryV3{
                    .size = entry_data.size(),
                    .checkpoint_info = OptionalCheckpointInfo(
                        CheckpointLocation{.data_file_id = std::make_shared<String>("apple_lock")},
                        true,
                        false)},
                1);
            // remote external entry ingest from other node
            edits.varExternal(
                "banana",
                PageVersion(3),
                PageEntryV3{
                    .checkpoint_info = OptionalCheckpointInfo(
                        CheckpointLocation{.data_file_id = std::make_shared<String>("banana_lock")},
                        true,
                        true)},
                1);
            edits.varDel("banana", PageVersion(4));
            edits.varEntry(
                "orange",
                PageVersion(5),
                PageEntryV3{
                    .size = entry_data.size(),
                    .offset = entry_data.size(),
                    .checkpoint_info
                    = OptionalCheckpointInfo(), // an entry written by this node, do not contains checkpoint_info
                },
                1);
            writer->writeEditsAndApplyCheckpointInfo(edits);
        }
        auto data_paths = writer->writeSuffix();
        LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
        writer.reset();

        S3::uploadFile(
            *mock_s3_client,
            manifest_file_path1,
            manifest_file_id1,
            EncryptionPath(manifest_file_path1, ""),
            ::DB::tests::TiFlashTestEnv::getMockFileProvider());
    }
    { // prepare the second manifest on S3
        const String entry_data = "cherry_value";
        auto writer = CPFilesWriter::create({
            .data_file_path_pattern = data_file_path_pattern2,
            .data_file_id_pattern = data_file_id_pattern2,
            .manifest_file_path = manifest_file_path2,
            .manifest_file_id = manifest_file_id2,
            .data_source = CPWriteDataSourceFixture::create({
                {0, entry_data},
            }),
        });

        writer->writePrefix({
            .writer = {},
            .sequence = 5,
            .last_sequence = 3,
        });
        {
            auto edits = universal::PageEntriesEdit{};
            edits.varEntry(
                "cherry",
                PageVersion(6),
                PageEntryV3{
                    .size = entry_data.size(),
                    .offset = 0,
                    .checkpoint_info
                    = OptionalCheckpointInfo(), // an entry written by this node, do not contains checkpoint_info
                },
                1);
            writer->writeEditsAndApplyCheckpointInfo(edits);
        }
        auto data_paths = writer->writeSuffix();
        LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
        writer.reset();

        S3::uploadFile(
            *mock_s3_client,
            manifest_file_path2,
            manifest_file_id2,
            EncryptionPath(manifest_file_path2, ""),
            ::DB::tests::TiFlashTestEnv::getMockFileProvider());
    }

    // read from S3 key
    {
        auto locks = gc_mgr->getValidLocksFromManifest({manifest_file_id1});
        EXPECT_EQ(locks.size(), 3) << fmt::format("{}", locks);
        // the lock ingest by FAP
        EXPECT_TRUE(locks.contains("apple_lock")) << fmt::format("{}", locks);
        EXPECT_TRUE(locks.contains("banana_lock")) << fmt::format("{}", locks);
        // the lock generated by checkpoint dump
        EXPECT_TRUE(locks.contains("data1_0")) << fmt::format("{}", locks);
    }
    {
        auto locks = gc_mgr->getValidLocksFromManifest({manifest_file_id1, manifest_file_id2});
        EXPECT_EQ(locks.size(), 4) << fmt::format("{}", locks);
        // the lock ingest by FAP
        EXPECT_TRUE(locks.contains("apple_lock")) << fmt::format("{}", locks);
        EXPECT_TRUE(locks.contains("banana_lock")) << fmt::format("{}", locks);
        // the lock generated by checkpoint dump
        EXPECT_TRUE(locks.contains("data1_0")) << fmt::format("{}", locks);
        // the lock in the second checkpoint
        EXPECT_TRUE(locks.contains("data2_0")) << fmt::format("{}", locks);
    }
}
CATCH

} // namespace DB::S3::tests
