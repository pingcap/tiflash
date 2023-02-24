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

#include <Common/Logger.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <TestUtils/MockS3Client.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/core/utils/DateTime.h>
#include <gtest/gtest.h>

#include <chrono>
#include <unordered_set>

namespace DB::S3
{

class S3GCManagerTest : public ::testing::Test
{
public:
    S3GCManagerTest()
        : log(Logger::get())
    {}

    static void SetUpTestCase()
    {
    }

    LoggerPtr log;
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
            objs.emplace_back(CheckpointManifestS3Object{
                .key = S3Filename::newCheckpointManifest(store_id, seq).toFullKey(),
                .last_modification = timepoint + std::chrono::milliseconds(diff_sec * 1000),
            });
        }
    }
    CheckpointManifestS3Set set = CheckpointManifestS3Set::create(objs);
    ASSERT_EQ(set.latestUploadSequence(), 81);
    ASSERT_EQ(set.latestManifestKey(), S3Filename::newCheckpointManifest(store_id, 81).toFullKey());

    S3GCConfig config{
        .manifest_expired_hour = 1,
        .delmark_expired_hour = 1,
        .temp_path = tests::TiFlashTestEnv::getTemporaryPath(),
    };
    auto mock_client = std::make_shared<MockS3Client>();
    auto mock_lock_client = std::make_shared<MockS3LockClient>(mock_client);
    S3GCManager gc_mgr(mock_client, mock_lock_client, config);
    gc_mgr.removeOutdatedManifest(set, timepoint);

    auto delete_keys = mock_client->delete_keys;
    ASSERT_EQ(delete_keys.size(), 2);
    ASSERT_EQ(delete_keys[0], S3Filename::newCheckpointManifest(store_id, 4).toFullKey());
    ASSERT_EQ(delete_keys[1], S3Filename::newCheckpointManifest(store_id, 5).toFullKey());
}
CATCH


TEST_F(S3GCManagerTest, RemoveDataFile)
try
{
    S3GCConfig config{
        .manifest_expired_hour = 1,
        .delmark_expired_hour = 1,
        .temp_path = tests::TiFlashTestEnv::getTemporaryPath(),
    };
    auto mock_client = std::make_shared<MockS3Client>();
    auto mock_lock_client = std::make_shared<MockS3LockClient>(mock_client);
    S3GCManager gc_mgr(mock_client, mock_lock_client, config);

    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    {
        // delmark expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3601 * 1000);
        gc_mgr.removeDataFileIfDelmarkExpired("datafile_key", "datafile_key.del", timepoint, delmark_mtime);

        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 2);
        ASSERT_EQ(delete_keys[0], "datafile_key");
        ASSERT_EQ(delete_keys[1], "datafile_key.del");
    }
    {
        // delmark not expired
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3599 * 1000);
        gc_mgr.removeDataFileIfDelmarkExpired("datafile_key", "datafile_key.del", timepoint, delmark_mtime);

        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 2);
        ASSERT_EQ(delete_keys[0], "datafile_key");
        ASSERT_EQ(delete_keys[1], "datafile_key.del");
    }
}
CATCH


TEST_F(S3GCManagerTest, RemoveLock)
try
{
    S3GCConfig config{
        .manifest_expired_hour = 1,
        .delmark_expired_hour = 1,
        .temp_path = tests::TiFlashTestEnv::getTemporaryPath(),
    };
    auto mock_client = std::make_shared<MockS3Client>();
    auto mock_lock_client = std::make_shared<MockS3LockClient>(mock_client);
    S3GCManager gc_mgr(mock_client, mock_lock_client, config);

    StoreID store_id = 20;
    auto df = S3Filename::newCheckpointData(store_id, 300, 1);

    auto lock_key = df.toView().getLockKey(store_id, 400);
    auto lock_view = S3FilenameView::fromKey(lock_key);

    auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
    {
        // delmark not exist, and no more lockfile
        mock_client->clear();
        gc_mgr.cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted and delmark is created
        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 1);
        ASSERT_EQ(delete_keys[0], lock_key);
        auto put_keys = mock_client->put_keys;
        ASSERT_EQ(put_keys.size(), 1);
        ASSERT_EQ(put_keys[0], df.toView().getDelMarkKey());
    }
    {
        // delmark not exist, but still locked by another lockfile
        mock_client->clear();
        auto another_lock_key = df.toView().getLockKey(store_id + 1, 450);
        mock_client->list_result = {another_lock_key};
        gc_mgr.cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted and delmark is created
        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 1);
        ASSERT_EQ(delete_keys[0], lock_key);
        auto put_keys = mock_client->put_keys;
        ASSERT_EQ(put_keys.size(), 0);
    }
    {
        // delmark exist, not expired
        mock_client->clear();
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3599 * 1000);
        mock_client->head_result_mtime = delmark_mtime;
        gc_mgr.cleanOneLock(lock_key, lock_view, timepoint);

        // lock is deleted, datafile and delmark remain
        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 1);
        ASSERT_EQ(delete_keys[0], lock_key);
        auto put_keys = mock_client->put_keys;
        ASSERT_EQ(put_keys.size(), 0);
    }
    {
        // delmark exist, expired
        mock_client->clear();
        auto delmark_mtime = timepoint - std::chrono::milliseconds(3601 * 1000);
        mock_client->head_result_mtime = delmark_mtime;
        gc_mgr.cleanOneLock(lock_key, lock_view, timepoint);

        // lock datafile and delmark are deleted
        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 3);
        ASSERT_EQ(delete_keys[0], lock_key);
        ASSERT_EQ(delete_keys[1], df.toFullKey());
        ASSERT_EQ(delete_keys[2], df.toView().getDelMarkKey());
        auto put_keys = mock_client->put_keys;
        ASSERT_EQ(put_keys.size(), 0);
    }
}
CATCH

TEST_F(S3GCManagerTest, ScanLocks)
try
{
    S3GCConfig config{
        .manifest_expired_hour = 1,
        .delmark_expired_hour = 1,
        .temp_path = tests::TiFlashTestEnv::getTemporaryPath(),
    };
    auto mock_client = std::make_shared<MockS3Client>();
    auto mock_lock_client = std::make_shared<MockS3LockClient>(mock_client);
    S3GCManager gc_mgr(mock_client, mock_lock_client, config);

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
            auto lock_key = df.toView().getLockKey(store_id, safe_sequence + 1);
            keys.emplace_back(lock_key);

            // not managed by the latest manifest yet
            df = S3Filename::newCheckpointData(store_id, 300, 1);
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence + 1);
            keys.emplace_back(lock_key);

            // still valid in latest manifest
            df = S3Filename::newCheckpointData(store_id, 300, 1);
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence - 1);
            valid_lock_files.emplace(lock_key);
            keys.emplace_back(lock_key);

            // not valid in latest manfiest, should be delete
            df = S3Filename::newCheckpointData(store_id, 300, 2);
            lock_key = df.toView().getLockKey(lock_store_id, safe_sequence - 1);
            expected_deleted_lock_key = lock_key;
            expected_created_delmark = df.toView().getDelMarkKey();
            keys.emplace_back(lock_key);
        }
        mock_client->clear();
        mock_client->list_result = keys; // set for `LIST`
    }

    {
        auto timepoint = Aws::Utils::DateTime("2023-02-01T08:00:00Z", Aws::Utils::DateFormat::ISO_8601);
        gc_mgr.cleanUnusedLocks(lock_store_id, S3Filename::getLockPrefix(), safe_sequence, valid_lock_files, timepoint);

        // lock is deleted and delmark is created
        auto delete_keys = mock_client->delete_keys;
        ASSERT_EQ(delete_keys.size(), 1);
        ASSERT_EQ(delete_keys[0], expected_deleted_lock_key);
        auto put_keys = mock_client->put_keys;
        ASSERT_EQ(put_keys.size(), 1);
        ASSERT_EQ(put_keys[0], expected_created_delmark);
    }
}
CATCH

} // namespace DB::S3
