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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Debug/TiFlashTestEnv.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/S3LockLocalManager.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>


namespace DB::ErrorCodes
{
extern const int S3_LOCK_CONFLICT;
}
namespace DB::tests
{

class S3LockLocalManagerTest : public testing::Test
{
public:
    S3LockLocalManagerTest()
        : s3_client(S3::ClientFactory::instance().sharedTiFlashClient())
        , log(Logger::get())
    {}

    void SetUp() override
    {
        if (DB::tests::TiFlashTestEnv::isMockedS3Client())
            ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client);
        ::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client);
    }

protected:
    std::shared_ptr<S3::TiFlashS3Client> s3_client;
    LoggerPtr log;
};

TEST_F(S3LockLocalManagerTest, LockForFAPIngest)
try
{
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    auto info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(1, info.upload_sequence);
    ASSERT_TRUE(info.pre_lock_keys.empty());

    // Mock FAP ingest following pages from another store
    // - 1 dtfile
    // - 2 remote page in the same CheckpointData
    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 44;
    auto s3name_dtfile
        = S3::S3Filename::fromDMFileOID(S3::DMFileOID{.store_id = old_store_id, .table_id = 10, .file_id = 5});
    auto s3name_datafile = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    {
        S3::uploadEmptyFile(
            *s3_client,
            fmt::format("{}/{}", s3name_dtfile.toFullKey(), DM::DMFileMetaV2::metaFileName()));
        PS::V3::CheckpointLocation loc{
            .data_file_id = std::make_shared<String>(s3name_dtfile.toFullKey()),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        wb.putRemoteExternal("1", loc);
    }
    {
        auto key = std::make_shared<String>(s3name_datafile.toFullKey());
        S3::uploadEmptyFile(*s3_client, *key);
        PS::V3::CheckpointLocation loc2{
            .data_file_id = key,
            .offset_in_file = 0,
            .size_in_file = 1024,
        };
        wb.putRemotePage("2", 0, 1024, loc2, {});

        PS::V3::CheckpointLocation loc3{
            .data_file_id = key,
            .offset_in_file = 1024,
            .size_in_file = 10240,
        };
        wb.putRemotePage("3", 0, 1024, loc3, {});
    }
    // mock UniversalPageStorage::write(wb)
    mgr.createS3LockForWriteBatch(wb);

    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(2, info.upload_sequence);
    ASSERT_EQ(2, info.pre_lock_keys.size());
    UInt64 lock_by_seq = info.upload_sequence;
    const String expected_lockkey1 = s3name_datafile.toView().getLockKey(this_store_id, lock_by_seq);
    ASSERT_GT(info.pre_lock_keys.count(expected_lockkey1), 0) << fmt::format("{}", lock_by_seq);
    const String expected_lockkey2 = s3name_dtfile.toView().getLockKey(this_store_id, info.upload_sequence);
    ASSERT_GT(info.pre_lock_keys.count(expected_lockkey2), 0) << fmt::format("{}", info.pre_lock_keys);
    EXPECT_TRUE(S3::objectExists(*s3_client, expected_lockkey1));
    EXPECT_TRUE(S3::objectExists(*s3_client, expected_lockkey2));

    // pre_lock_keys won't be cleaned after `allocateNewUploadLocksInfo`
    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(3, info.upload_sequence);
    ASSERT_EQ(2, info.pre_lock_keys.size());
    ASSERT_GT(info.pre_lock_keys.count(expected_lockkey1), 0) << fmt::format("{}", info.pre_lock_keys);
    ASSERT_GT(info.pre_lock_keys.count(expected_lockkey2), 0) << fmt::format("{}", info.pre_lock_keys);

    // clean pre_lock_keys
    mgr.cleanAppliedS3ExternalFiles({expected_lockkey1, expected_lockkey2});
    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(4, info.upload_sequence);
    ASSERT_EQ(0, info.pre_lock_keys.size()); // empty
}
CATCH

TEST_F(S3LockLocalManagerTest, LockForFAPIngestFail)
try
{
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    // Mock FAP ingest following pages from another store
    // - 1 dtfile
    // - 2 remote page in the same CheckpointData
    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 44;
    auto s3name_dtfile
        = S3::S3Filename::fromDMFileOID(S3::DMFileOID{.store_id = old_store_id, .table_id = 10, .file_id = 5});
    auto s3name_datafile = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    {
        S3::uploadEmptyFile(*s3_client, s3name_dtfile.toFullKey());
        PS::V3::CheckpointLocation loc{
            .data_file_id = std::make_shared<String>(s3name_dtfile.toFullKey()),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        wb.putRemoteExternal("1", loc);
    }
    {
        auto key = std::make_shared<String>(s3name_datafile.toFullKey());
        S3::uploadEmptyFile(*s3_client, *key);
        PS::V3::CheckpointLocation loc2{
            .data_file_id = key,
            .offset_in_file = 0,
            .size_in_file = 1024,
        };
        wb.putRemotePage("2", 0, 1024, loc2, {});

        PS::V3::CheckpointLocation loc3{
            .data_file_id = key,
            .offset_in_file = 1024,
            .size_in_file = 10240,
        };
        wb.putRemotePage("3", 0, 1024, loc3, {});
    }

    // However, the dtfile is marked as deleted by S3GC before FAP apply
    auto mark_del_res = mock_s3lock_client->sendTryMarkDeleteRequest(s3name_dtfile.toFullKey(), 1);
    ASSERT_TRUE(mark_del_res.first) << mark_del_res.second;

    try
    {
        // Then when FAP want to apply the write batch,
        // it should be comes into exception
        mgr.createS3LockForWriteBatch(wb);
        ASSERT_TRUE(false) << "should not run into here";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(ErrorCodes::S3_LOCK_CONFLICT, e.code());
    }
}
CATCH

TEST_F(S3LockLocalManagerTest, CleanAppliedS3ExternalFilesPartialCleanup)
try
{
    // Purpose: verify partial cleanup only removes applied lock keys and keeps
    // remaining pre-lock keys for later manifest upload.
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 55;
    auto s3name_datafile1 = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    auto s3name_datafile2 = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 2);
    {
        auto key1 = std::make_shared<String>(s3name_datafile1.toFullKey());
        auto key2 = std::make_shared<String>(s3name_datafile2.toFullKey());
        S3::uploadEmptyFile(*s3_client, *key1);
        S3::uploadEmptyFile(*s3_client, *key2);
        PS::V3::CheckpointLocation loc1{.data_file_id = key1, .offset_in_file = 0, .size_in_file = 1024};
        PS::V3::CheckpointLocation loc2{.data_file_id = key2, .offset_in_file = 0, .size_in_file = 1024};
        wb.putRemotePage("1", 0, 1024, loc1, {});
        wb.putRemotePage("2", 0, 1024, loc2, {});
    }

    mgr.createS3LockForWriteBatch(wb);

    // Step 1: confirm two pre-lock keys are present after lock creation.
    auto info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(info.pre_lock_keys.size(), 2);

    const String expected_lockkey1 = s3name_datafile1.toView().getLockKey(this_store_id, info.upload_sequence);
    const String expected_lockkey2 = s3name_datafile2.toView().getLockKey(this_store_id, info.upload_sequence);

    // Step 2: clean one applied lock key, then verify the other remains.
    mgr.cleanAppliedS3ExternalFiles({expected_lockkey1});
    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(info.pre_lock_keys.size(), 1);
    ASSERT_EQ(info.pre_lock_keys.count(expected_lockkey2), 1);

    // Step 3: clean the remaining lock key and verify no residual keys.
    mgr.cleanAppliedS3ExternalFiles({expected_lockkey2});
    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_TRUE(info.pre_lock_keys.empty());
}
CATCH

TEST_F(S3LockLocalManagerTest, CleanPreLockKeysOnWriteFailure)
try
{
    // Purpose: simulate "pre-locks created but write fails" and ensure the
    // failure-cleanup API can remove those pre-lock keys completely.
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 88;
    auto s3name_datafile1 = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    auto s3name_datafile2 = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 2);
    {
        auto key1 = std::make_shared<String>(s3name_datafile1.toFullKey());
        auto key2 = std::make_shared<String>(s3name_datafile2.toFullKey());
        S3::uploadEmptyFile(*s3_client, *key1);
        S3::uploadEmptyFile(*s3_client, *key2);
        PS::V3::CheckpointLocation loc1{.data_file_id = key1, .offset_in_file = 0, .size_in_file = 1024};
        PS::V3::CheckpointLocation loc2{.data_file_id = key2, .offset_in_file = 0, .size_in_file = 1024};
        wb.putRemotePage("1", 0, 1024, loc1, {});
        wb.putRemotePage("2", 0, 1024, loc2, {});
    }

    // Step 1: create remote locks and verify pre_lock_keys are populated.
    mgr.createS3LockForWriteBatch(wb);
    auto info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(info.pre_lock_keys.size(), 2);

    const String expected_lockkey1 = s3name_datafile1.toView().getLockKey(this_store_id, info.upload_sequence);
    const String expected_lockkey2 = s3name_datafile2.toView().getLockKey(this_store_id, info.upload_sequence);

    // Step 2: mimic write failure path and cleanup all lock keys created above.
    mgr.cleanPreLockKeysOnWriteFailure({expected_lockkey1, expected_lockkey2});

    // Step 3: verify no residual pre_lock_keys remain.
    info = mgr.allocateNewUploadLocksInfo();
    ASSERT_TRUE(info.pre_lock_keys.empty());
}
CATCH

TEST_F(S3LockLocalManagerTest, CreateS3LockForWriteBatchReturnsOnlyNewlyAppendedKeys)
try
{
    // Purpose: verify return keys only contain newly created lock keys appended
    // into pre_lock_keys, and exclude lock-file inputs that are already locked.
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 99;
    auto s3name_datafile = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    auto s3name_locked_file = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 2);

    // Step 1: add one normal data-file input and one already-lock-file input.
    {
        auto data_file_key = std::make_shared<String>(s3name_datafile.toFullKey());
        S3::uploadEmptyFile(*s3_client, *data_file_key);
        PS::V3::CheckpointLocation loc{
            .data_file_id = data_file_key,
            .offset_in_file = 0,
            .size_in_file = 1024,
        };
        wb.putRemotePage("1", 0, 1024, loc, {});
    }
    String reused_lock_key = s3name_locked_file.toView().getLockKey(this_store_id, 233);
    {
        auto lock_file_key = std::make_shared<String>(reused_lock_key);
        PS::V3::CheckpointLocation loc{
            .data_file_id = lock_file_key,
            .offset_in_file = 0,
            .size_in_file = 1024,
        };
        wb.putRemotePage("2", 0, 1024, loc, {});
    }

    // Step 2: create locks and check returned key set.
    auto created_keys = mgr.createS3LockForWriteBatch(wb);
    const String expected_new_lock = s3name_datafile.toView().getLockKey(this_store_id, 1);
    ASSERT_EQ(created_keys.size(), 1);
    ASSERT_EQ(created_keys.count(expected_new_lock), 1);
    ASSERT_EQ(created_keys.count(reused_lock_key), 0);

    // Step 3: pre_lock_keys should only contain newly appended lock keys.
    auto info = mgr.allocateNewUploadLocksInfo();
    ASSERT_EQ(info.pre_lock_keys.size(), 1);
    ASSERT_EQ(info.pre_lock_keys.count(expected_new_lock), 1);
    ASSERT_EQ(info.pre_lock_keys.count(reused_lock_key), 0);
}
CATCH

TEST_F(S3LockLocalManagerTest, CreateS3LockForWriteBatchPartialFailureKeepsPreLockKeysEmpty)
try
{
    // Purpose: verify partial lock creation failure does not append partial keys
    // into pre_lock_keys.
    StoreID this_store_id = 100;
    PS::V3::S3LockLocalManager mgr;
    auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto last_mf = mgr.initStoreInfo(this_store_id, mock_s3lock_client, PS::V3::universal::PageDirectoryPtr{});
    ASSERT_FALSE(last_mf.has_value());

    UniversalWriteBatch wb;
    StoreID old_store_id = 5;
    UInt64 old_store_seq = 123;
    auto s3name_datafile_ok = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 1);
    auto s3name_datafile_conflict = S3::S3Filename::newCheckpointData(old_store_id, old_store_seq, 2);
    {
        auto key_ok = std::make_shared<String>(s3name_datafile_ok.toFullKey());
        auto key_conflict = std::make_shared<String>(s3name_datafile_conflict.toFullKey());
        S3::uploadEmptyFile(*s3_client, *key_ok);
        S3::uploadEmptyFile(*s3_client, *key_conflict);
        PS::V3::CheckpointLocation loc1{.data_file_id = key_ok, .offset_in_file = 0, .size_in_file = 1024};
        PS::V3::CheckpointLocation loc2{.data_file_id = key_conflict, .offset_in_file = 0, .size_in_file = 1024};
        wb.putRemotePage("1", 0, 1024, loc1, {});
        wb.putRemotePage("2", 0, 1024, loc2, {});
    }

    // Step 1: make the second file conflict, so the batch fails after at least
    // one successful lock creation attempt.
    auto mark_del_res = mock_s3lock_client->sendTryMarkDeleteRequest(s3name_datafile_conflict.toFullKey(), 1);
    ASSERT_TRUE(mark_del_res.first) << mark_del_res.second;

    // Step 2: batch lock creation should throw S3_LOCK_CONFLICT.
    try
    {
        mgr.createS3LockForWriteBatch(wb);
        FAIL() << "should throw S3_LOCK_CONFLICT";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(ErrorCodes::S3_LOCK_CONFLICT, e.code());
    }

    // Step 3: no partial pre_lock_keys should be appended on failure.
    auto info = mgr.allocateNewUploadLocksInfo();
    ASSERT_TRUE(info.pre_lock_keys.empty());
}
CATCH

} // namespace DB::tests
