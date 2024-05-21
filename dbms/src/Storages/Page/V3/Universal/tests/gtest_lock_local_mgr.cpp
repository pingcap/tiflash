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
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/S3LockLocalManager.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
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

} // namespace DB::tests
