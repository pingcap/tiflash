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

#include <Common/Logger.h>
#include <Common/typeid_cast.h>
#include <Flash/Disaggregated/S3LockService.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/MockOwnerManager.h>
#include <TiDB/OwnerManager.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateBucketResult.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/types.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>

namespace DB::S3::tests
{

class S3LockServiceTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    try
    {
        db_context = DB::tests::TiFlashTestEnv::getContext();
        log = Logger::get();

        auto & client_factory = DB::S3::ClientFactory::instance();
        is_s3_test_enabled = client_factory.isEnabled();

        owner_manager = std::static_pointer_cast<MockOwnerManager>(OwnerManager::createMockOwner("owner_0"));
        owner_manager->campaignOwner();

        s3_client = client_factory.sharedTiFlashClient();
        s3_lock_service = std::make_unique<DB::S3::S3LockService>(owner_manager);
        ::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client);
        createS3DataFiles();
    }
    CATCH

    void createS3DataFiles()
    {
        // create 5 data files
        for (size_t i = 1; i <= 5; ++i)
        {
            auto data_filename = S3Filename::fromDMFileOID(
                DMFileOID{.store_id = store_id, .table_id = physical_table_id, .file_id = dm_file_id});
            DB::S3::uploadEmptyFile(
                *s3_client,
                fmt::format("{}/{}", data_filename.toFullKey(), DM::DMFileMetaV2::metaFileName()));
            ++dm_file_id;
        }
    }

    void TearDown() override
    {
        // clean data files
        while (dm_file_id > 0)
        {
            --dm_file_id;
            auto data_filename = S3Filename::fromDMFileOID(
                DMFileOID{.store_id = store_id, .table_id = physical_table_id, .file_id = dm_file_id});
            DB::S3::deleteObject(*s3_client, data_filename.toFullKey());
        }
    }

    S3Filename getDataFilename(std::optional<UInt64> get_fid = std::nullopt)
    {
        auto file_id = get_fid.has_value() ? get_fid.value() : dm_file_id - 1; // the last uploaded dmfile id
        return S3Filename::fromDMFileOID(
            DMFileOID{.store_id = store_id, .table_id = physical_table_id, .file_id = file_id});
    }

protected:
    bool is_s3_test_enabled = false;

    std::shared_ptr<MockOwnerManager> owner_manager;
    std::unique_ptr<DB::S3::S3LockService> s3_lock_service;

    std::shared_ptr<S3::TiFlashS3Client> s3_client;
    const UInt64 store_id = 1;
    const Int64 physical_table_id = 1;
    UInt64 dm_file_id = 1;
    UInt64 lock_seq = 0;
    const UInt64 lock_store_id = 2;
    LoggerPtr log;
};

#define CHECK_S3_ENABLED                                                                                          \
    if (!is_s3_test_enabled)                                                                                      \
    {                                                                                                             \
        const auto * t = ::testing::UnitTest::GetInstance() -> current_test_info();                               \
        LOG_INFO(log, "{}.{} is skipped because S3ClientFactory is not inited.", t->test_case_name(), t->name()); \
        return;                                                                                                   \
    }


TEST_F(S3LockServiceTest, SingleTryAddLockRequest)
try
{
    auto data_filename = getDataFilename();
    auto data_file_key = data_filename.toFullKey();
    auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);

    auto request = ::disaggregated::TryAddLockRequest();
    request.set_data_file_key(data_file_key);
    request.set_lock_store_id(lock_store_id);
    request.set_lock_seq(lock_seq);
    auto response = ::disaggregated::TryAddLockResponse();
    auto status_code = s3_lock_service->tryAddLock(&request, &response);

    ASSERT_TRUE(status_code.ok()) << status_code.error_message();
    ASSERT_TRUE(response.result().has_success()) << response.ShortDebugString();
    ASSERT_TRUE(DB::S3::objectExists(*s3_client, lock_key));

    DB::S3::deleteObject(*s3_client, lock_key);
}
CATCH


TEST_F(S3LockServiceTest, SingleTryMarkDeleteTest)
try
{
    auto data_filename = getDataFilename();
    auto data_file_key = data_filename.toFullKey();
    auto delmark_key = data_filename.toView().getDelMarkKey();

    auto request = ::disaggregated::TryMarkDeleteRequest();
    request.set_data_file_key(data_file_key);
    auto response = ::disaggregated::TryMarkDeleteResponse();
    auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

    ASSERT_TRUE(status_code.ok()) << status_code.error_message();
    ASSERT_TRUE(response.result().has_success()) << response.ShortDebugString();
    ASSERT_TRUE(DB::S3::objectExists(*s3_client, delmark_key));

    DB::S3::deleteObject(*s3_client, delmark_key);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryAddLockRequestWithDeleteFileTest)
try
{
    auto data_filename = getDataFilename();
    auto data_file_key = data_filename.toFullKey();
    auto delmark_key = data_filename.toView().getDelMarkKey();
    auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);

    // Add delete file first
    {
        auto request = ::disaggregated::TryMarkDeleteRequest();
        request.set_data_file_key(data_file_key);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok()) << status_code.error_message();
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, delmark_key));
    }

    // Try add lock file, should fail
    {
        auto request = ::disaggregated::TryAddLockRequest();
        request.set_data_file_key(data_file_key);
        request.set_lock_seq(lock_seq);
        request.set_lock_store_id(lock_store_id);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.result().has_success()) << response.ShortDebugString();
        ASSERT_TRUE(response.result().has_conflict()) << response.ShortDebugString();
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, lock_key));
    }

    DB::S3::deleteObject(*s3_client, delmark_key);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryMarkDeleteRequestWithLockFileTest)
try
{
    auto data_filename = getDataFilename();
    auto data_file_key = data_filename.toFullKey();
    auto delmark_key = data_filename.toView().getDelMarkKey();
    auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);

    // Add lock file first
    {
        auto request = ::disaggregated::TryAddLockRequest();

        request.set_data_file_key(data_file_key);
        request.set_lock_store_id(lock_store_id);
        request.set_lock_seq(lock_seq);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, lock_key));
    }

    // Try add delete mark, should fail
    {
        auto request = ::disaggregated::TryMarkDeleteRequest();

        request.set_data_file_key(data_file_key);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.result().has_success()) << response.ShortDebugString();
        ASSERT_TRUE(response.result().has_conflict()) << response.ShortDebugString();
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, delmark_key));
    }

    DB::S3::deleteObject(*s3_client, lock_key);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryAddLockRequestWithDataFileLostTest)
try
{
    auto data_filename = getDataFilename(dm_file_id); // not created dmfile key
    auto data_file_key = data_filename.toFullKey();
    auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);

    // Try add lock file, data file is not exist, should fail
    {
        auto request = ::disaggregated::TryAddLockRequest();

        request.set_data_file_key(data_file_key);
        request.set_lock_seq(lock_seq);
        request.set_lock_store_id(lock_store_id);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.result().has_success()) << response.ShortDebugString();
        ASSERT_TRUE(response.result().has_conflict()) << response.ShortDebugString();
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, lock_key));
    }
}
CATCH

TEST_F(S3LockServiceTest, MultipleTryAddLockRequest)
try
{
    auto job = [&](size_t store_id) -> void {
        auto data_filename = getDataFilename();
        auto data_file_key = data_filename.toFullKey();

        // Try add lock file simultaneously, should success
        {
            auto lock_key = data_filename.toView().getLockKey(store_id, lock_seq);

            auto request = ::disaggregated::TryAddLockRequest();
            request.set_data_file_key(data_file_key);
            request.set_lock_store_id(store_id);
            request.set_lock_seq(lock_seq);
            auto response = ::disaggregated::TryAddLockResponse();
            auto status_code = s3_lock_service->tryAddLock(&request, &response);

            ASSERT_TRUE(status_code.ok());
            ASSERT_TRUE(response.result().has_success()) << response.ShortDebugString();
            ASSERT_TRUE(DB::S3::objectExists(*s3_client, lock_key));

            DB::S3::deleteObject(*s3_client, lock_key);
        }
    };

    std::vector<std::thread> threads;
    const size_t thread_num = 10;
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        threads.emplace_back(job, /*store_id*/ 40 + i);
    }
    for (auto & thread : threads)
    {
        thread.join();
    }
}
CATCH

TEST_F(S3LockServiceTest, MultipleTryMarkDeleteRequest)
try
{
    auto data_filename = getDataFilename();
    auto data_file_key = data_filename.toFullKey();
    auto delmark_key = data_filename.toView().getDelMarkKey();

    auto job = [&]() -> void {
        auto request = ::disaggregated::TryMarkDeleteRequest();
        request.set_data_file_key(data_file_key);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, delmark_key));
    };

    std::vector<std::thread> threads;
    const size_t thread_num = 10;
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        threads.emplace_back(job);
    }
    for (auto & thread : threads)
    {
        thread.join();
    }

    DB::S3::deleteObject(*s3_client, delmark_key);
}
CATCH

TEST_F(S3LockServiceTest, MultipleMixRequest)
try
{
    auto lock_job = [&](size_t file_id) -> void {
        auto data_filename = getDataFilename(file_id);
        auto data_file_key = data_filename.toFullKey();
        auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);

        auto request = ::disaggregated::TryAddLockRequest();
        request.set_data_file_key(data_file_key);
        request.set_lock_seq(lock_seq);
        request.set_lock_store_id(lock_store_id);

        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);
        ASSERT_TRUE(status_code.ok());
    };

    auto delete_job = [&](size_t file_id) -> void {
        auto data_filename = getDataFilename(file_id);
        auto data_file_key = data_filename.toFullKey();
        auto delmark_key = data_filename.toView().getDelMarkKey();

        auto request = ::disaggregated::TryMarkDeleteRequest();
        request.set_data_file_key(data_file_key);

        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);
        ASSERT_TRUE(status_code.ok()) << status_code.error_message();
    };

    std::vector<std::thread> threads;
    const size_t thread_num = (dm_file_id - 1) * 2;
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        if (random() % 2 == 0)
            threads.emplace_back(lock_job, i / 2 + 1);
        else
            threads.emplace_back(delete_job, i / 2 + 1);
    }

    for (auto & thread : threads)
    {
        thread.join();
    }

    for (size_t i = 1; i < dm_file_id; ++i)
    {
        auto data_filename = getDataFilename(i);
        auto data_file_key = data_filename.toFullKey();
        auto lock_key = data_filename.toView().getLockKey(lock_store_id, lock_seq);
        auto delmark_key = data_filename.toView().getDelMarkKey();

        // Either lock or delete file should exist
        if (DB::S3::objectExists(*s3_client, delmark_key))
        {
            DB::S3::deleteObject(*s3_client, delmark_key);
        }
        else if (DB::S3::objectExists(*s3_client, lock_key))
        {
            DB::S3::deleteObject(*s3_client, lock_key);
        }
        else
        {
            ASSERT_TRUE(false) << fmt::format(
                "none of delmark or lock exist! data_key={} delmark={} lock={}",
                data_file_key,
                delmark_key,
                lock_key);
        }
    }
}
CATCH

} // namespace DB::S3::tests
