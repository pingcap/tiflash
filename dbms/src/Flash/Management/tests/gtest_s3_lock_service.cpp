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

#include <Common/Logger.h>
#include <Flash/Management/S3Lock.h>
#include <Storages/S3/S3Common.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/types.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <fstream>

namespace DB::Management::tests
{

class S3LockServiceTest
    : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        Aws::Client::ClientConfiguration client_config;
        auto & client_factory = DB::S3::ClientFactory::instance();
        client_config.endpointOverride = "172.16.5.85:9000";
        client_config.scheme = Aws::Http::Scheme::HTTP;
        client_config.verifySSL = false;
        Aws::Auth::AWSCredentials cred("minioadmin", "minioadmin");
        s3_lock_service = std::make_unique<Management::S3LockService>(*db_context, bucket_name, client_config, cred);
        log = Logger::get();
        s3_client = client_factory.create("172.16.5.85:9000", Aws::Http::Scheme::HTTP, false, "minioadmin", "minioadmin");
        setDataFiles();
    }

    void setDataFiles()
    {
        const char * tmp_file_name = "tmp_file";
        std::ofstream tmp_file(tmp_file_name);
        tmp_file.close();
        // create 5 data files
        for (size_t i = 1; i <= 5; ++i)
        {
            auto dm_file_name = fmt::format("/s{}/stable/t_{}/dmf_{}", store_id, physical_table_id, dm_file_id);
            DB::S3::uploadFile(*s3_client, bucket_name, tmp_file_name, dm_file_name);
            ++dm_file_id;
        }
        std::remove(tmp_file_name);
    }

    void TearDown() override
    {
        // clean data files
        while (dm_file_id > 0)
        {
            --dm_file_id;
            auto dm_file_name = fmt::format("/s{}/stable/t_{}/dmf_{}", store_id, physical_table_id, dm_file_id);

            DB::S3::deletaFile(*s3_client, bucket_name, dm_file_name);
        }
    }

protected:
    const String bucket_name = "qiuyang";
    std::unique_ptr<Aws::S3::S3Client> s3_client;
    std::unique_ptr<Management::S3LockService> s3_lock_service;
    const UInt64 store_id = 1;
    const UInt64 physical_table_id = 1;
    UInt64 dm_file_id = 1;
    UInt64 upload_seq = 0;
    const UInt64 lock_store_id = 2;
    LoggerPtr log;
};

TEST_F(S3LockServiceTest, SingleTryAddLockRequest)
try
{
    auto request = ::disaggregated::TryAddLockRequest();
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
    String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);
    request.set_ori_store_id(store_id);
    request.set_ori_data_file(data_file_name);
    request.set_upload_seq(upload_seq);
    request.set_lock_store_id(lock_store_id);
    auto response = ::disaggregated::TryAddLockResponse();
    auto status_code = s3_lock_service->tryAddLock(&request, &response);

    ASSERT_TRUE(status_code.ok());
    ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, lock_file_name));

    DB::S3::deletaFile(*s3_client, bucket_name, lock_file_name);
}
CATCH


TEST_F(S3LockServiceTest, SingleTryMarkDeleteTest)
try
{
    auto request = ::disaggregated::TryMarkDeleteRequest();
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
    String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);

    request.set_ori_store_id(store_id);
    request.set_ori_data_file(data_file_name);
    auto response = ::disaggregated::TryMarkDeleteResponse();
    auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

    ASSERT_TRUE(status_code.ok());
    ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, delete_file_name));

    DB::S3::deletaFile(*s3_client, bucket_name, delete_file_name);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryAddLockRequestWithDeleteFileTest)
try
{
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
    String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);
    String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);

    // Add delete file first
    {
        auto request = ::disaggregated::TryMarkDeleteRequest();

        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, delete_file_name));
    }

    // Try add lock file, should fail
    {
        auto request = ::disaggregated::TryAddLockRequest();
        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        request.set_upload_seq(upload_seq);
        request.set_lock_store_id(lock_store_id);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.is_success());
        ASSERT_TRUE(response.error().has_err_data_file_is_deleted());
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, bucket_name, lock_file_name));
    }

    DB::S3::deletaFile(*s3_client, bucket_name, delete_file_name);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryMarkDeleteRequestWithLockFileTest)
try
{
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
    String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);
    String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);

    // Add lock file first
    {
        auto request = ::disaggregated::TryAddLockRequest();

        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        request.set_upload_seq(upload_seq);
        request.set_lock_store_id(lock_store_id);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, lock_file_name));
    }

    // Try add delete mark, should fail
    {
        auto request = ::disaggregated::TryMarkDeleteRequest();

        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.is_success());
        ASSERT_TRUE(response.error().has_err_data_file_is_locked());
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, bucket_name, delete_file_name));
    }

    DB::S3::deletaFile(*s3_client, bucket_name, lock_file_name);
}
CATCH

TEST_F(S3LockServiceTest, SingleTryAddLockRequestWithDataFileLostTest)
try
{
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id);
    String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);

    // Try add lock file, data file is not exist, should fail
    {
        auto request = ::disaggregated::TryAddLockRequest();

        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        request.set_upload_seq(upload_seq);
        request.set_lock_store_id(lock_store_id);
        auto response = ::disaggregated::TryAddLockResponse();
        auto status_code = s3_lock_service->tryAddLock(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(!response.is_success());
        ASSERT_TRUE(response.error().has_err_data_file_is_missing());
        ASSERT_TRUE(!DB::S3::objectExists(*s3_client, bucket_name, lock_file_name));
    }
}
CATCH

TEST_F(S3LockServiceTest, MultipleTryAddLockRequest)
try
{
    auto job = [&](size_t lock_id) -> void {
        String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
        String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_id, upload_seq);

        // Try add lock file simultaneously, should success
        {
            auto request = ::disaggregated::TryAddLockRequest();

            request.set_ori_store_id(store_id);
            request.set_ori_data_file(data_file_name);
            request.set_upload_seq(upload_seq);
            request.set_lock_store_id(lock_id);
            auto response = ::disaggregated::TryAddLockResponse();
            auto status_code = s3_lock_service->tryAddLock(&request, &response);

            ASSERT_TRUE(status_code.ok());
            ASSERT_TRUE(response.is_success());
            ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, lock_file_name));

            DB::S3::deletaFile(*s3_client, bucket_name, lock_file_name);
        }
    };

    std::vector<std::thread> threads;
    const size_t thread_num = 10;
    threads.reserve(thread_num);
    for (size_t i = 0; i < thread_num; ++i)
    {
        threads.emplace_back(job, i);
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
    String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id - 1);
    String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);
    auto job = [&]() -> void {
        auto request = ::disaggregated::TryMarkDeleteRequest();
        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
        ASSERT_TRUE(DB::S3::objectExists(*s3_client, bucket_name, delete_file_name));
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

    DB::S3::deletaFile(*s3_client, bucket_name, delete_file_name);
}
CATCH

TEST_F(S3LockServiceTest, MultipleMixRequest)
try
{
    auto lock_job = [&](size_t file_id) -> void {
        String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, file_id);
        String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);

        {
            auto request = ::disaggregated::TryAddLockRequest();

            request.set_ori_store_id(store_id);
            request.set_ori_data_file(data_file_name);
            request.set_upload_seq(upload_seq);
            request.set_lock_store_id(lock_store_id);
            auto response = ::disaggregated::TryAddLockResponse();
            auto status_code = s3_lock_service->tryAddLock(&request, &response);

            ASSERT_TRUE(status_code.ok());
        }
    };

    auto delete_job = [&](size_t file_id) -> void {
        String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, file_id);
        String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);
        auto request = ::disaggregated::TryMarkDeleteRequest();
        request.set_ori_store_id(store_id);
        request.set_ori_data_file(data_file_name);
        auto response = ::disaggregated::TryMarkDeleteResponse();
        auto status_code = s3_lock_service->tryMarkDelete(&request, &response);

        ASSERT_TRUE(status_code.ok());
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
        String data_file_name = fmt::format("t_{}/dmf_{}", physical_table_id, i);
        String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", store_id, data_file_name, lock_store_id, upload_seq);
        String delete_file_name = fmt::format("/s{}/stable/{}.del", store_id, data_file_name);

        // Either lock or delete file should exist
        if (DB::S3::objectExists(*s3_client, bucket_name, delete_file_name))
        {
            DB::S3::deletaFile(*s3_client, bucket_name, delete_file_name);
        }
        else if (DB::S3::objectExists(*s3_client, bucket_name, lock_file_name))
        {
            DB::S3::deletaFile(*s3_client, bucket_name, lock_file_name);
        }
        else
        {
            ASSERT_TRUE(false);
        }
    }
}
CATCH

} // namespace DB::Management::tests
