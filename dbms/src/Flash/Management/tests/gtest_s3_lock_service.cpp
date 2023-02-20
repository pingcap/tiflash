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
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/types.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>
#include <future>
#include <thread>

namespace DB::Management::tests
{

class S3LockServiceTest
    : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        s3_lock_service = std::make_unique<Management::S3LockService>(*db_context, file_latch_map, file_latch_map_mutex, bucket_name, client_config);
        log = Logger::get();

        setDataFiles();
    }

    void setDataFiles()
    {
        Aws::S3::S3Client s3_client(client_config);

        for (size_t i = 0; i < 100; ++i)
        {
            auto dm_file_name = fmt::format("s{}/stable/t_{}dmf_{}", store_id, physical_table_id, dm_file_id);
            Aws::S3::Model::PutObjectRequest request;
            request.WithBucket(bucket_name);
            request.WithKey(dm_file_name);

            auto outcome = s3_client.PutObject(request);

            ASSERT_TRUE(outcome.IsSuccess());
            ++dm_file_id;
        }
    }

    void TearDown() override
    {
        Aws::S3::S3Client s3_client(client_config);

        while (dm_file_id >= 0)
        {
            auto dm_file_name = fmt::format("s{}/stable/t_{}dmf_{}", store_id, physical_table_id, dm_file_id);
            Aws::S3::Model::DeleteObjectRequest request;
            request.WithBucket(bucket_name);
            request.WithKey(dm_file_name);

            auto outcome = s3_client.DeleteObject(request);

            ASSERT_TRUE(outcome.IsSuccess());
            --dm_file_id;
        }
    }

protected:
    std::unordered_map<String, Management::S3LockService::DataFileMutexPtr> file_latch_map;
    std::shared_mutex file_latch_map_mutex;
    const String bucket_name = "ap-storage";
    const Aws::Client::ClientConfiguration client_config;
    std::unique_ptr<Management::S3LockService> s3_lock_service;
    const UInt64 store_id = 1;
    const UInt64 physical_table_id = 1;
    UInt64 dm_file_id = 0;
    UInt64 upload_seq = 0;
    const UInt64 lock_store_id = 2;
    LoggerPtr log;
};

TEST_F(S3LockServiceTest, SingleTryAddLockRequest)
try
{
    auto request = ::kvrpcpb::TryAddLockRequest();
    request.set_ori_store_id(store_id);
    request.set_ori_data_file(fmt::format("t_{}/dmf_{}", physical_table_id, dm_file_id));
    request.set_upload_seq(upload_seq);
    request.set_lock_store_id(lock_store_id);
    auto response = ::kvrpcpb::TryAddLockResponse();
    auto status_code = s3_lock_service->tryAddLock(&request, &response);
}
CATCH

} // namespace DB::Management::tests
