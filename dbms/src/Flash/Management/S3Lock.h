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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <common/types.h>
#include <grpcpp/support/status.h>
#include <kvrpcpb.pb.h>

#include <fstream>
#include <mutex>
#include <unordered_map>


namespace DB::Management
{

class S3LockService final : private boost::noncopyable
{
public:
    static constexpr auto tmp_empty_file = "/tmp/tmp_empty_file";

    struct DataFileMutex
    {
        std::mutex file_mutex;
        std::atomic<UInt32> ref_count = 0;

        DataFileMutex() = default;

        void lock()
        {
            ++ref_count;
            file_mutex.lock();
        }

        void unlock()
        {
            file_mutex.unlock();
            --ref_count;
        }

        UInt32 getRefCount() const
        {
            return ref_count;
        }
    };

    using DataFileMutexPtr = std::shared_ptr<DataFileMutex>;

private:
    Context & context;

    std::unordered_map<String, DataFileMutexPtr> & file_latch_map;
    std::mutex & file_latch_map_mutex;

    const String bucket_name;
    const std::unique_ptr<Aws::S3::S3Client> s3_client;

    LoggerPtr log;

public:
    S3LockService(Context & context_,
                  std::unordered_map<String, DataFileMutexPtr> & file_latch_map_,
                  std::mutex & file_latch_map_mutex_,
                  const String bucket_name_,
                  const Aws::Client::ClientConfiguration & client_config_,
                  const Aws::Auth::AWSCredentials & credentials_)
        : context(context_)
        , file_latch_map(file_latch_map_)
        , file_latch_map_mutex(file_latch_map_mutex_)
        , bucket_name(bucket_name_)
        , s3_client(std::make_unique<Aws::S3::S3Client>(
              credentials_,
              client_config_,
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              /*useVirtualAddressing*/ true))
        , log(Logger::get())
    {
        std::ofstream tmp_file(tmp_empty_file);
        tmp_file.close();
        (void)context;
    }

    ~S3LockService()
    {
        std::remove(tmp_empty_file);
    }

    grpc::Status tryAddLock(const kvrpcpb::TryAddLockRequest * request, kvrpcpb::TryAddLockResponse * response)
    try
    {
        response->set_is_success(tryAddLockImpl(request->ori_data_file(), request->ori_store_id(), request->lock_store_id(), request->upload_seq(), response));
        return grpc::Status::OK;
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, e.displayText());
        return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }

    grpc::Status tryMarkDelete(const kvrpcpb::TryMarkDeleteRequest * request, kvrpcpb::TryMarkDeleteResponse * response)
    try
    {
        response->set_is_success(tryMarkDeleteImpl(request->ori_data_file(), request->ori_store_id(), response));
        return grpc::Status::OK;
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, e.displayText());
        return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
    }

private:
    bool tryAddLockImpl(const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq, kvrpcpb::TryAddLockResponse * response);

    bool tryMarkDeleteImpl(String data_file, UInt64 ori_store_id, kvrpcpb::TryMarkDeleteResponse * response);
};

class S3LockClient
{
private:
    Context & context;
    LoggerPtr log;

public:
    explicit S3LockClient(Context & context_)
        : context(context_)
        , log(Logger::get())
    {}

    std::pair<bool, std::optional<kvrpcpb::S3LockError>> sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq);

    std::pair<bool, std::optional<kvrpcpb::S3LockError>> sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id);
};

} // namespace DB::Management
