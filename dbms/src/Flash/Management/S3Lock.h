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
#include <aws/s3/S3Client.h>
#include <common/types.h>
#include <grpcpp/support/status.h>
#include <kvrpcpb.pb.h>

#include <ext/singleton.h>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>


namespace DB::Management
{

class S3LockService final : private boost::noncopyable
{
private:
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

    std::unordered_map<String, DataFileMutexPtr> file_latch_map;
    std::shared_mutex file_latch_map_mutex;
    
    Context & context;
    const String bucket_name;
    const Aws::Client::ClientConfiguration client_config;

    LoggerPtr log;

public:
    S3LockService(Context & context_, const String bucket_name_, const Aws::Client::ClientConfiguration & client_config_)
        : context(context_)
        , bucket_name(bucket_name_)
        , client_config(client_config_)
        , log(Logger::get())
    {}

    ~S3LockService() = default;

    grpc::Status tryAddLock(const kvrpcpb::TryAddLockRequest * request, kvrpcpb::TryAddLockResponse * response)
    try
    {
        response->set_is_success(tryAddLockImpl(request->ori_data_file(), request->ori_store_id(), request->lock_store_id(), request->upload_seq()));
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
        response->set_is_success(tryMarkDeleteImpl(request->ori_data_file(), request->ori_store_id()));
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

    bool sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq);
    bool sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id);

private:
    bool tryAddLockImpl(const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq);

    bool tryMarkDeleteImpl(String data_file, UInt64 ori_store_id);
};

} // namespace DB::Management
