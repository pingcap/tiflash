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

#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Storages/S3/S3Common.h>
#include <common/types.h>
#include <disaggregated.pb.h>
#include <grpcpp/support/status.h>

#include <fstream>
#include <mutex>
#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>
#pragma GCC diagnostic pop

namespace DB::S3
{

class S3LockService final : private boost::noncopyable
{
public:
    explicit S3LockService(Context & context_);

    ~S3LockService() = default;

    grpc::Status tryAddLock(const disaggregated::TryAddLockRequest * request, disaggregated::TryAddLockResponse * response);


    grpc::Status tryMarkDelete(const disaggregated::TryMarkDeleteRequest * request, disaggregated::TryMarkDeleteResponse * response);

public:
    struct DataFileMutex;
    using DataFileMutexPtr = std::shared_ptr<DataFileMutex>;
    struct DataFileMutex
    {
        std::mutex file_mutex;
        UInt32 ref_count = 0;

        void lock()
        {
            file_mutex.lock();
        }

        void addRefCount()
        {
            ++ref_count;
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

private:
    Context & context;

    std::unordered_map<String, DataFileMutexPtr> file_latch_map;
    std::mutex file_latch_map_mutex;

    const std::unique_ptr<TiFlashS3Client> s3_client;

    LoggerPtr log;

private:
    bool tryAddLockImpl(const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq, disaggregated::TryAddLockResponse * response);

    bool tryMarkDeleteImpl(String data_file, UInt64 ori_store_id, disaggregated::TryMarkDeleteResponse * response);
};


} // namespace DB::S3
