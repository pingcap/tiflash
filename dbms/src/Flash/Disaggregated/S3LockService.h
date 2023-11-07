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

#pragma once

#include <Common/Logger.h>
#include <Storages/S3/S3Common.h>
#include <TiDB/OwnerManager.h>
#include <common/types.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/support/status.h>
#include <kvproto/disaggregated.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class Context;
}

namespace DB::S3
{

// Disaggregated TiFlash could share the same S3 object by multiple TiFlash
// instances. And a TiFlash instance claims the ownership of the S3 object
// by "add lock".
// The S3 GC will respect the lock and only "mark delete" when there is
// no any lock on a given S3 object.
//
// This class provide atomic "add lock" and "mark deleted" service for a
// given S3 object.
class S3LockService final : private boost::noncopyable
{
public:
    explicit S3LockService(Context & context_);

    explicit S3LockService(OwnerManagerPtr owner_mgr_);

    ~S3LockService() = default;

    grpc::Status tryAddLock(
        const disaggregated::TryAddLockRequest * request,
        disaggregated::TryAddLockResponse * response);

    grpc::Status tryMarkDelete(
        const disaggregated::TryMarkDeleteRequest * request,
        disaggregated::TryMarkDeleteResponse * response);

private:
    struct DataFileMutex;
    using DataFileMutexPtr = std::shared_ptr<DataFileMutex>;
    struct DataFileMutex
    {
        std::mutex file_mutex;
        UInt32 ref_count = 0;

        void lock() NO_THREAD_SAFETY_ANALYSIS { file_mutex.lock(); }

        void unlock() NO_THREAD_SAFETY_ANALYSIS { file_mutex.unlock(); }

        // must be protected by the mutex on the whole map
        void addRefCount() { ++ref_count; }

        // must be protected by the mutex on the whole map
        UInt32 decreaseRefCount()
        {
            --ref_count;
            return ref_count;
        }
    };

    bool tryAddLockImpl(
        const String & data_file_key,
        UInt64 lock_store_id,
        UInt64 lock_seq,
        disaggregated::TryAddLockResponse * response);

    bool tryMarkDeleteImpl(const String & data_file_key, disaggregated::TryMarkDeleteResponse * response);

    DataFileMutexPtr getDataFileLatch(const String & data_file_key);

private:
    std::unordered_map<String, DataFileMutexPtr> file_latch_map;
    std::mutex file_latch_map_mutex;

    OwnerManagerPtr gc_owner;

    LoggerPtr log;
};


} // namespace DB::S3
