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

#include <Flash/Disaggregated/S3LockService.h>
#include <Flash/ServiceUtils.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/Transaction/TMTContext.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <ext/scope_guard.h>


namespace DB::S3
{


S3LockService::S3LockService(Context & context_)
    : tmt(context_.getGlobalContext().getTMTContext())
    , s3_client(S3::ClientFactory::instance().createWithBucket())
    , log(Logger::get())
{
    UNUSED(tmt);
}

grpc::Status S3LockService::tryAddLock(const disaggregated::TryAddLockRequest * request, disaggregated::TryAddLockResponse * response)
{
    try
    {
        response->set_is_success(tryAddLockImpl(request->ori_data_file(), request->lock_store_id(), request->upload_seq(), response));
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        return grpc::Status(grpc::StatusCode::INTERNAL, "other exception");
    }
}

grpc::Status S3LockService::tryMarkDelete(const disaggregated::TryMarkDeleteRequest * request, disaggregated::TryMarkDeleteResponse * response)
{
    try
    {
        response->set_is_success(tryMarkDeleteImpl(request->ori_data_file(), response));
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        return grpc::Status(grpc::StatusCode::INTERNAL, "other exception");
    }
}

bool S3LockService::tryAddLockImpl(const String & data_file_key, UInt64 lock_store_id, UInt64 lock_seq, disaggregated::TryAddLockResponse * response)
{
    if (data_file_key.empty())
        return false;

    const S3FilenameView key_view = S3FilenameView::fromKey(data_file_key);
    RUNTIME_CHECK(key_view.isDataFile(), data_file_key);

    // Get the lock of the file
    DataFileMutexPtr file_lock;
    {
        std::unique_lock lock(file_latch_map_mutex);
        auto it = file_latch_map.find(data_file_key);
        if (it == file_latch_map.end())
        {
            it = file_latch_map.emplace(data_file_key, std::make_shared<DataFileMutex>()).first;
        }
        file_lock = it->second;
        file_lock->addRefCount();
    }

    file_lock->lock();
    SCOPE_EXIT({
        file_lock->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        if (file_lock->getRefCount() == 0)
        {
            file_latch_map.erase(data_file_key);
        }
    });

    // make sure data file exists
    if (!DB::S3::objectExists(*s3_client, s3_client->bucket(), data_file_key))
    {
        response->mutable_error()->mutable_err_data_file_is_missing();
        return false;
    }

    // make sure data file is not mark as deleted
    const auto delmark_key = key_view.getDelMarkKey();
    if (DB::S3::objectExists(*s3_client, s3_client->bucket(), delmark_key))
    {
        response->mutable_error()->mutable_err_data_file_is_deleted();
        return false;
    }

    try
    {
        // upload lock file
        const auto lock_key = key_view.getLockKey(lock_store_id, lock_seq);
        DB::S3::uploadEmptyFile(*s3_client, s3_client->bucket(), lock_key);
    }
    catch (...)
    {
        response->mutable_error()->mutable_err_add_lock_file_fail();
        return false;
    }

    return true;
}

bool S3LockService::tryMarkDeleteImpl(const String & data_file_key, disaggregated::TryMarkDeleteResponse * response)
{
    if (data_file_key.empty())
        return false;

    const S3FilenameView key_view = S3FilenameView::fromKey(data_file_key);
    RUNTIME_CHECK(key_view.isDataFile(), data_file_key);

    // Get the lock of the file
    DataFileMutexPtr file_lock;
    {
        std::unique_lock lock(file_latch_map_mutex);
        auto it = file_latch_map.find(data_file_key);
        if (it == file_latch_map.end())
        {
            it = file_latch_map.emplace(data_file_key, std::make_shared<DataFileMutex>()).first;
        }
        file_lock = it->second;
        file_lock->addRefCount();
    }

    file_lock->lock();
    SCOPE_EXIT({
        file_lock->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        if (file_lock->getRefCount() == 0)
        {
            file_latch_map.erase(data_file_key);
        }
    });

    const auto lock_prefix = key_view.getLockPrefix();
    bool has_any_lock = false;
    // make sure data file has not been locked
    DB::S3::listPrefix(*s3_client, s3_client->bucket(), lock_prefix, "", [&has_any_lock](const auto & result) -> S3::PageResult {
        const auto & contents = result.GetContents();
        has_any_lock = !contents.empty();
        return S3::PageResult{
            .num_keys = contents.size(),
            .more = false, // do not need more result
        };
    });
    if (has_any_lock)
    {
        response->mutable_error()->mutable_err_data_file_is_locked();
        return false;
    }

    // upload delete file
    try
    {
        DB::S3::uploadEmptyFile(*s3_client, s3_client->bucket(), lock_prefix);
    }
    catch (...)
    {
        response->mutable_error()->mutable_err_add_delete_file_fail();
        return false;
    }

    return true;
}

} // namespace DB::S3
