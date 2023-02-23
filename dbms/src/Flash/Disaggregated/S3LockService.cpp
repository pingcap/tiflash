// Copyright 2023 PingCAP, Ltd.
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
#include <TiDB/OwnerInfo.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>


namespace DB::S3
{


S3LockService::S3LockService(Context & context_)
    : S3LockService(
        context_.getGlobalContext().getTMTContext().getS3GCOwnerManager(),
        S3::ClientFactory::instance().createWithBucket())
{
}

S3LockService::S3LockService(OwnerManagerPtr owner_mgr_, std::unique_ptr<TiFlashS3Client> && s3_cli_)
    : gc_owner(std::move(owner_mgr_))
    , s3_client(std::move(s3_cli_))
    , log(Logger::get())
{
}

grpc::Status S3LockService::tryAddLock(const disaggregated::TryAddLockRequest * request, disaggregated::TryAddLockResponse * response)
{
    try
    {
        tryAddLockImpl(request->data_file_key(), request->lock_store_id(), request->lock_seq(), response);
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
        tryMarkDeleteImpl(request->data_file_key(), response);
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

S3LockService::DataFileMutexPtr
S3LockService::getDataFileLatch(const String & data_file_key)
{
    std::unique_lock lock(file_latch_map_mutex);
    auto it = file_latch_map.find(data_file_key);
    if (it == file_latch_map.end())
    {
        it = file_latch_map.emplace(data_file_key, std::make_shared<DataFileMutex>()).first;
    }
    auto file_latch = it->second;
    // must add ref count under the protection of `file_latch_map_mutex`
    file_latch->addRefCount();
    return file_latch;
}

bool S3LockService::tryAddLockImpl(const String & data_file_key, UInt64 lock_store_id, UInt64 lock_seq, disaggregated::TryAddLockResponse * response)
{
    if (data_file_key.empty())
        return false;

    const S3FilenameView key_view = S3FilenameView::fromKey(data_file_key);
    RUNTIME_CHECK(key_view.isDataFile(), data_file_key);

    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        return false;
    }

    // Get the latch of the file, with ref count added
    auto file_latch = getDataFileLatch(data_file_key);
    file_latch->lock(); // prevent other request on the same key
    SCOPE_EXIT({
        file_latch->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        // currently no request for the same key, release
        if (file_latch->decreaseRefCount() == 0)
        {
            file_latch_map.erase(data_file_key);
        }
    });

    // make sure data file exists
    if (!DB::S3::objectExists(*s3_client, s3_client->bucket(), data_file_key))
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file not exist, key={}", data_file_key));
        return false;
    }

    // make sure data file is not mark as deleted
    const auto delmark_key = key_view.getDelMarkKey();
    if (DB::S3::objectExists(*s3_client, s3_client->bucket(), delmark_key))
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file is mark deleted, key={} delmark={}", data_file_key, delmark_key));
        return false;
    }

    // Check whether this node is owner again before uploading lock file
    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        return false;
    }
    const auto lock_key = key_view.getLockKey(lock_store_id, lock_seq);
    // upload lock file
    DB::S3::uploadEmptyFile(*s3_client, s3_client->bucket(), lock_key);
    if (!gc_owner->isOwner())
    {
        // altough the owner is changed after lock file is uploaded, but
        // it is safe to return owner change and let the client retry.
        // the obsolete lock file will finally get removed in S3 GC.
        response->mutable_result()->mutable_not_owner();
        return false;
    }

    response->mutable_result()->mutable_success();
    return true;
}

std::optional<String> S3LockService::anyLockExist(const String & lock_prefix) const
{
    std::optional<String> lock_key;
    DB::S3::listPrefix(
        *s3_client,
        s3_client->bucket(),
        lock_prefix,
        "",
        [&lock_key](const Aws::S3::Model::ListObjectsV2Result & result) -> S3::PageResult {
            const auto & contents = result.GetContents();
            if (!contents.empty())
            {
                lock_key = contents.front().GetKey();
            }
            return S3::PageResult{
                .num_keys = contents.size(),
                .more = false, // do not need more result
            };
        });
    return lock_key;
}

bool S3LockService::tryMarkDeleteImpl(const String & data_file_key, disaggregated::TryMarkDeleteResponse * response)
{
    const S3FilenameView key_view = S3FilenameView::fromKey(data_file_key);
    RUNTIME_CHECK(key_view.isDataFile(), data_file_key);

    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        return false;
    }

    // Get the latch of the file, with ref count added
    auto file_latch = getDataFileLatch(data_file_key);
    file_latch->lock(); // prevent other request on the same key
    SCOPE_EXIT({
        file_latch->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        // currently no request for the same key, release
        if (file_latch->decreaseRefCount() == 0)
        {
            file_latch_map.erase(data_file_key);
        }
    });

    // make sure data file has not been locked
    const auto lock_prefix = key_view.getLockPrefix();
    std::optional<String> lock_key = anyLockExist(lock_prefix);
    if (lock_key)
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file is locked, key={} lock_by={}", data_file_key, lock_key.value()));
        return false;
    }

    // Check whether this node is owner again before marking delete
    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        return false;
    }
    // upload delete mark
    const auto delmark_key = key_view.getDelMarkKey();
    DB::S3::uploadEmptyFile(*s3_client, s3_client->bucket(), delmark_key);
    if (!gc_owner->isOwner())
    {
        // owner changed happens when delmark is uploading, can not
        // ensure whether this is safe or not.
        LOG_ERROR(log, "owner changed when marking file deleted! key={} delmark={}", data_file_key, delmark_key);
    }

    response->mutable_result()->mutable_success();
    return true;
}

} // namespace DB::S3
