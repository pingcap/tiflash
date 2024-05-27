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

#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Disaggregated/S3LockService.h>
#include <Flash/ServiceUtils.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <TiDB/OwnerInfo.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>


namespace CurrentMetrics
{
extern const Metric S3LockServiceNumLatches;
}

namespace DB::S3
{
S3LockService::S3LockService(Context & context_)
    : S3LockService(context_.getGlobalContext().getTMTContext().getS3GCOwnerManager())
{}

S3LockService::S3LockService(OwnerManagerPtr owner_mgr_)
    : gc_owner(std::move(owner_mgr_))
    , log(Logger::get())
{}

grpc::Status S3LockService::tryAddLock(
    const disaggregated::TryAddLockRequest * request,
    disaggregated::TryAddLockResponse * response)
{
    try
    {
        Stopwatch watch;
        SCOPE_EXIT({
            GET_METRIC(tiflash_disaggregated_object_lock_request_duration_seconds, type_lock)
                .Observe(watch.elapsedSeconds());
        });
        tryAddLockImpl(request->data_file_key(), request->lock_store_id(), request->lock_seq(), response);
        if (response->result().has_conflict())
        {
            GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_lock_conflict).Increment();
        }
        else if (response->result().has_not_owner())
        {
            GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_owner_changed).Increment();
        }
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, "other exception");
    }
}

grpc::Status S3LockService::tryMarkDelete(
    const disaggregated::TryMarkDeleteRequest * request,
    disaggregated::TryMarkDeleteResponse * response)
{
    try
    {
        Stopwatch watch;
        SCOPE_EXIT({
            GET_METRIC(tiflash_disaggregated_object_lock_request_duration_seconds, type_delete)
                .Observe(watch.elapsedSeconds());
        });
        tryMarkDeleteImpl(request->data_file_key(), response);
        if (response->result().has_conflict())
        {
            GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_delete_conflict).Increment();
        }
        else if (response->result().has_not_owner())
        {
            GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_owner_changed).Increment();
        }
        return grpc::Status::OK;
    }
    catch (const TiFlashException & e)
    {
        LOG_ERROR(log, "TiFlash Exception: {}\n{}", e.displayText(), e.getStackTrace().toString());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}\n{}", e.message(), e.getStackTrace().toString());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_ERROR(log, "KV Client Exception: {}", e.message());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "std exception: {}", e.what());
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "other exception");
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_error).Increment();
        return grpc::Status(grpc::StatusCode::INTERNAL, "other exception");
    }
}

S3LockService::DataFileMutexPtr S3LockService::getDataFileLatch(const String & data_file_key)
{
    std::unique_lock lock(file_latch_map_mutex);
    auto it = file_latch_map.find(data_file_key);
    if (it == file_latch_map.end())
    {
        it = file_latch_map.emplace(data_file_key, std::make_shared<DataFileMutex>()).first;
        CurrentMetrics::add(CurrentMetrics::S3LockServiceNumLatches);
    }
    auto file_latch = it->second;
    // must add ref count under the protection of `file_latch_map_mutex`
    file_latch->addRefCount();
    return file_latch;
}

bool S3LockService::tryAddLockImpl(
    const String & data_file_key,
    UInt64 lock_store_id,
    UInt64 lock_seq,
    disaggregated::TryAddLockResponse * response) NO_THREAD_SAFETY_ANALYSIS
{
    GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_lock).Increment();
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
            CurrentMetrics::sub(CurrentMetrics::S3LockServiceNumLatches);
        }
    });

    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    // make sure data file exists
    auto object_key
        = key_view.isDMFile() ? fmt::format("{}/{}", data_file_key, DM::DMFileMetaV2::metaFileName()) : data_file_key;
    if (!DB::S3::objectExists(*s3_client, object_key))
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file not exist, key={}", data_file_key));
        LOG_INFO(log, "data file lock conflict: not exist, key={}", data_file_key);
        return false;
    }

    // make sure data file is not mark as deleted
    const auto delmark_key = key_view.getDelMarkKey();
    if (DB::S3::objectExists(*s3_client, delmark_key))
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file is mark deleted, key={} delmark={}", data_file_key, delmark_key));
        LOG_INFO(log, "data file lock conflict: mark deleted, key={} delmark={}", data_file_key, delmark_key);
        return false;
    }

    // Check whether this node is owner again before uploading lock file
    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        LOG_INFO(log, "data file lock conflict: owner changed, key={}", data_file_key);
        return false;
    }
    const auto lock_key = key_view.getLockKey(lock_store_id, lock_seq);
    // upload lock file
    DB::S3::uploadEmptyFile(*s3_client, lock_key);
    if (!gc_owner->isOwner())
    {
        // although the owner is changed after lock file is uploaded, but
        // it is safe to return owner change and let the client retry.
        // the obsolete lock file will finally get removed by S3GCManager.
        response->mutable_result()->mutable_not_owner();
        LOG_INFO(
            log,
            "data file lock conflict: owner changed after lock added, key={} lock_key={}",
            data_file_key,
            lock_key);
        return false;
    }

    LOG_INFO(log, "data file is locked, key={} lock_key={}", data_file_key, lock_key);
    response->mutable_result()->mutable_success();
    return true;
}

bool S3LockService::tryMarkDeleteImpl(const String & data_file_key, disaggregated::TryMarkDeleteResponse * response)
{
    const S3FilenameView key_view = S3FilenameView::fromKey(data_file_key);
    RUNTIME_CHECK(key_view.isDataFile(), data_file_key);
    GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_delete).Increment();

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
            CurrentMetrics::sub(CurrentMetrics::S3LockServiceNumLatches);
        }
    });

    // make sure data file has not been locked
    const auto lock_prefix = key_view.getLockPrefix();
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    std::optional<String> lock_key = S3::anyKeyExistWithPrefix(*s3_client, lock_prefix);
    if (lock_key)
    {
        auto * e = response->mutable_result()->mutable_conflict();
        e->set_reason(fmt::format("data file is locked, key={} lock_by={}", data_file_key, lock_key.value()));
        LOG_INFO(
            log,
            "data file mark delete conflict: file is locked, key={} lock_by={}",
            data_file_key,
            lock_key.value());
        return false;
    }

    // Check whether this node is owner again before marking delete
    if (!gc_owner->isOwner())
    {
        // client should retry
        response->mutable_result()->mutable_not_owner();
        LOG_INFO(log, "data file mark delete conflict: owner changed, key={}", data_file_key);
        return false;
    }
    // upload delete mark
    const auto delmark_key = key_view.getDelMarkKey();
    String tagging;
    if (S3::ClientFactory::instance().gc_method == S3GCMethod::Lifecycle)
    {
        tagging = TaggingObjectIsDeleted;
    }
    DB::S3::uploadEmptyFile(*s3_client, delmark_key, tagging);
    if (!gc_owner->isOwner())
    {
        // owner changed happens when delmark is uploading, can not
        // ensure whether this is safe or not.
        LOG_ERROR(
            log,
            "data file mark delete conflict: owner changed when marking file deleted! key={} delmark={}",
            data_file_key,
            delmark_key);
        GET_METRIC(tiflash_disaggregated_object_lock_request_count, type_delete_risk).Increment();
    }

    LOG_INFO(log, "data file is mark deleted, key={} delmark={}", data_file_key, delmark_key);
    response->mutable_result()->mutable_success();
    return true;
}

} // namespace DB::S3
