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

#include <Common/setThreadName.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Management/ManualCompact.h>
#include <Flash/ServiceUtils.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>

#include <ext/scope_guard.h>
#include <future>

namespace DB
{

namespace Management
{

ManualCompactManager::ManualCompactManager(const Context & global_context_, const Settings & settings_)
    : global_context(global_context_.getGlobalContext())
    , settings(settings_)
    , log(Logger::get("ManualCompactManager"))
{
    worker_pool = std::make_unique<legacy::ThreadPool>(static_cast<size_t>(settings.manual_compact_pool_size), [] {
        setThreadName("m-compact-pool");
    });
}

grpc::Status ManualCompactManager::handleRequest(
    const ::kvrpcpb::CompactRequest * request,
    ::kvrpcpb::CompactResponse * response)
{
    auto ks_tbl_id = KeyspaceTableID{RequestUtils::deriveKeyspaceID(*request), request->logical_table_id()};
    {
        std::lock_guard lock(mutex);

        // Check whether there are duplicated executions.
        if (unsync_active_logical_table_ids.count(ks_tbl_id))
        {
            response->mutable_error()->mutable_err_compact_in_progress();
            response->set_has_remaining(false);
            return grpc::Status::OK;
        }

        // Check whether exceeds the maximum number of concurrent executions.
        if (unsync_running_or_pending_tasks > getSettingMaxConcurrency())
        {
            response->mutable_error()->mutable_err_too_many_pending_tasks();
            response->set_has_remaining(false);
            return grpc::Status::OK;
        }

        unsync_active_logical_table_ids.insert(ks_tbl_id);
        unsync_running_or_pending_tasks++;
    }
    SCOPE_EXIT({
        std::lock_guard lock(mutex);
        unsync_active_logical_table_ids.erase(ks_tbl_id);
        unsync_running_or_pending_tasks--;
    });

    std::packaged_task<grpc::Status()> task([&] { return this->doWorkWithCatch(request, response); });
    std::future<grpc::Status> future = task.get_future();
    worker_pool->schedule([&task] { task(); });
    return future.get();
}

grpc::Status ManualCompactManager::doWorkWithCatch(
    const ::kvrpcpb::CompactRequest * request,
    ::kvrpcpb::CompactResponse * response)
{
    try
    {
        return this->doWork(request, response);
    }
    catch (Exception & e)
    {
        LOG_ERROR(log, "DB Exception: {}", e.message());
        return grpc::Status(tiflashErrorCodeToGrpcStatusCode(e.code()), e.message());
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

grpc::Status ManualCompactManager::doWork(
    const ::kvrpcpb::CompactRequest * request,
    ::kvrpcpb::CompactResponse * response)
{
    const auto & tmt_context = global_context.getTMTContext();
    const auto keyspace_id = RequestUtils::deriveKeyspaceID(*request);
    auto storage = tmt_context.getStorages().get(keyspace_id, request->physical_table_id());
    if (storage == nullptr)
    {
        response->mutable_error()->mutable_err_physical_table_not_exist();
        response->set_has_remaining(false);
        return grpc::Status::OK;
    }

    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    if (dm_storage == nullptr)
    {
        response->mutable_error()->mutable_err_physical_table_not_exist();
        response->set_has_remaining(false);
        return grpc::Status::OK;
    }

    DM::RowKeyValue start_key;
    if (request->start_key().empty())
    {
        if (dm_storage->isCommonHandle())
        {
            start_key = DM::RowKeyValue::COMMON_HANDLE_MIN_KEY;
        }
        else
        {
            start_key = DM::RowKeyValue::INT_HANDLE_MIN_KEY;
        }
    }
    else
    {
        try
        {
            ReadBufferFromString buf(request->start_key());

            // TODO: This function seems to be not safe for accepting arbitrary client inputs.
            // When client passes a string with "invalid length" encoded, e.g. a very large length, we will allocate memory first and then discover the input is invalid.
            // This will cause OOM.
            // Also it is not a good idea to use Try-Catch for this scenario.
            start_key = DM::RowKeyValue::deserialize(buf);

            if (start_key.is_common_handle != dm_storage->isCommonHandle())
            {
                response->mutable_error()->mutable_err_invalid_start_key();
                response->set_has_remaining(false);
                return grpc::Status::OK;
            }
        }
        catch (...)
        {
            response->mutable_error()->mutable_err_invalid_start_key();
            response->set_has_remaining(false);
            return grpc::Status::OK;
        }
    }

    size_t compacted_segments = 0;
    bool has_remaining = true;
    std::optional<DM::RowKeyValue> compacted_start_key;
    std::optional<DM::RowKeyValue> compacted_end_key;

    Stopwatch timer;

    auto ks_log = log->getChild(fmt::format("keyspace={}", keyspace_id));
    LOG_INFO(
        ks_log,
        "Manual compaction begin for table {}, start_key = {}",
        request->physical_table_id(),
        start_key.toDebugString());

    // Repeatedly merge multiple segments as much as possible.
    while (true)
    {
        auto compacted_range = dm_storage->mergeDeltaBySegment(global_context, start_key);

        if (compacted_range == std::nullopt)
        {
            // Segment not found according to current start key
            has_remaining = false;
            break;
        }

        compacted_segments++;
        if (compacted_start_key == std::nullopt)
        {
            compacted_start_key = compacted_range->start;
        }
        compacted_end_key = compacted_range->end;

        if (timer.elapsedMilliseconds() < getSettingCompactMoreUntilMs())
        {
            // Let's compact next segment, since the elapsed time is short. This saves round trip.
            start_key = compacted_range->end;
        }
        else
        {
            break;
        }
    }

    if (unlikely(
            has_remaining
            && (compacted_start_key == std::nullopt || compacted_end_key == std::nullopt || compacted_segments == 0)))
    {
        LOG_ERROR(
            ks_log,
            "Assert failed: has_remaining && (compacted_start_key == std::nullopt || compacted_end_key == std::nullopt "
            "|| compacted_segments == 0)");
        throw Exception("Assert failed", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_INFO(
        ks_log,
        "Manual compaction finished for table {}, compacted_start_key = {}, compacted_end_key = {}, has_remaining = "
        "{}, compacted_segments = {}, elapsed_ms = {}",
        request->physical_table_id(),
        compacted_start_key ? compacted_start_key->toDebugString() : "(null)",
        compacted_end_key ? compacted_end_key->toDebugString() : "(null)",
        has_remaining,
        compacted_segments,
        timer.elapsedMilliseconds());

    response->clear_error();
    response->set_has_remaining(has_remaining);
    if (compacted_start_key != std::nullopt)
    {
        WriteBufferFromOwnString wb;
        compacted_start_key->serialize(wb);
        response->set_compacted_start_key(wb.releaseStr());
    }
    if (compacted_end_key != std::nullopt)
    {
        WriteBufferFromOwnString wb;
        compacted_end_key->serialize(wb);
        response->set_compacted_end_key(wb.releaseStr());
    }

    return grpc::Status::OK;
}

uint64_t ManualCompactManager::getSettingCompactMoreUntilMs() const
{
    return settings.manual_compact_more_until_ms.get();
}

uint64_t ManualCompactManager::getSettingMaxConcurrency() const
{
    auto current_thread_size = worker_pool->size();
    auto val = settings.manual_compact_max_concurrency.get();
    return std::max(val, current_thread_size);
}

} // namespace Management

} // namespace DB
