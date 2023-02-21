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

#include "S3Lock.h"

#include <Storages/S3/S3Common.h>
#include <Storages/Transaction/TMTContext.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/logger_useful.h>
#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/conn.h>

#include <ext/scope_guard.h>


namespace pingcap::kv
{
// The rpc trait
template <>
struct RpcTypeTraits<disaggregated::TryAddLockRequest>
{
    using RequestType = disaggregated::TryAddLockRequest;
    using ResultType = disaggregated::TryAddLockResponse;
    static const char * err_msg() { return "tryAddLock Failed"; } // NOLINT(readability-identifier-naming)
    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->tryAddLock(context, req, res);
    }
};

template <>
struct RpcTypeTraits<disaggregated::TryMarkDeleteRequest>
{
    using RequestType = disaggregated::TryMarkDeleteRequest;
    using ResultType = disaggregated::TryMarkDeleteResponse;
    static const char * err_msg() { return "tryMarkDelete Failed"; } // NOLINT(readability-identifier-naming)
    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->tryMarkDelete(context, req, res);
    }
};

} // namespace pingcap::kv


namespace DB::Management
{

std::pair<bool, std::optional<disaggregated::S3LockError>> S3LockClient::sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq)
{
    auto req = std::make_shared<disaggregated::TryAddLockRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);
    req->set_lock_store_id(lock_store_id);
    req->set_upload_seq(upload_seq);

    auto res = std::make_shared<disaggregated::TryAddLockResponse>();
    auto call = pingcap::kv::RpcCall<disaggregated::TryAddLockRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryAddLock request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryAddLock response, resp={}", resp->DebugString());
    if (resp->has_error())
    {
        LOG_ERROR(log, "TryMarkDelete get resp with error={}", resp->error().DebugString());
        return {false, resp->error()};
    }

    return {resp->is_success(), std::nullopt};
}

std::pair<bool, std::optional<disaggregated::S3LockError>> S3LockClient::sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id)
{
    auto req = std::make_shared<disaggregated::TryMarkDeleteRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);

    auto res = std::make_shared<disaggregated::TryMarkDeleteResponse>();
    auto call = pingcap::kv::RpcCall<disaggregated::TryMarkDeleteRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryMarkDelete request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryMarkDelete response, resp={}", resp->DebugString());
    if (resp->has_error())
    {
        LOG_ERROR(log, "TryMarkDelete get resp with error={}", resp->error().DebugString());
        return {false, resp->error()};
    }

    return {resp->is_success(), std::nullopt};
}


bool S3LockService::tryAddLockImpl(const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq, disaggregated::TryAddLockResponse * response)
{
    if (ori_data_file.empty())
        return false;

    String data_file_name;
    if (ori_data_file[0] == 't')
        data_file_name = fmt::format("/s{}/stable/{}", ori_store_id, ori_data_file);
    else if (ori_data_file[0] == 'd')
        data_file_name = fmt::format("/s{}/data/{}", ori_store_id, ori_data_file);
    else
        return false;

    String delete_file_name = fmt::format("{}.del", data_file_name);
    String lock_file_name = fmt::format("/s{}/lock/{}.lock_{}_{}", ori_store_id, ori_data_file, lock_store_id, upload_seq);

    // Get the lock of the file
    DataFileMutexPtr file_lock;
    {
        std::unique_lock lock(file_latch_map_mutex);
        auto it = file_latch_map.find(data_file_name);
        if (it == file_latch_map.end())
        {
            it = file_latch_map.emplace(data_file_name, std::make_shared<DataFileMutex>()).first;
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
            file_latch_map.erase(data_file_name);
        }
    });

    // make sure data file exists
    if (!DB::S3::objectExists(*s3_client, bucket_name, data_file_name))
    {
        response->mutable_error()->mutable_err_data_file_is_missing();
        return false;
    }

    // make sure data file is not mark as deleted
    if (DB::S3::objectExists(*s3_client, bucket_name, delete_file_name))
    {
        response->mutable_error()->mutable_err_data_file_is_deleted();
        return false;
    }

    // upload lock file
    try
    {
        DB::S3::uploadFile(*s3_client, bucket_name, tmp_empty_file, lock_file_name);
    }
    catch (...)
    {
        response->mutable_error()->mutable_err_add_lock_file_fail();
        return false;
    }

    return true;
}

bool S3LockService::tryMarkDeleteImpl(String data_file, UInt64 ori_store_id, disaggregated::TryMarkDeleteResponse * response)
{
    if (data_file.empty())
        return false;

    String data_file_name;
    if (data_file[0] == 't')
        data_file_name = fmt::format("/s{}/stable/{}", ori_store_id, data_file);
    else if (data_file[0] == 'd')
        data_file_name = fmt::format("/s{}/data/{}", ori_store_id, data_file);
    else
        return false;

    String lock_file_name_prefix = fmt::format("/s{}/lock/{}.", ori_store_id, data_file);
    String delete_file_name = fmt::format("{}.del", data_file_name);

    // Get the lock of the file
    DataFileMutexPtr file_lock;
    {
        std::unique_lock lock(file_latch_map_mutex);
        auto it = file_latch_map.find(data_file_name);
        if (it == file_latch_map.end())
        {
            it = file_latch_map.emplace(data_file_name, std::make_shared<DataFileMutex>()).first;
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
            file_latch_map.erase(data_file_name);
        }
    });

    // make sure data file has not been locked
    if (DB::S3::getListPrefixSize(*s3_client, bucket_name, lock_file_name_prefix) > 0)
    {
        response->mutable_error()->mutable_err_data_file_is_locked();
        return false;
    }

    // upload delete file
    try
    {
        DB::S3::uploadFile(*s3_client, bucket_name, tmp_empty_file, delete_file_name);
    }
    catch (...)
    {
        response->mutable_error()->mutable_err_add_delete_file_fail();
        return false;
    }

    return true;
}

} // namespace DB::Management