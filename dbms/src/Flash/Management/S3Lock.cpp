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

#include <Storages/Transaction/TMTContext.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fmt/core.h>
#include <grpcpp/client_context.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/conn.h>

#include <ext/scope_guard.h>


namespace pingcap::kv
{
// The rpc trait
template <>
struct RpcTypeTraits<kvrpcpb::TryAddLockRequest>
{
    using RequestType = kvrpcpb::TryAddLockRequest;
    using ResultType = kvrpcpb::TryAddLockResponse;
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
struct RpcTypeTraits<kvrpcpb::TryMarkDeleteRequest>
{
    using RequestType = kvrpcpb::TryMarkDeleteRequest;
    using ResultType = kvrpcpb::TryMarkDeleteResponse;
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

bool S3LockService::sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq)
{
    auto req = std::make_shared<kvrpcpb::TryAddLockRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);
    req->set_lock_store_id(lock_store_id);
    req->set_upload_seq(upload_seq);

    auto res = std::make_shared<kvrpcpb::TryAddLockResponse>();
    auto call = pingcap::kv::RpcCall<kvrpcpb::TryAddLockRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryAddLock request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryAddLock response.");
    // TODO: handle error
    if (!resp->error_msg().empty())
        throw Exception(fmt::format("TryAddLock get resp with error={}", resp->error_msg()));

    return resp->is_success();
}

bool S3LockService::sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id)
{
    auto req = std::make_shared<kvrpcpb::TryMarkDeleteRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);

    auto res = std::make_shared<kvrpcpb::TryMarkDeleteResponse>();
    auto call = pingcap::kv::RpcCall<kvrpcpb::TryMarkDeleteRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryMarkDelete request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryMarkDelete response.");

    if (!resp->error_msg().empty())
        throw Exception(fmt::format("TryMarkDelete get resp with error={}", resp->error_msg()));

    return resp->is_success();
}


bool S3LockService::tryAddLockImpl(const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq)
{
    if (ori_data_file.empty())
        return false;

    String data_file_name;
    if (ori_data_file[0] == 't')
        data_file_name = fmt::format("s{}/stable/{}", ori_store_id, ori_data_file);
    else if (ori_data_file[0] == 'd')
        data_file_name = fmt::format("s{}/data/{}", ori_store_id, ori_data_file);
    else
        return false;

    String lock_file_name = fmt::format("s{}/lock/{}.lock_{}_{}", ori_store_id, ori_data_file, lock_store_id, upload_seq);

    // Get the lock of the file
    std::unordered_map<std::string, DataFileMutexPtr>::iterator it;
    {
        std::shared_lock lock(file_latch_map_mutex);
        it = file_latch_map.find(data_file_name);
        if (it == file_latch_map.end())
        {
            std::unique_lock lock(file_latch_map_mutex);
            it = file_latch_map.emplace(data_file_name, std::make_shared<DataFileMutex>()).first;
        }
    }
    auto & file_lock = it->second;

    Aws::S3::S3Client s3_client(client_config);

    file_lock->lock();
    SCOPE_EXIT({
        file_lock->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        if (file_lock->getRefCount() == 0)
        {
            file_latch_map.erase(data_file_name);
        }
    });

    {
        Aws::S3::Model::GetObjectRequest request;
        request.WithBucket(bucket_name);
        request.WithKey(data_file_name);

        auto outcome = s3_client.GetObject(request);

        if (!outcome.IsSuccess())
            return false;
    }

    {
        Aws::S3::Model::GetObjectRequest request;
        request.WithBucket(bucket_name);
        request.WithKey(lock_file_name);

        auto outcome = s3_client.GetObject(request);

        if (outcome.IsSuccess())
            return false;
    }

    {
        Aws::S3::Model::PutObjectRequest request;
        request.WithBucket(bucket_name);
        request.WithKey(lock_file_name);

        auto outcome = s3_client.PutObject(request);

        if (!outcome.IsSuccess())
            return false;
    }

    return true;
}

bool S3LockService::tryMarkDeleteImpl(String data_file, UInt64 ori_store_id)
{
    if (data_file.empty())
        return false;

    String data_file_name;
    if (data_file[0] == 't')
        data_file_name = fmt::format("s{}/stable/{}", ori_store_id, data_file);
    else if (data_file[0] == 'd')
        data_file_name = fmt::format("s{}/data/{}", ori_store_id, data_file);
    else
        return false;

    String lock_file_name_prefix = fmt::format("s{}/lock/{}.", ori_store_id, data_file);
    String delete_file_name = fmt::format("s{}/lock/{}.del", ori_store_id, data_file);

    // Get the lock of the file
    std::unordered_map<std::string, DataFileMutexPtr>::iterator it;
    {
        std::shared_lock lock(file_latch_map_mutex);
        it = file_latch_map.find(data_file_name);
        if (it == file_latch_map.end())
        {
            std::unique_lock lock(file_latch_map_mutex);
            it = file_latch_map.emplace(data_file_name, std::make_shared<DataFileMutex>()).first;
        }
    }
    auto & file_lock = it->second;

    Aws::S3::S3Client s3_client(client_config);

    file_lock->lock();
    SCOPE_EXIT({
        file_lock->unlock();
        std::unique_lock lock(file_latch_map_mutex);
        if (file_lock->getRefCount() == 0)
        {
            file_latch_map.erase(data_file_name);
        }
    });

    {
        Aws::S3::Model::ListObjectsRequest request;
        request.WithBucket(bucket_name);
        request.WithPrefix(lock_file_name_prefix);

        auto outcome = s3_client.ListObjects(request);
        if (!outcome.IsSuccess())
            return false;

        const auto & result = outcome.GetResult().GetContents();

        if (!result.empty())
            return false;
    }

    {
        Aws::S3::Model::PutObjectRequest request;
        request.WithBucket(bucket_name);
        request.WithKey(delete_file_name);

        auto outcome = s3_client.PutObject(request);

        if (!outcome.IsSuccess())
            return false;
    }

    return true;
}

} // namespace DB::Management