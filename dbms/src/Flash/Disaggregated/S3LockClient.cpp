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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/OwnerInfo.h>
#include <common/defines.h>
#include <common/types.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status_code_enum.h>
#include <kvproto/disaggregated.pb.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/conn.h>

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>
#include <thread>

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
}

namespace DB::S3
{
S3LockClient::S3LockClient(pingcap::kv::Cluster * kv_cluster_, OwnerManagerPtr s3gc_owner_)
    : kv_cluster(kv_cluster_)
    , s3gc_owner(s3gc_owner_)
    , log(Logger::get())
{}

std::pair<bool, String> S3LockClient::sendTryAddLockRequest(
    const String & data_file_key,
    UInt32 lock_store_id,
    UInt32 lock_seq,
    Int64 timeout_s)
{
    using Req = disaggregated::TryAddLockRequest;
    using Resp = disaggregated::TryAddLockResponse;
    Req req;
    req.set_data_file_key(data_file_key);
    req.set_lock_store_id(lock_store_id);
    req.set_lock_seq(lock_seq);
    auto tracing_log
        = log->getChild(fmt::format("<key=<{},{},{}>,type=AddLock>", data_file_key, lock_store_id, lock_seq));

    return makeCall<Resp>(
        [](grpc::ClientContext * grpc_context,
           const std::shared_ptr<pingcap::kv::KvConnClient> & kvstub,
           const Req & req,
           Resp * response) { return kvstub->stub->tryAddLock(grpc_context, req, response); },
        req,
        timeout_s,
        tracing_log);
}

std::pair<bool, String> S3LockClient::sendTryMarkDeleteRequest(const String & data_file_key, Int64 timeout_s)
{
    using Req = disaggregated::TryMarkDeleteRequest;
    using Resp = disaggregated::TryMarkDeleteResponse;
    Req req;
    req.set_data_file_key(data_file_key);
    auto tracing_log = log->getChild(fmt::format("<key={},type=MarkDelete>", data_file_key));

    return makeCall<Resp>(
        [](grpc::ClientContext * grpc_context,
           const std::shared_ptr<pingcap::kv::KvConnClient> & kvstub,
           const Req & req,
           Resp * response) { return kvstub->stub->tryMarkDelete(grpc_context, req, response); },
        req,
        timeout_s,
        tracing_log);
}

// Try send the response to GC Owner in timeout_s seconds
// If the call success, return <true, "">
// Otherwise return <false, conflict_message>
// This method will update the owner info when owner changed.
// If deadline exceed or failed to get the owner info within
// `timeout_s`, it will throw exception.
template <typename Response, typename Request, typename SendRpc>
std::pair<bool, String> S3LockClient::makeCall(
    SendRpc send,
    const Request & req,
    Int64 timeout_s,
    const LoggerPtr & tracing_log)
{
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout_s);
    String address = getOwnerAddr(deadline, tracing_log);

    do
    {
        LOG_DEBUG(tracing_log, "request, address={} req={}", address, req.ShortDebugString());
        auto kvstub = kv_cluster->rpc_client->getConnArray(address)->get();
        grpc::ClientContext grpc_context;
        grpc_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(timeout_s));
        Response resp;
        auto status = send(&grpc_context, kvstub, req, &resp);
        if (!status.ok())
        {
            if (Clock::now() > deadline)
            {
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "deadline exceed, {}, address={}, request={}", tracing_log->identifier(), address, req.ShortDebugString());
            }
            // retry
            LOG_ERROR(
                tracing_log,
                "meets error, code={} msg={}",
                magic_enum::enum_name(status.error_code()),
                status.error_message());
            address = updateOwnerAddr(deadline, tracing_log);
        }

        if (resp.result().has_success())
        {
            // success
            return {true, ""};
        }
        else if (resp.result().has_conflict())
        {
            // not retriable
            LOG_INFO(tracing_log, "meets conflict, reason={}", resp.result().conflict().reason());
            return {false, resp.result().conflict().reason()};
        }
        else if (resp.result().has_not_owner())
        {
            if (Clock::now() > deadline)
            {
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "deadline exceed, {}, address={}, request={}", tracing_log->identifier(), address, req.ShortDebugString());
            }
            // retry
            auto not_owner = resp.result().not_owner();
            LOG_WARNING(tracing_log, "meets not owner, retry");
            address = updateOwnerAddr(deadline, tracing_log);
        }
    } while (true);
}

String S3LockClient::getOwnerAddr(const Timepoint & deadline, const LoggerPtr & tracing_log)
{
    {
        std::shared_lock lk(mtx_owner);
        if (!owner_cache.empty())
            return owner_cache;
    }
    // owner_cache is empty, try update
    return updateOwnerAddr(deadline, tracing_log);
}

String S3LockClient::updateOwnerAddr(const Timepoint & deadline, const LoggerPtr & tracing_log)
{
    using namespace std::chrono_literals;
    while (true)
    {
        auto owner_info = s3gc_owner->getOwnerID();
        switch (owner_info.status)
        {
        case DB::OwnerType::IsOwner:
        case DB::OwnerType::NotOwner:
        {
            {
                std::unique_lock lk(mtx_owner);
                owner_cache = owner_info.owner_id;
            }
            return owner_info.owner_id;
        }
        case DB::OwnerType::NoLeader:
        {
            if (Clock::now() > deadline)
            {
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "deadline exceed, owner not available, " + tracing_log->identifier());
            }
            LOG_ERROR(tracing_log, "owner not available");
            std::this_thread::sleep_for(500ms);
            break; // continue retry
        }
        case DB::OwnerType::GrpcError:
        {
            if (Clock::now() > deadline)
            {
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    fmt::format(
                        "deadline exceed, owner not available, msg={} {}",
                        owner_info.errMsg(),
                        tracing_log->identifier()));
            }
            LOG_ERROR(tracing_log, "owner not available, msg={}", owner_info.errMsg());
            std::this_thread::sleep_for(500ms);
            break; // continue retry
        }
        }
    }
}

} // namespace DB::S3
