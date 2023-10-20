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

#include <Storages/KVStore/Types.h>
#include <TiDB/OwnerManager.h>
#include <common/types.h>
#include <kvproto/disaggregated.pb.h>
#include <pingcap/kv/Cluster.h>

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB

namespace DB::S3
{

class IS3LockClient;
using S3LockClientPtr = std::shared_ptr<IS3LockClient>;

class IS3LockClient
{
public:
    virtual ~IS3LockClient() = default;

    // Try add lock to the `data_file_key` by `lock_store_id` and `lock_seq`
    // If the file is locked successfully, return <true, "">
    // Otherwise return <false, conflict_message>
    // This method will update the owner info when owner changed.
    // If deadline exceed or failed to get the owner info within
    // `timeout_s`, it will throw exception.
    virtual std::pair<bool, String> sendTryAddLockRequest(
        const String & data_file_key,
        UInt32 lock_store_id,
        UInt32 lock_seq,
        Int64 timeout_s)
        = 0;

    // Try mark the `data_file_key` as deleted
    // If the file is marked as deleted, return <true, "">
    // Otherwise return <false, conflict_message>
    // This method will update the owner info when owner changed.
    // If deadline exceed or failed to get the owner info within
    // `timeout_s`, it will throw exception.
    virtual std::pair<bool, String> sendTryMarkDeleteRequest(const String & data_file_key, Int64 timeout_s) = 0;
};

class S3LockClient : public IS3LockClient
{
public:
    explicit S3LockClient(pingcap::kv::Cluster * kv_cluster_, OwnerManagerPtr s3gc_owner_);

    // Try add lock to the `data_file_key` by `lock_store_id` and `lock_seq`
    // If the file is locked successfully, return <true, "">
    // Otherwise return <false, conflict_message>
    // This method will update the owner info when owner changed.
    // If deadline exceed or failed to get the owner info within
    // `timeout_s`, it will throw exception.
    std::pair<bool, String> sendTryAddLockRequest(
        const String & data_file_key,
        UInt32 lock_store_id,
        UInt32 lock_seq,
        Int64 timeout_s) override;

    // Try mark the `data_file_key` as deleted
    // If the file is marked as deleted, return <true, "">
    // Otherwise return <false, conflict_message>
    // This method will update the owner info when owner changed.
    // If deadline exceed or failed to get the owner info within
    // `timeout_s`, it will throw exception.
    std::pair<bool, String> sendTryMarkDeleteRequest(const String & data_file_key, Int64 timeout_s) override;

private:
    template <typename Response, typename Request, typename SendRpc>
    std::pair<bool, String> makeCall(SendRpc send, const Request & req, Int64 timeout_s, const LoggerPtr & tracing_log);

    String getOwnerAddr(const Timepoint & deadline, const LoggerPtr & tracing_log);
    String updateOwnerAddr(const Timepoint & deadline, const LoggerPtr & tracing_log);

private:
    pingcap::kv::Cluster * kv_cluster;
    OwnerManagerPtr s3gc_owner;

    mutable std::shared_mutex mtx_owner;
    String owner_cache;

    LoggerPtr log;
};

} // namespace DB::S3
