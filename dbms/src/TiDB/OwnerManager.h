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

#pragma once

#include <Storages/BackgroundProcessingPool.h>
#include <TiDB/OwnerInfo.h>
#include <common/types.h>

#include <condition_variable>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <etcd/v3election.pb.h>
#include <grpcpp/client_context.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <thread>

namespace DB
{
class Context;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

namespace Etcd
{
class Client;
using ClientPtr = std::shared_ptr<Client>;
class Session;
using SessionPtr = std::shared_ptr<Session>;

using LeaseID = Int64;
using LeaderKey = v3electionpb::LeaderKey;
} // namespace Etcd

class OwnerManager;
using OwnerManagerPtr = std::unique_ptr<OwnerManager>;


class OwnerManager
{
public:
    static OwnerManagerPtr
    createS3GCOwner(
        Context & context,
        std::string_view id,
        const Etcd::ClientPtr & client,
        Int64 owner_ttl = 60);

    OwnerManager(
        Context & context,
        std::string_view campaign_name_,
        std::string_view id_,
        const Etcd::ClientPtr & client_,
        Int64 owner_ttl = 60);

    ~OwnerManager();

    // start a thread to campaign owner
    void campaignOwner();

    bool isOwner();

    // Return the owner info.
    // If this node is not the owner, it will try
    // to get the owner id from etcd.
    OwnerInfo getOwnerID();

    // If this node is the owner, resign, start a new
    // campaign and return true.
    // Return false if this node is not the owner.
    bool resignOwner();

    // cancel the campaign and waits till the camaign thread exit
    void cancel();

    // Set a callback after being owner. Set it before `campaignOwner`.
    // Export for testing
    void setBeOwnerHook(std::function<void()> && hook)
    {
        be_owner = hook;
    }

private:
    std::pair<bool, Etcd::SessionPtr> runNextCampaign(Etcd::SessionPtr && old_session);
    void camaignLoop(Etcd::SessionPtr session);

    std::optional<String> getOwnerKey(const String & expect_id);

    void toBeOwner(Etcd::LeaderKey && leader_key);

    void watchOwner(const String & owner_key, grpc::ClientContext * watch_ctx);

    void retireOwner();

    Etcd::SessionPtr createEtcdSessionWithRetry(Int64 max_retry);
    Etcd::SessionPtr createEtcdSession();
    void revokeEtcdSession(Etcd::LeaseID lease_id);

private:
    const String campaign_name;
    const String id;
    Etcd::ClientPtr client;
    const Int64 leader_ttl;

    enum class State
    {
        Init,
        Normal,
        // owner key deleted, retry
        CancelByKeyDeleted,
        // lease expired, retry
        CancelByLeaseInvalid,
        // cancel by another methd, won't retry
        CancelByCaller,
        // cancelled and all sub tasks end.
        // Can call `campaignOwner` again.
        CancelDone,
    };

    std::mutex mtx_camaign;
    State state = State::Normal;
    std::condition_variable cv_camaign;
    std::atomic<bool> enable_camaign{true};
    std::thread th_camaign;

    std::thread th_watch_owner;
    grpc::ClientContext keep_alive_ctx;
    BackgroundProcessingPool::TaskHandle keep_alive_handle{nullptr};

    std::mutex mtx_leader;
    Etcd::LeaderKey leader;

    std::function<void()> be_owner;

    Context & global_ctx;
    LoggerPtr log;
};
} // namespace DB
