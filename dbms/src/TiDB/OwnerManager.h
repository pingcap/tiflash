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

#include <Storages/BackgroundProcessingPool.h>
#include <TiDB/OwnerInfo.h>
#include <common/types.h>

#include <condition_variable>
#include <string_view>

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
using OwnerManagerPtr = std::shared_ptr<OwnerManager>;

namespace tests
{
class OwnerManagerTest;
}


class OwnerManager
{
public:
    static constexpr Int64 DefaultOwnerTTL = 60;

    static OwnerManagerPtr createS3GCOwner(
        Context & context,
        std::string_view id,
        const Etcd::ClientPtr & client,
        Int64 owner_ttl = DefaultOwnerTTL);

    static OwnerManagerPtr createMockOwner(std::string_view id);

    virtual ~OwnerManager() = default;

    // start a thread to campaign owner
    virtual void campaignOwner() = 0;

    // Quick check whether this node is owner or not
    virtual bool isOwner() = 0;

    // Return the owner info.
    // If this node is not the owner, it will try
    // to get the owner id from etcd.
    virtual OwnerInfo getOwnerID() = 0;

    // If this node is the owner, resign, start a new
    // campaign and return true.
    // Return false if this node is not the owner.
    virtual bool resignOwner() = 0;

    // cancel the campaign and waits till the camaign thread exit
    virtual void cancel() = 0;

    // Set a callback after being owner. Set it before `campaignOwner`.
    // Export for testing
    virtual void setBeOwnerHook(std::function<void()> && hook) = 0;
};

class EtcdOwnerManager : public OwnerManager
{
public:
    EtcdOwnerManager(
        Context & context,
        std::string_view campaign_name_,
        std::string_view id_,
        const Etcd::ClientPtr & client_,
        Int64 owner_ttl);

    ~EtcdOwnerManager() override;

    void campaignOwner() override;

    bool isOwner() override;

    OwnerInfo getOwnerID() override;

    bool resignOwner() override;

    void cancel() override;

    void setBeOwnerHook(std::function<void()> && hook) override { be_owner = hook; }

    friend class tests::OwnerManagerTest;

    enum class State
    {
        // inited but campaign is not running
        Init,
        // campaign is running
        Normal,
        // owner key deleted, retry
        CancelByKeyDeleted,
        // lease expired, retry
        CancelByLeaseInvalid,
        // cancel by `cancel()` method, won't retry
        // after all sub tasks end, the state will
        // be changed to `CancelDone`.
        CancelByStop,
        // cancelled and all sub tasks end.
        // Can call `campaignOwner` again.
        CancelDone,
    };

private:
    void cancelImpl();

    std::pair<bool, Etcd::SessionPtr> runNextCampaign(Etcd::SessionPtr && old_session);

    // handle state change when
    // - watch key is deleted
    // - etcd lease revoke
    void tryChangeState(State coming_state);

    void camaignLoop(Etcd::SessionPtr session);

    // get the owner key from etcd and check whether the owner value is `expect_id`
    std::optional<String> getOwnerKey(const String & expect_id);

    void toBeOwner(Etcd::LeaderKey && leader_key);

    // waits until owner key get expired and deleted
    void watchOwner(const String & owner_key, grpc::ClientContext * watch_ctx);

    void retireOwner();

    // create an etcd lease and keep the lease valid
    Etcd::SessionPtr createEtcdSessionWithRetry(Int64 max_retry);
    Etcd::SessionPtr createEtcdSession();

    // revoke the etcd lease
    void revokeEtcdSession(Etcd::LeaseID lease_id);

private:
    const String campaign_name;
    const String id;
    Etcd::ClientPtr client;
    const Int64 leader_ttl;

    std::mutex mtx_camaign;
    State state = State::Init;
    std::condition_variable cv_camaign;
    std::unique_ptr<grpc::ClientContext> campaing_ctx;

    // A thread for running camaign logic
    std::thread th_camaign;
    // A thread to watch whether the owner key's delete
    std::thread th_watch_owner;

    // Keep etcd lease valid
    std::unique_ptr<grpc::ClientContext> keep_alive_ctx;
    // A task to send lease keep alive, send lease keep alive every 2/3 ttl
    BackgroundProcessingPool::TaskHandle keep_alive_handle{nullptr};
    // A task to check lease is valid or not, cause `keep_alive_handle`
    // could be blocked for network issue.
    BackgroundProcessingPool::TaskHandle session_check_handle{nullptr};

    std::mutex mtx_leader;
    Etcd::LeaderKey leader;

    // a hook function for test
    std::function<void()> be_owner;

    Context & global_ctx;
    LoggerPtr log;
};
} // namespace DB
