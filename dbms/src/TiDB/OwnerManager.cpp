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
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/ThreadFactory.h>
#include <Interpreters/Context.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/MockOwnerManager.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <common/logger_useful.h>
#include <etcd/v3election.grpc.pb.h>
#include <fiu.h>

#include <chrono>
#include <condition_variable>
#include <ext/scope_guard.h>
#include <limits>
#include <magic_enum.hpp>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/client_context.h>
#include <grpcpp/impl/codegen/call_op_set.h>
#include <grpcpp/support/sync_stream.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB
{
namespace FailPoints
{
extern const char force_owner_mgr_state[];
extern const char force_owner_mgr_campaign_failed[];
extern const char force_fail_to_create_etcd_session[];
} // namespace FailPoints

static constexpr std::string_view S3GCOwnerKey = "/tiflash/s3gc/owner";

OwnerManagerPtr OwnerManager::createMockOwner(std::string_view id)
{
    return std::make_shared<MockOwnerManager>(id);
}

OwnerManagerPtr OwnerManager::createS3GCOwner(
    Context & context,
    std::string_view id,
    const Etcd::ClientPtr & client,
    Int64 owner_ttl)
{
    // Notice: Need to add suffix for logger id to distingust owner type when
    // there is more than one kind of owner manager in one TiFlash instance.
    return std::make_shared<EtcdOwnerManager>(context, S3GCOwnerKey, id, client, owner_ttl);
}

EtcdOwnerManager::EtcdOwnerManager(
    Context & context,
    std::string_view campaign_name_,
    std::string_view id_,
    const Etcd::ClientPtr & client_,
    Int64 owner_ttl)
    : campaign_name(String(campaign_name_))
    , id(String(id_))
    , client(client_)
    , leader_ttl(owner_ttl)
    , global_ctx(context.getGlobalContext())
    , log(Logger::get(fmt::format("owner_id={}", id)))
{}

EtcdOwnerManager::~EtcdOwnerManager()
{
    cancelImpl(); // avoid calling virtual method inside dctor
}

void EtcdOwnerManager::cancel()
{
    cancelImpl();
}

void EtcdOwnerManager::cancelImpl()
{
    {
        std::unique_lock lk(mtx_camaign);
        state = State::CancelByStop;
    }
    cv_camaign.notify_all();

    auto & bkg_pool = global_ctx.getBackgroundPool();
    if (keep_alive_handle)
    {
        bkg_pool.removeTask(keep_alive_handle);
        keep_alive_handle.reset();
        keep_alive_ctx.reset();
    }
    if (session_check_handle)
    {
        bkg_pool.removeTask(session_check_handle);
        session_check_handle.reset();
    }
    if (th_camaign.joinable())
    {
        {
            std::unique_lock lock(mtx_camaign);
            if (campaing_ctx)
                campaing_ctx->TryCancel();
        }
        th_camaign.join();
    }
    if (th_watch_owner.joinable())
    {
        th_watch_owner.join();
    }

    assert(state == State::CancelByStop); // should not be overwrite
    {
        std::unique_lock lk(mtx_camaign);
        state = State::CancelDone;
    }
}

void EtcdOwnerManager::campaignOwner()
{
    {
        std::unique_lock lk(mtx_camaign);
        if (state != State::Init && state != State::CancelDone)
        {
            LOG_INFO(log, "campaign is already running");
            return;
        }

        state = State::Normal;
    }

    auto session = createEtcdSessionWithRetry(3); // retry 3 time
    RUNTIME_CHECK_MSG(session != nullptr, "failed to create etcd session");

    LOG_INFO(log, "start campaign owner");
    th_camaign = ThreadFactory::newThread(
        false,
        /*thread_name*/ "OwnerMgr",
        [this, s = std::move(session)] { camaignLoop(s); });
}

std::pair<bool, Etcd::SessionPtr> EtcdOwnerManager::runNextCampaign(Etcd::SessionPtr && old_session)
{
    bool run_next_campaign = true;
    std::unique_lock lk(mtx_camaign);

    fiu_do_on(FailPoints::force_owner_mgr_state, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_owner_mgr_state); v)
        {
            auto s = std::any_cast<EtcdOwnerManager::State>(v.value());
            LOG_WARNING(
                log,
                "state change by failpoint {} -> {}",
                magic_enum::enum_name(state),
                magic_enum::enum_name(s));
            state = s;
        }
    });

    switch (state)
    {
    case State::Normal:
        break;
    case State::CancelByStop:
    {
        // the campaign is stopping, maybe program is exiting
        run_next_campaign = false;
        lk.unlock();

        LOG_INFO(log, "break campaign loop, disabled");
        revokeEtcdSession(old_session->leaseID());
        break;
    }
    case State::CancelByKeyDeleted:
    {
        // try next campaign with the same etcd session (lease_id)
        state = State::Normal;
        break;
    }
    case State::CancelByLeaseInvalid:
    {
        // try next campaign with new etcd session
        state = State::Normal;
        lk.unlock(); // unlock for network request

        LOG_INFO(log, "etcd session is expired, create a new one");
        auto old_lease_id = old_session->leaseID();
        // Start a new session with inf retries
        old_session = createEtcdSessionWithRetry(std::numeric_limits<Int64>::max());
        if (old_session == nullptr)
        {
            LOG_INFO(log, "break campaign loop, create session failed");
            revokeEtcdSession(old_lease_id); // try revoke old lease id

            run_next_campaign = false;
        }
        break;
    }
    case State::Init:
    case State::CancelDone:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected state: {}", magic_enum::enum_name(state));
    }
    return {run_next_campaign, old_session};
}

void EtcdOwnerManager::tryChangeState(State coming_state)
{
    std::unique_lock lk(mtx_camaign);
    // When the campaign is stopping, it will cause
    // - leader key deleted in watch thread => try set state to `CancelByKeyDeleted`
    // - etcd lease expired in keepalive task => try set state to `CancelByLeaseInvalid`
    // Do not let those actually overwrite `CancelByStop` because the next campaign is expected to be stopped.
    if (state != State::CancelByStop)
    {
        // ok to set the state
        state = coming_state;
    }
}

void EtcdOwnerManager::camaignLoop(Etcd::SessionPtr session)
{
    try
    {
        while (true)
        {
            bool run_next_campaign = true;
            Etcd::SessionPtr old_session = session; // workaround for clang-tidy
            std::tie(run_next_campaign, session) = runNextCampaign(std::move(old_session));
            if (!run_next_campaign)
                break;

            const auto lease_id = session->leaseID();
            LOG_DEBUG(log, "new campaign loop with lease_id={:x}", lease_id);
            // Let this thread blocks until becone owner or error occurs
            {
                std::unique_lock lock(mtx_camaign);
                campaing_ctx = std::make_unique<grpc::ClientContext>();
            }
            auto && [new_leader, status] = client->campaign(campaing_ctx.get(), campaign_name, id, lease_id);
            fiu_do_on(FailPoints::force_owner_mgr_campaign_failed, {
                status = grpc::Status(grpc::StatusCode::UNKNOWN, "<mock error> etcdserver: requested lease not found");
                LOG_WARNING(log, "force_owner_mgr_campaign_failed enabled, return failed grpc::Status");
            });
            if (!status.ok())
            {
                // if error, continue next campaign
                LOG_INFO(
                    log,
                    "failed to campaign, id={} lease={:x} code={} msg={}",
                    id,
                    lease_id,
                    magic_enum::enum_name(status.error_code()),
                    status.error_message());
                // The error is possible cause by lease invalid, create a new etcd
                // session next round.
                tryChangeState(State::CancelByLeaseInvalid);
                static constexpr std::chrono::milliseconds CampaignRetryInterval(200);
                std::this_thread::sleep_for(CampaignRetryInterval);
                continue;
            }

            auto owner_key = getOwnerKey(id);
            if (!owner_key)
            {
                // if error, continue
                continue;
            }

            // become owner
            toBeOwner(std::move(new_leader));
            LOG_INFO(log, "become the owner with lease={:x}", lease_id);

            grpc::ClientContext watch_ctx;
            // waits until owner key get expired and deleted
            th_watch_owner = ThreadFactory::newThread(false, "OwnerWatch", [this, key = owner_key.value(), &watch_ctx] {
                watchOwner(key, &watch_ctx);
            });

            {
                // etcd session expired / owner key get deleted / caller cancel,
                // it means this node is not owner anymore
                std::unique_lock lk(mtx_camaign);
                cv_camaign.wait(lk, [this] { return state != State::Normal; });
            }

            watch_ctx.TryCancel();
            th_watch_owner.join();
            retireOwner();

            LOG_WARNING(log, "is not the owner");
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "OwnerManager::camaignLoop");
    }
}

void EtcdOwnerManager::toBeOwner(Etcd::LeaderKey && leader_key)
{
    RUNTIME_CHECK(!leader_key.name().empty(), leader_key.ShortDebugString());

    {
        std::lock_guard lk(mtx_leader);
        leader.Swap(&leader_key);
    }

    if (be_owner)
        be_owner();
}

void EtcdOwnerManager::watchOwner(const String & owner_key, grpc::ClientContext * watch_ctx)
{
    try
    {
        SCOPE_EXIT({
            tryChangeState(State::CancelByKeyDeleted);
            // notify the campaign thread to start a new election
            cv_camaign.notify_all();
        });

        auto rw = client->watch(watch_ctx);

        etcdserverpb::WatchRequest req;
        req.mutable_create_request()->set_key(owner_key);
        bool ok = rw->Write(req);
        if (!ok)
        {
            auto s = rw->Finish();
            LOG_DEBUG(log, "watch finish, code={} msg={}", magic_enum::enum_name(s.error_code()), s.error_message());
            return;
        }

        bool key_deleted = false;
        while (!key_deleted)
        {
            etcdserverpb::WatchResponse resp;
            ok = rw->Read(&resp);
            LOG_TRACE(log, "watch key={} ok={} resp={}", owner_key, ok, resp.ShortDebugString());
            if (!ok)
            {
                LOG_INFO(log, "watcher is closed");
                break;
            }
            if (resp.canceled())
            {
                LOG_INFO(log, "watch cancel");
                break;
            }
            for (const auto & event : resp.events())
            {
                if (event.type() == mvccpb::Event_EventType_DELETE)
                {
                    LOG_INFO(log, "watch failed, owner key is deleted");
                    key_deleted = true;
                    break;
                }
            }
        } // loop until key deleted or failed
        auto s = rw->Finish();
        LOG_INFO(log, "watch finish, code={} msg={}", magic_enum::enum_name(s.error_code()), s.error_message());
    }
    catch (...)
    {
        tryLogCurrentException(log, "OwnerManager::watchOwner");
    }
}

std::optional<String> EtcdOwnerManager::getOwnerKey(const String & expect_id)
{
    const auto & [kv, status] = client->leader(campaign_name);
    if (!status.ok())
    {
        LOG_INFO(
            log,
            "failed to get leader, code={} msg={}",
            magic_enum::enum_name(status.error_code()),
            status.error_message());
        return std::nullopt;
    }
    // Check whether the owner id get from etcd is the same as this node
    // If not, it means the leadership is expired. Start next campaign.
    const auto & owner_id = kv.value();
    if (owner_id != expect_id)
    {
        LOG_WARNING(log, "is not the owner");
        return std::nullopt;
    }
    return kv.key();
}

bool EtcdOwnerManager::isOwner()
{
    std::lock_guard lk(mtx_leader);
    return !leader.name().empty();
}

void EtcdOwnerManager::retireOwner()
{
    std::lock_guard lk(mtx_leader);
    leader.Clear();
}

bool EtcdOwnerManager::resignOwner()
{
    std::lock_guard lk(mtx_leader);
    // this node is not the owner, can not resign
    if (leader.name().empty())
        return false;

    // resign and start next campaign
    client->resign(std::move(leader));
    leader.Clear();
    // resign owner success
    LOG_WARNING(log, "resign owner success");
    return true;
}

namespace session
{
static constexpr std::chrono::milliseconds RetryInterval(200);
static constexpr Int64 ErrorLogInterval = 15;
} // namespace session

Etcd::SessionPtr EtcdOwnerManager::createEtcdSessionWithRetry(Int64 max_retry)
{
    for (Int64 i = 0; i < max_retry; ++i)
    {
        if (auto session = createEtcdSession(); session)
            return session;

        if (i % session::ErrorLogInterval)
            LOG_WARNING(log, "fail to create new etcd session");
        std::this_thread::sleep_for(session::RetryInterval);
    }
    return {};
}

Etcd::SessionPtr EtcdOwnerManager::createEtcdSession()
{
    auto & bkg_pool = global_ctx.getBackgroundPool();
    if (session_check_handle)
    {
        bkg_pool.removeTask(session_check_handle);
        session_check_handle.reset();
    }
    if (keep_alive_handle)
    {
        bkg_pool.removeTask(keep_alive_handle);
        keep_alive_handle.reset();
        keep_alive_ctx.reset();
    }

    keep_alive_ctx = std::make_unique<grpc::ClientContext>();
    auto session = client->createSession(keep_alive_ctx.get(), leader_ttl);
    fiu_do_on(FailPoints::force_fail_to_create_etcd_session, { session = nullptr; });
    if (!session)
    {
        // create failed, skip adding keep alive tasks
        return {};
    }

    keep_alive_handle = bkg_pool.addTask(
        [this, s = session] {
            if (!s->keepAliveOne())
            {
                tryChangeState(State::CancelByLeaseInvalid);
                // notify the campaign thread to start a new election
                cv_camaign.notify_all();
            }
            return false;
        },
        /*multi*/ false,
        /*interval_ms*/ leader_ttl * 1000 * 2 / 3);
    session_check_handle = bkg_pool.addTask(
        [this, s = session] {
            if (s->isValid())
                return false;
            tryChangeState(State::CancelByLeaseInvalid);
            // notify the campaign thread to start a new election
            cv_camaign.notify_all();
            return false;
        },
        /*multi*/ false,
        /*interval_ms*/ 5 * 1000);
    return session;
}

void EtcdOwnerManager::revokeEtcdSession(Etcd::LeaseID lease_id)
{
    // revoke the session lease
    // if revoke takes longer than the ttl, lease is expired anyway. it is safe to ignore error here.
    auto status = client->leaseRevoke(lease_id);
    LOG_INFO(log, "revoke session, code={} msg={}", magic_enum::enum_name(status.error_code()), status.error_message());
}

OwnerInfo EtcdOwnerManager::getOwnerID()
{
    if (isOwner())
    {
        return OwnerInfo{
            .status = OwnerType::IsOwner,
            .owner_id = id,
        };
    }

    // This node is not the owner, get the owner id from etcd
    const auto & [val, status] = client->getFirstCreateKey(campaign_name);
    if (!status.ok())
        return OwnerInfo{
            .status = OwnerType::GrpcError,
            .owner_id
            = fmt::format("code={} msg={}", magic_enum::enum_name(status.error_code()), status.error_message()),
        };
    if (val.empty())
        return OwnerInfo{
            .status = OwnerType::NoLeader,
            .owner_id = "",
        };
    return OwnerInfo{
        .status = OwnerType::NotOwner,
        .owner_id = val,
    };
}

} // namespace DB
