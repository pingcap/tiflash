
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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadFactory.h>
#include <Interpreters/Context.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerInfo.h>
#include <TiDB/OwnerManager.h>
#include <common/logger_useful.h>
#include <etcd/v3election.grpc.pb.h>

#include <chrono>
#include <condition_variable>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <mutex>

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

static constexpr std::string_view S3GCOwnerKey = "/tiflash/s3gc/owner";

OwnerManagerPtr
OwnerManager::createS3GCOwner(
    Context & context,
    std::string_view id,
    const Etcd::ClientPtr & client,
    Int64 owner_ttl)
{
    return std::make_unique<OwnerManager>(context, S3GCOwnerKey, id, client, owner_ttl);
}

OwnerManager::OwnerManager(
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
    , log(Logger::get(fmt::format("name:{} id:{}", campaign_name, id)))
{
}

OwnerManager::~OwnerManager()
{
    cancel();
}

void OwnerManager::cancel()
{
    {
        std::unique_lock lk(mtx_camaign);
        state = CancelByCaller;
    }
    cv_camaign.notify_all();

    if (keep_alive_handle)
    {
        auto & bkg_pool = global_ctx.getBackgroundPool();
        bkg_pool.removeTask(keep_alive_handle);
        keep_alive_handle = nullptr;
    }
    if (th_camaign.joinable())
    {
        th_camaign.join();
    }
    if (th_watch_owner.joinable())
    {
        th_watch_owner.join();
    }
}

void OwnerManager::campaignOwner()
{
    auto session = createEtcdSession();
    RUNTIME_CHECK_MSG(session != nullptr, "failed to create etcd session");

    LOG_INFO(log, "start campaign owner");
    th_camaign = ThreadFactory::newThread(
        false,
        /*thread_name*/ "OwnerMgr",
        [this, s = std::move(session)] {
            camaignLoop(s);
        });
}

std::pair<bool, Etcd::SessionPtr>
OwnerManager::runNextCampaign(Etcd::SessionPtr && old_session)
{
    bool run_next_campaign = true;
    std::unique_lock lk(mtx_camaign);
    switch (state)
    {
    case Normal:
        break;
    case CancelByCaller:
    {
        // Caller cancel the campaign, maybe program is exiting
        run_next_campaign = false;
        lk.unlock();

        LOG_INFO(log, "break campaign loop, disabled");
        revokeEtcdSession(old_session->leaseID());
        break;
    }
    case CancelByKeyDeleted:
    {
        // try next campaign with the same etcd session (lease_id)
        state = Normal;
        break;
    }
    case CancelByLeaseInvalid:
    {
        // try next campaign with new etcd session
        state = Normal;
        lk.unlock();

        LOG_INFO(log, "etcd session is expired, create a new one");
        auto old_lease_id = old_session->leaseID();
        // Start a new session
        old_session = createEtcdSession();
        if (old_session == nullptr)
        {
            LOG_INFO(log, "break campaign loop, create session failed");
            revokeEtcdSession(old_lease_id); // try revoke old lease id
            run_next_campaign = false;
        }
        break;
    }
    }
    return {run_next_campaign, old_session};
}

void OwnerManager::camaignLoop(Etcd::SessionPtr session)
{
    try
    {
        while (true)
        {
            bool run_next_campaign = true;
            std::tie(run_next_campaign, session) = runNextCampaign(std::move(session));
            if (!run_next_campaign)
                break;

            const auto lease_id = session->leaseID();
            LOG_DEBUG(log, "new campaign loop with lease_id={:x}", lease_id);
            Etcd::LeaderKey new_leader;
            grpc::Status status;
            std::tie(new_leader, status) = client->campaign(campaign_name, id, lease_id);
            if (!status.ok())
            {
                // if error, continue next campaign
                LOG_INFO(
                    log,
                    "failed to campaign, id={} lease={:x} code={} msg={}",
                    id,
                    lease_id,
                    status.error_code(),
                    status.error_message());
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
            th_watch_owner = ThreadFactory::newThread(
                false,
                "OwnerWatch",
                [this, key = owner_key.value(), &watch_ctx] {
                    watchOwner(key, &watch_ctx);
                });

            {
                // etcd session expired / owner key get deleted / caller cancel,
                // it means this node is not owner anymore
                std::unique_lock lk(mtx_camaign);
                cv_camaign.wait(lk, [this] { return state != State::Normal; });
                LOG_INFO(log, "{}", magic_enum::enum_name(state));
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

void OwnerManager::toBeOwner(Etcd::LeaderKey && leader_key)
{
    RUNTIME_CHECK(!leader_key.name().empty(), leader_key.ShortDebugString());

    {
        std::lock_guard lk(mtx_leader);
        leader.Swap(&leader_key);
    }

    if (be_owner)
        be_owner();
}

void OwnerManager::watchOwner(const String & owner_key, grpc::ClientContext * watch_ctx)
{
    try
    {
        SCOPE_EXIT({
            {
                std::unique_lock lk(mtx_camaign);
                // do not overwrite cancel by caller
                if (state != CancelByCaller)
                {
                    state = CancelByKeyDeleted;
                }
            }
            // notify the campaign thread to start a new election
            cv_camaign.notify_all();
        });

        auto rw = client->watch(watch_ctx);

        etcdserverpb::WatchRequest req;
        req.mutable_create_request()->set_key(owner_key);
        bool ok = rw->Write(req);
        if (!ok)
        {
            rw->Finish();
            return;
        }

        bool key_deleted = false;
        while (!key_deleted)
        {
            etcdserverpb::WatchResponse resp;
            ok = rw->Read(&resp);
            LOG_TRACE(log, "watch key:{} ok:{} resp: {}", owner_key, ok, resp.ShortDebugString());
            if (!ok)
                break;
            for (const auto & event : resp.events())
            {
                if (event.type() == mvccpb::Event_EventType_DELETE)
                {
                    key_deleted = true;
                    break;
                }
            }
        }
        rw->Finish();
    }
    catch (...)
    {
        tryLogCurrentException("OwnerManager::watchOwner");
    }
}

std::optional<String> OwnerManager::getOwnerKey(const String & expect_id)
{
    const auto & [kv, status] = client->leader(campaign_name);
    if (!status.ok())
    {
        LOG_INFO(log, "failed to get leader, code={} msg={}", status.error_code(), status.error_message());
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

bool OwnerManager::isOwner()
{
    std::lock_guard lk(mtx_leader);
    return !leader.name().empty();
}

void OwnerManager::retireOwner()
{
    std::lock_guard lk(mtx_leader);
    leader.Clear();
}

bool OwnerManager::resignOwner()
{
    std::lock_guard lk(mtx_leader);
    // this node is not
    if (leader.name().empty())
        return false;

    client->resign(std::move(leader));
    leader.Clear();
    // resign owner success
    LOG_WARNING(log, "resign owner success");
    return true;
}

Etcd::SessionPtr OwnerManager::createEtcdSession()
{
    auto & bkg_pool = global_ctx.getBackgroundPool();
    if (keep_alive_handle)
    {
        bkg_pool.removeTask(keep_alive_handle);
        keep_alive_handle = nullptr;
    }

    auto session = client->createSession(&keep_alive_ctx, leader_ttl);
    keep_alive_handle = bkg_pool.addTask(
        [this, s = session] {
            if (!s->keepAliveOne())
            {
                {
                    std::unique_lock lk(mtx_camaign);
                    // do not overwrite cancel by caller
                    if (state != CancelByCaller)
                    {
                        state = CancelByLeaseInvalid;
                    }
                }
                // notify the campaign thread to start a new election
                cv_camaign.notify_all();
            }
            return false;
        },
        /*multi*/ false,
        /*interval_ms*/ leader_ttl * 1000 * 2 / 3);
    return session;
}

void OwnerManager::revokeEtcdSession(Etcd::LeaseID lease_id)
{
    // revoke the session lease
    // if revoke takes longer than the ttl, lease is expired anyway. it is safe to ignore error here.
    auto status = client->leaseRevoke(lease_id);
    LOG_INFO(log, "revoke session, code={} msg={}", status.error_code(), status.error_message());
}

OwnerInfo OwnerManager::getOwnerID()
{
    if (isOwner())
    {
        return OwnerInfo{
            .status = OwnerType::IsOwner,
            .owner_id = id,
        };
    }

    // This node is not the owner, get the owner id from etcd
    const auto & [val, status] = client->getFirstKey(campaign_name);
    if (!status.ok())
        return OwnerInfo{
            .status = OwnerType::GrpcError,
            .owner_id = fmt::format("code={} msg={}", status.error_code(), status.error_message()),
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
