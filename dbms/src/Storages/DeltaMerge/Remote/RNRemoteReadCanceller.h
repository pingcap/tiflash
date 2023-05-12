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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadManager.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadCanceller_fwd.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>
#include <kvproto/disaggregated.pb.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/Rpc.h>

#include <memory>
#include <mutex>
#include <set>

namespace DB
{
namespace DM
{

/// RNRemoteReadCanceller notifies Write Nodes that a read request is cancelled
/// forever and related resources should be released.
class RNRemoteReadCanceller
{
public:
    static RNRemoteReadCancellerPtr create(
        const Context & db_context,
        const DisaggTaskId & task_id)
    {
        return std::make_shared<RNRemoteReadCanceller>(db_context, task_id);
    }

    explicit RNRemoteReadCanceller(
        const Context & db_context,
        const DisaggTaskId & task_id_)
        : cluster(db_context.getTMTContext().getKVCluster())
        , task_id(task_id_)
    {}

    /// Thread-safe.
    void addStore(const String & addr)
    {
        std::unique_lock lock(mu);
        cancel_addrs.emplace(addr);
    }

    /// Thread-safe. Blocking.
    void notifyCancel()
    {
        bool old_val = false;
        if (is_cancelled.compare_exchange_strong(old_val, true))
            doNotifyCancel();
    }

private:
    void doNotifyCancel()
    {
        std::set<String> cancel_addrs_copy;
        {
            // Sending cancel requests may take time, so we keep the lock scope small.
            std::unique_lock lock(mu);
            cancel_addrs_copy = cancel_addrs;
        }

        auto req = std::make_shared<disaggregated::CancelDisaggTaskRequest>();
        req->mutable_meta()->CopyFrom(task_id.toMeta());
        auto call = pingcap::kv::RpcCall<disaggregated::CancelDisaggTaskRequest>(req);

        LOG_INFO(Logger::get(), "Read is cancelled, notify cancel to write nodes. task_id={} wn_addrs={}", task_id, cancel_addrs);

        auto thread_manager = newThreadManager();

        for (auto addr : cancel_addrs_copy)
        {
            thread_manager->schedule(false, "cancelRemoteRead", [&]() {
                try
                {
                    cluster->rpc_client->sendRequest(addr, call, /* timeout */ 5000);
                }
                catch (...)
                {
                    auto msg = getCurrentExceptionMessage(false);
                    LOG_WARNING(
                        Logger::get(),
                        "Failed to cancel disagg task, task_id={} addr={} msg={}",
                        task_id,
                        addr,
                        msg);
                }
            });
        }
        thread_manager->wait();
    }

    const pingcap::kv::Cluster * cluster;
    const DisaggTaskId task_id;

    std::mutex mu;
    std::set<String> cancel_addrs;

    std::atomic_bool is_cancelled = false;
};

} // namespace DM
} // namespace DB
