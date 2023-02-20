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

#include <Common/Logger.h>
#include <common/types.h>
#include <etcd/kv.pb.h>
#include <etcd/v3election.pb.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/client_context.h>
#include <pingcap/Config.h>
#include <pingcap/pd/Client.h>
#include <pingcap/pd/IClient.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB::Etcd
{

struct EtcdConnClient;
using EtcdConnClientPtr = std::shared_ptr<EtcdConnClient>;

using LeaseID = Int64;
static constexpr LeaseID InvalidLeaseID = 0;

class Client;
using ClientPtr = std::shared_ptr<Client>;
class Session;
using SessionPtr = std::shared_ptr<Session>;


class Client
{
public:
    static ClientPtr create(const pingcap::pd::ClientPtr & pd_client, const pingcap::ClusterConfig & config);

    void update(const pingcap::ClusterConfig & new_config);

    std::tuple<String, grpc::Status> getFirstKey(const String & prefix);

    std::tuple<LeaseID, grpc::Status> leaseGrant(Int64 ttl);

    SessionPtr createSession(grpc::ClientContext * grpc_context, Int64 ttl);

    grpc::Status leaseRevoke(LeaseID lease_id);

    std::tuple<v3electionpb::LeaderKey, grpc::Status>
    campaign(const String & name, const String & value, LeaseID lease_id);

    std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>>
    watch(grpc::ClientContext * grpc_context);

    std::tuple<mvccpb::KeyValue, grpc::Status> leader(const String & name);

    grpc::Status resign(const v3electionpb::LeaderKey & leader_key);

private:
    EtcdConnClientPtr getOrCreateGRPCConn(const String & addr);

    EtcdConnClientPtr leaderClient();

    void updateLeader();

private:
    pingcap::pd::ClientPtr pd_client;

    std::chrono::seconds timeout;

    pingcap::ClusterConfig config;

    std::mutex mtx_channel_map;
    std::unordered_map<String, EtcdConnClientPtr> channel_map;

    LoggerPtr log;
};

class Session
{
public:
    LeaseID leaseID() const
    {
        return lease_id;
    }

    bool keepAliveOne();

private:
    using KeepAliveWriter = std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>>;

    Session(LeaseID l, KeepAliveWriter && w)
        : lease_id(l)
        , writer(std::move(w))
    {
    }

    friend class Client;

private:
    LeaseID lease_id{InvalidLeaseID};

    KeepAliveWriter writer;
};


} // namespace DB::Etcd
