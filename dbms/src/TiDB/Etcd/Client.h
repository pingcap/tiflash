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

#include <Common/Logger.h>
#include <common/types.h>
#include <etcd/kv.pb.h>
#include <etcd/v3election.pb.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
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
    // Create an etcd client with pd client
    // default timeout is 2 seconds (the same as pd client)
    static ClientPtr create(
        const pingcap::pd::ClientPtr & pd_client,
        const pingcap::ClusterConfig & config,
        Int64 timeout_s = 2);

    // update TLS related
    void update(const pingcap::ClusterConfig & new_config);

    std::tuple<String, grpc::Status> getFirstCreateKey(const String & prefix);

    std::tuple<LeaseID, grpc::Status> leaseGrant(Int64 ttl);

    SessionPtr createSession(grpc::ClientContext * grpc_context, Int64 ttl);

    grpc::Status leaseRevoke(LeaseID lease_id);

    std::tuple<v3electionpb::LeaderKey, grpc::Status> campaign(
        grpc::ClientContext * grpc_context,
        const String & name,
        const String & value,
        LeaseID lease_id);

    std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> watch(
        grpc::ClientContext * grpc_context);

    std::tuple<mvccpb::KeyValue, grpc::Status> leader(const String & name);

    grpc::Status resign(const v3electionpb::LeaderKey & leader_key);

    // Basically same with tidb's Domain::acquireServerID.
    // Only for tiflash resource control.
    UInt64 acquireServerIDFromGAC();
    void deleteServerIDFromGAC(UInt64 serverID);

private:
    EtcdConnClientPtr getOrCreateGRPCConn(const String & addr);

    EtcdConnClientPtr leaderClient();

    void updateLeader();

    std::unordered_set<UInt64> getExistsServerID();

    static const String TIDB_SERVER_ID_ETCD_PATH;

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
    LeaseID leaseID() const { return lease_id; }

    // Send one rpc LeaseKeepAliveRequest to etcd for
    // keeping the lease valid. Note that it could be blocked
    // cause by network issue.
    // Returning false means the lease is not valid anymore.
    bool keepAliveOne();

    // Check whether the lease exceed its expected deadline.
    // Returing false means the lease is not valid anymore.
    bool isValid() const;

private:
    using KeepAliveWriter = std::unique_ptr<
        grpc::ClientReaderWriter<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>>;
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

    Session(LeaseID l, TimePoint first_deadline, KeepAliveWriter && w)
        : lease_id(l)
        , lease_deadline(first_deadline)
        , writer(std::move(w))
        , finished(false)
        , log(Logger::get(fmt::format("lease={:x}", lease_id)))
    {}

    friend class Client;

private:
    LeaseID lease_id{InvalidLeaseID};
    TimePoint lease_deadline;

    KeepAliveWriter writer;
    bool finished;

    LoggerPtr log;
};


} // namespace DB::Etcd
