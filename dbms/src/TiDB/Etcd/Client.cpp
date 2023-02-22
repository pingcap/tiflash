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

#include <Poco/URI.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/Etcd/EtcdConnClient.h>
#include <common/logger_useful.h>
#include <etcd/rpc.grpc.pb.h>
#include <etcd/rpc.pb.h>
#include <etcd/v3election.grpc.pb.h>
#include <etcd/v3election.pb.h>
#include <fmt/chrono.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/client_context.h>
#include <grpcpp/impl/codegen/call_op_set.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/status_code_enum.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <chrono>
#include <mutex>
#include <shared_mutex>

namespace DB::Etcd
{
ClientPtr Client::create(const pingcap::pd::ClientPtr & pd_client, const pingcap::ClusterConfig & config)
{
    auto etcd_client = std::make_shared<Client>();
    etcd_client->pd_client = pd_client;
    etcd_client->timeout = std::chrono::seconds(2);
    etcd_client->config = config;
    etcd_client->log = Logger::get();

    return etcd_client;
}

void Client::update(const pingcap::ClusterConfig & new_config)
{
    // Note that config of pd_client is updated outside of this function
    // before the etcd client config update.

    {
        std::lock_guard lk(mtx_channel_map);
        config = new_config;
        channel_map.clear();
    }
}

EtcdConnClientPtr Client::getOrCreateGRPCConn(const String & addr)
{
    std::lock_guard lk(mtx_channel_map);
    auto it = channel_map.find(addr);
    if (it != channel_map.end())
        return it->second;

    Poco::URI uri(addr);
    auto client = std::make_shared<EtcdConnClient>(uri.getAuthority(), config);
    channel_map[addr] = client;
    return client;
}

EtcdConnClientPtr Client::leaderClient()
{
    auto leader_url = pd_client->getLeaderUrl();
    return getOrCreateGRPCConn(leader_url);
}

String getPrefix(const String & key)
{
    String end_key(key);
    std::string_view end(end_key);
    for (size_t i = end.size() - 1; i >= 0; i--)
    {
        auto ch = static_cast<UInt8>(end[i]);
        if (ch < 0xff)
        {
            end_key[i] = ch + 1;
            end = end.substr(0, i + 1);
            return String(end);
        }
    }
    // next prefix does not exist (e.g., 0xffff)
    // default to the end
    return String("\x00", 1);
}

std::tuple<String, grpc::Status> Client::getFirstKey(const String & prefix)
{
    etcdserverpb::RangeRequest req;
    req.set_key(prefix);
    // get the key with the oldest creation revision in the request range
    req.set_sort_target(etcdserverpb::RangeRequest_SortTarget_CREATE);
    req.set_sort_order(etcdserverpb::RangeRequest_SortOrder_ASCEND);
    req.set_limit(1);
    req.set_range_end(getPrefix(prefix));

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    etcdserverpb::RangeResponse resp;

    auto status = leaderClient()->kv_stub->Range(&context, req, &resp);
    if (!status.ok())
        return {"", status};

    if (resp.kvs_size() == 0)
        return {"", grpc::Status::OK};
    return {resp.kvs(0).value(), grpc::Status::OK};
}

std::tuple<LeaseID, grpc::Status> Client::leaseGrant(Int64 ttl)
{
    etcdserverpb::LeaseGrantRequest req;
    req.set_ttl(ttl);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    etcdserverpb::LeaseGrantResponse resp;
    auto status = leaderClient()->lease_stub->LeaseGrant(&context, req, &resp);
    if (!status.ok())
    {
        return {InvalidLeaseID, status};
    }
    return {resp.id(), status};
}

SessionPtr Client::createSession(grpc::ClientContext * grpc_context, Int64 ttl)
{
    auto first_deadline = std::chrono::system_clock::now() + std::chrono::seconds(ttl);
    const auto & [lease_id, status] = leaseGrant(ttl);
    if (!status.ok())
    {
        LOG_ERROR(log, "etcd lease grant failed, code={} msg={}", status.error_code(), status.error_message());
        return {};
    }

    // the timeout for first keep alive
    grpc_context->set_deadline(std::chrono::system_clock::now() + timeout);
    auto writer = leaderClient()->lease_stub->LeaseKeepAlive(grpc_context);
    auto session = std::shared_ptr<Session>(new Session(lease_id, first_deadline, std::move(writer)));
    if (session->keepAliveOne())
    {
        return session;
    }
    return nullptr;
}

grpc::Status Client::leaseRevoke(LeaseID lease_id)
{
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    etcdserverpb::LeaseRevokeRequest req;
    req.set_id(lease_id);

    etcdserverpb::LeaseRevokeResponse resp;
    return leaderClient()->lease_stub->LeaseRevoke(&context, req, &resp);
}

std::tuple<v3electionpb::LeaderKey, grpc::Status>
Client::campaign(const String & name, const String & value, LeaseID lease_id)
{
    v3electionpb::CampaignRequest req;
    req.set_name(name);
    req.set_value(value);
    req.set_lease(lease_id);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    v3electionpb::CampaignResponse resp;
    auto status = leaderClient()->election_stub->Campaign(&context, req, &resp);
    return {resp.leader(), status};
}

std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>>
Client::watch(grpc::ClientContext * grpc_context)
{
    return leaderClient()->watch_stub->Watch(grpc_context);
}

std::tuple<mvccpb::KeyValue, grpc::Status> Client::leader(const String & name)
{
    v3electionpb::LeaderRequest req;
    req.set_name(name);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    v3electionpb::LeaderResponse resp;
    auto status = leaderClient()->election_stub->Leader(&context, req, &resp);

    return {resp.kv(), status};
}

grpc::Status Client::resign(const v3electionpb::LeaderKey & leader_key)
{
    v3electionpb::ResignRequest req;
    req.mutable_leader()->CopyFrom(leader_key);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    v3electionpb::ResignResponse resp;
    auto status = leaderClient()->election_stub->Resign(&context, req, &resp);
    return status;
}

bool Session::isValid() const
{
    TimePoint now = std::chrono::system_clock::now();
    return now < lease_deadline;
}

bool Session::keepAliveOne()
{
    etcdserverpb::LeaseKeepAliveRequest req;
    req.set_id(lease_id);
    bool ok = writer->Write(req);
    if (!ok)
    {
        auto status = writer->Finish();
        LOG_INFO(log, "keep alive write fail, code={} msg={}", status.error_code(), status.error_message());
        return false;
    }
    etcdserverpb::LeaseKeepAliveResponse resp;
    TimePoint next_timepoint = std::chrono::system_clock::now();
    ok = writer->Read(&resp);
    if (!ok)
    {
        auto status = writer->Finish();
        LOG_INFO(log, "keep alive read fail, code={} msg={}", status.error_code(), status.error_message());
        return false;
    }

    // the lease is not valid anymore
    if (resp.ttl() <= 0)
    {
        LOG_DEBUG(log, "keep alive fail, ttl={}", resp.ttl());
        return false;
    }
    lease_deadline = next_timepoint + std::chrono::seconds(resp.ttl());
    LOG_DEBUG(log, "keep alive update deadline, ttl={} lease_deadline={:%Y-%m-%d %H:%M:%S}", resp.ttl(), lease_deadline);
    return true;
}

} // namespace DB::Etcd
