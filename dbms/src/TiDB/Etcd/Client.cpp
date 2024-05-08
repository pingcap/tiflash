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
#include <Poco/URI.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/Etcd/EtcdConnClient.h>
#include <common/logger_useful.h>
#include <etcd/rpc.grpc.pb.h>
#include <etcd/rpc.pb.h>
#include <etcd/v3election.grpc.pb.h>
#include <etcd/v3election.pb.h>
#include <fmt/chrono.h>

#include <magic_enum.hpp>

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
#include <random>

namespace DB::Etcd
{
ClientPtr Client::create(
    const pingcap::pd::ClientPtr & pd_client,
    const pingcap::ClusterConfig & config,
    Int64 timeout_s)
{
    auto etcd_client = std::make_shared<Client>();
    etcd_client->pd_client = pd_client;
    etcd_client->timeout = std::chrono::seconds(timeout_s);
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

// Get the RangeRequest.range_end for "--prefix"
// etcdserverpb/rpc.proto
// range_end is the upper bound on the requested range [key, range_end).
// If range_end is '\0', the range is all keys >= key.
// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
// then the range request gets all keys prefixed with key.
// If both key and range_end are '\0', then the range request returns all keys.
String getPrefix(String key)
{
    assert(!key.empty());
    std::string_view end(key);
    for (Int64 i = end.size() - 1; i >= 0; i--)
    {
        auto ch = static_cast<UInt8>(end[i]);
        if (ch == 0xff)
        {
            continue;
        }
        // plus one to the first ch != 0xff
        key[i] = ch + 1;
        end = end.substr(0, i + 1);
        return String(end);
    }
    // next prefix does not exist (e.g., 0xffff)
    // default to the end
    return String("\x00", 1);
}

std::tuple<String, grpc::Status> Client::getFirstCreateKey(const String & prefix)
{
    etcdserverpb::RangeRequest req;
    // get the key with the oldest creation revision in the request range
    req.set_key(prefix);
    req.set_range_end(getPrefix(prefix));
    req.set_limit(1);
    req.set_sort_target(etcdserverpb::RangeRequest_SortTarget_CREATE);
    req.set_sort_order(etcdserverpb::RangeRequest_SortOrder_ASCEND);

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
        LOG_ERROR(
            log,
            "etcd lease grant failed, code={} msg={}",
            magic_enum::enum_name(status.error_code()),
            status.error_message());
        return {};
    }

    auto writer = leaderClient()->lease_stub->LeaseKeepAlive(grpc_context);
    auto session = std::shared_ptr<Session>(new Session(lease_id, first_deadline, std::move(writer)));
    if (session->keepAliveOne())
    {
        return session;
    }
    return {};
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

std::tuple<v3electionpb::LeaderKey, grpc::Status> Client::campaign(
    grpc::ClientContext * grpc_context,
    const String & name,
    const String & value,
    LeaseID lease_id)
{
    v3electionpb::CampaignRequest req;
    req.set_name(name);
    req.set_value(value);
    req.set_lease(lease_id);

    // usually use `campaign` blocks until become leader or error happens,
    // don't set timeout.

    v3electionpb::CampaignResponse resp;
    auto status = leaderClient()->election_stub->Campaign(grpc_context, req, &resp);
    return {resp.leader(), status};
}

std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> Client::watch(
    grpc::ClientContext * grpc_context)
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

// Using ectd Txn to check if /tidb/server_id/server_id exists.
// If doesn't exists, put it to etcd and return. Otherwise retry(max retry 3 times).
UInt64 Client::acquireServerIDFromGAC()
{
    const Int32 retry_times = 3;
    UInt64 random_server_id = 0;
    bool acquire_succ = false;

    std::random_device dev;
    std::mt19937_64 gen(dev());
    std::uniform_int_distribution dist;

    for (Int32 i = 0; i < retry_times; ++i)
    {
        auto exists_server_ids = getExistsServerID();

        const Int32 max_count = std::numeric_limits<Int32>::max();
        bool gen_random_server_id_succ = false;
        for (Int32 count = 0; count < max_count; ++count)
        {
            random_server_id = dist(gen);
            if (exists_server_ids.find(random_server_id) == exists_server_ids.end())
            {
                gen_random_server_id_succ = true;
                break;
            }
        }

        if (!gen_random_server_id_succ)
            continue;

        etcdserverpb::TxnRequest txn_req;
        etcdserverpb::Compare * cmp = txn_req.add_compare();

        cmp->set_result(etcdserverpb::Compare_CompareResult::Compare_CompareResult_EQUAL);
        cmp->set_target(etcdserverpb::Compare_CompareTarget::Compare_CompareTarget_CREATE);
        String key = fmt::format("{}/{}", TIDB_SERVER_ID_ETCD_PATH, random_server_id);
        cmp->set_key(key);
        cmp->set_create_revision(0);

        etcdserverpb::RequestOp * succ_req_op = txn_req.add_success();
        etcdserverpb::PutRequest * put_req = succ_req_op->mutable_request_put();
        put_req->set_key(key);
        put_req->set_value("0");

        etcdserverpb::TxnResponse txn_resp;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + timeout);
        auto status = leaderClient()->kv_stub->Txn(&context, txn_req, &txn_resp);
        if (!status.ok())
            throw Exception("acquireServerIDFromPD failed, grpc error: {}", status.error_message());

        if (txn_resp.succeeded())
        {
            acquire_succ = true;
            break;
        }
    }

    if (!acquire_succ)
        throw Exception("too many times({}) retry when acquireServerIDFromPD", retry_times);

    return random_server_id;
}

std::unordered_set<UInt64> Client::getExistsServerID()
{
    etcdserverpb::RangeRequest range_req;
    range_req.set_key(TIDB_SERVER_ID_ETCD_PATH);
    char next_ch = '/' + 1;
    range_req.set_range_end(TIDB_SERVER_ID_ETCD_PATH + std::string{next_ch});
    range_req.set_keys_only(true);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);
    etcdserverpb::RangeResponse range_resp;
    auto status = leaderClient()->kv_stub->Range(&context, range_req, &range_resp);
    if (!status.ok())
        throw Exception("getExistsServerID failed, grpc error: {}", status.error_message());

    std::unordered_set<UInt64> exists_server_ids;
    for (const auto & kv : range_resp.kvs())
    {
        String key = kv.key();
        String prefix(key.begin(), key.begin() + TIDB_SERVER_ID_ETCD_PATH.size());
        RUNTIME_CHECK(prefix == TIDB_SERVER_ID_ETCD_PATH);
        String server_id_str(key.begin() + TIDB_SERVER_ID_ETCD_PATH.size() + 1, key.end());
        exists_server_ids.insert(std::stoi(server_id_str));
    }
    LOG_INFO(log, "existing server ids: {}", exists_server_ids.size());
    return exists_server_ids;
}

void Client::deleteServerIDFromGAC(UInt64 serverID)
{
    etcdserverpb::DeleteRangeRequest del_range_req;
    const String key = fmt::format("{}/{}", TIDB_SERVER_ID_ETCD_PATH, serverID);
    del_range_req.set_key(key);

    etcdserverpb::DeleteRangeResponse del_range_resp;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    auto status = leaderClient()->kv_stub->DeleteRange(&context, del_range_req, &del_range_resp);
    if (!status.ok())
        throw Exception("deleteServerIDFromGAC failed, grpc error: {}", status.error_message());

    if (del_range_resp.deleted() != 1)
        throw Exception(
            "deleteServerIDFromGAC failed, unexpected deleted num, expect 1, got {}",
            del_range_resp.deleted());
}

bool Session::isValid() const
{
    TimePoint now = std::chrono::system_clock::now();
    return now < lease_deadline;
}

bool Session::keepAliveOne()
{
    if (finished)
    {
        // The `writer` has been finished, can not be called again
        return false;
    }

    etcdserverpb::LeaseKeepAliveRequest req;
    req.set_id(lease_id);
    bool ok = writer->Write(req);
    if (!ok)
    {
        auto status = writer->Finish();
        LOG_INFO(
            log,
            "keep alive write fail, code={} msg={}",
            magic_enum::enum_name(status.error_code()),
            status.error_message());
        finished = true;
        return false;
    }
    etcdserverpb::LeaseKeepAliveResponse resp;
    TimePoint next_timepoint = std::chrono::system_clock::now();
    ok = writer->Read(&resp);
    if (!ok)
    {
        auto status = writer->Finish();
        LOG_INFO(
            log,
            "keep alive read fail, code={} msg={}",
            magic_enum::enum_name(status.error_code()),
            status.error_message());
        finished = true;
        return false;
    }

    // the lease is not valid anymore
    if (resp.ttl() <= 0)
    {
        auto status = writer->Finish();
        LOG_INFO(
            log,
            "keep alive fail, ttl={}, code={} msg={}",
            resp.ttl(),
            magic_enum::enum_name(status.error_code()),
            status.error_message());
        finished = true;
        return false;
    }
    lease_deadline = next_timepoint + std::chrono::seconds(resp.ttl());
    LOG_DEBUG(
        log,
        "keep alive update deadline, ttl={} lease_deadline={:%Y-%m-%d %H:%M:%S}",
        resp.ttl(),
        lease_deadline);
    return true;
}

const String Client::TIDB_SERVER_ID_ETCD_PATH = "/tidb/server_id";
} // namespace DB::Etcd
