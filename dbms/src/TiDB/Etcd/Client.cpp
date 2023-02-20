#include <Common/Exception.h>
#include <Common/ThreadFactory.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/Etcd/EtcdConnClient.h>
#include <common/logger_useful.h>
#include <etcd/rpc.grpc.pb.h>
#include <etcd/rpc.pb.h>
#include <etcd/v3election.grpc.pb.h>
#include <etcd/v3election.pb.h>

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
    auto members = pd_client->getMembers();
    const auto & etcd_leader = members.etcd_leader();
    RUNTIME_CHECK_MSG(etcd_leader.client_urls_size() != 0, "No available etcd endpoints");

    Strings etcd_urls;
    etcd_urls.reserve(etcd_leader.client_urls_size());
    for (const auto & u : etcd_leader.client_urls())
    {
        etcd_urls.emplace_back(u);
    }

    auto etcd_client = std::make_shared<Client>();
    etcd_client->pd_client = pd_client;
    etcd_client->timeout = std::chrono::seconds(2);
    etcd_client->urls.swap(etcd_urls);
    etcd_client->config = config;
    etcd_client->log = Logger::get();

    return etcd_client;
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

SessionPtr Client::createSession(Context & context, Int64 ttl)
{
    const auto & [lease_id, status] = leaseGrant(ttl);
    if (!status.ok())
        return {};

    auto session = std::shared_ptr<Session>(new Session(context, lease_id));
    session->writer = leaderClient()->lease_stub->LeaseKeepAlive(&session->grpc_context);
    auto & bkg_pool = session->global_ctx.getBackgroundPool();
    session->keep_alive_handle = bkg_pool.addTask(
        [s = session, log = log] {
            if (s->isCanceled())
                return false;

            etcdserverpb::LeaseKeepAliveRequest req;
            req.set_id(s->lease_id);
            bool ok = s->writer->Write(req);
            if (!ok)
            {
                auto status = s->writer->Finish();
                LOG_DEBUG(log, "keep alive write faile, finish. lease={:x} code={} msg={}", s->lease_id.load(), status.error_code(), status.error_message());
                s->setCanceled();
                return false;
            }
            etcdserverpb::LeaseKeepAliveResponse resp;
            ok = s->writer->Read(&resp);
            if (!ok)
            {
                auto status = s->writer->Finish();
                LOG_DEBUG(log, "keep alive read faile, finish. lease={:x} code={} msg={}", s->lease_id.load(), status.error_code(), status.error_message());
                s->setCanceled();
                return false;
            }
            return false;
        },
        /*multi*/ false,
        /*interval_ms*/ ttl * 1000 * 2 / 3);
    return session;
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

void Client::proclaim(const String & value, const v3electionpb::LeaderKey & leader_key)
{
    v3electionpb::ProclaimRequest req;
    req.mutable_leader()->CopyFrom(leader_key);
    req.set_value(value);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    v3electionpb::ProclaimResponse resp;
    auto status = leaderClient()->election_stub->Proclaim(&context, req, &resp);
    RUNTIME_CHECK_MSG(status.ok(), "proclaim failed, code={} msg={}", status.error_code(), status.error_message());
}

std::unique_ptr<grpc::ClientReader<v3electionpb::LeaderResponse>>
Client::observe(grpc::ClientContext * grpc_context, const String & name)
{
    v3electionpb::LeaderRequest req;
    req.set_name(name);
    return leaderClient()->election_stub->Observe(grpc_context, req);
}

grpc::Status Client::waitsUntilDeleted(grpc::ClientContext * grpc_context, const String & key)
{
    auto rw = leaderClient()->watch_stub->Watch(grpc_context);

    etcdserverpb::WatchRequest req;
    req.mutable_create_request()->set_key(key);
    bool ok = rw->Write(req);
    if (!ok)
        return rw->Finish();

    bool done = false;
    while (!done)
    {
        etcdserverpb::WatchResponse resp;
        ok = rw->Read(&resp);
        LOG_INFO(Logger::get(), "watch key:{} ok:{} resp: {}", key, ok, resp.ShortDebugString());
        if (!ok)
            break;
        for (const auto & event : resp.events())
        {
            if (event.type() == mvccpb::Event_EventType_DELETE)
            {
                done = true;
                break;
            }
        }
    }
    return rw->Finish();
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

void Client::resign(const v3electionpb::LeaderKey & leader_key)
{
    v3electionpb::ResignRequest req;
    req.mutable_leader()->CopyFrom(leader_key);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + timeout);

    v3electionpb::ResignResponse resp;
    auto status = leaderClient()->election_stub->Resign(&context, req, &resp);
    RUNTIME_CHECK_MSG(status.ok(), "resign failed, code={} msg={}", status.error_code(), status.error_message());
}

Session::Session(Context & context, LeaseID l)
    : lease_id(l)
    , global_ctx(context.getGlobalContext())
{
}

Session::~Session()
{
    cancel();
}

void Session::setCanceled()
{
    lease_id = InvalidLeaseID;
}

bool Session::isCanceled() const
{
    return lease_id == InvalidLeaseID;
}

void Session::cancel()
{
    std::lock_guard lk(mtx);
    if (keep_alive_handle)
    {
        LOG_ERROR(Logger::get(), "canceling {} {}", fmt::ptr(writer), lease_id);
        global_ctx.getBackgroundPool().removeTask(keep_alive_handle);
        keep_alive_handle = nullptr;
        grpc_context.TryCancel();
        writer = nullptr;
        lease_id = InvalidLeaseID;
    }
}

} // namespace DB::Etcd
