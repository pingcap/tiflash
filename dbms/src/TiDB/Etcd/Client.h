#pragma once

#include <Common/Logger.h>
#include <Storages/BackgroundProcessingPool.h>
#include <common/types.h>
#include <etcd/kv.pb.h>
#include <etcd/v3election.pb.h>

#include <chrono>
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

namespace DB
{
class Context;
}

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

    std::tuple<String, grpc::Status> getFirstKey(const String & prefix);

    std::tuple<LeaseID, grpc::Status> leaseGrant(Int64 ttl);

    SessionPtr createSession(Context & context, Int64 ttl);

    grpc::Status leaseRevoke(LeaseID lease_id);

    std::tuple<v3electionpb::LeaderKey, grpc::Status>
    campaign(const String & name, const String & value, LeaseID lease_id);

    void proclaim(const String & value, const v3electionpb::LeaderKey & leader_key);

    std::unique_ptr<grpc::ClientReader<v3electionpb::LeaderResponse>>
    observe(grpc::ClientContext * grpc_context, const String & name);

    grpc::Status waitsUntilDeleted(grpc::ClientContext * grpc_context, const String & key);

    std::tuple<mvccpb::KeyValue, grpc::Status> leader(const String & name);

    void resign(const v3electionpb::LeaderKey & leader_key);

private:
    EtcdConnClientPtr getOrCreateGRPCConn(const String & addr);

    EtcdConnClientPtr leaderClient();

    void updateLeader();

private:
    pingcap::pd::ClientPtr pd_client;

    std::chrono::seconds timeout;

    Strings urls;

    pingcap::ClusterConfig config;

    std::mutex mtx_channel_map;
    std::unordered_map<String, EtcdConnClientPtr> channel_map;

    LoggerPtr log;
};

class Session
{
public:
    ~Session();

    LeaseID leaseID() const
    {
        return lease_id;
    }

    void setCanceled();
    bool isCanceled() const;

    void cancel();

private:
    explicit Session(Context & context, LeaseID l);

    friend class Client;

private:
    std::mutex mtx;
    std::atomic<LeaseID> lease_id{InvalidLeaseID};

    grpc::ClientContext grpc_context;
    using KeepAliveWriter = std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>>;
    KeepAliveWriter writer;

    Context & global_ctx;

    BackgroundProcessingPool::TaskHandle keep_alive_handle;
};


} // namespace DB::Etcd
