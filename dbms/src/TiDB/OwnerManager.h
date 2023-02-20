#pragma once

#include <TiDB/OwnerInfo.h>
#include <common/types.h>
#include <etcd/v3election.pb.h>

#include <condition_variable>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
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
using OwnerManagerPtr = std::unique_ptr<OwnerManager>;


class OwnerManager
{
public:
    static OwnerManagerPtr
    createS3GCOwner(
        Context & context,
        std::string_view id,
        const Etcd::ClientPtr & client,
        Int64 owner_ttl = 60);

    OwnerManager(
        Context & context,
        std::string_view campaign_name_,
        std::string_view id_,
        const Etcd::ClientPtr & client_,
        Int64 owner_ttl = 60);

    ~OwnerManager();

    // start a thread to campaign owner
    void campaignOwner();

    bool isOwner();

    OwnerInfo getOwnerID();

    bool resignOwner();

    void cancel();

    void campaignCancel();

    void setBeOwnerHook(std::function<void()> && hook)
    {
        be_owner = hook;
    }

private:
    void camaignLoop(Etcd::SessionPtr session);

    std::optional<String> getOwnerKey(const String & expect_id);

    void toBeOwner(Etcd::LeaderKey && leader_key);

    void watchOwner(const Etcd::SessionPtr & session, const String & owner_key);
    void retireOwner();

    void revokeEtcdSession(Etcd::LeaseID lease_id);

private:
    String campaign_name;
    String id;
    Etcd::ClientPtr client;
    Int64 leader_ttl;

    std::mutex mtx_camaign;
    std::atomic<bool> enable_camaign{true};
    grpc::ClientContext watch_ctx;

    std::thread th_camaign;

    std::mutex mtx_leader;
    Etcd::LeaderKey leader;

    std::function<void()> be_owner;

    Context & global_ctx;
    LoggerPtr log;
};
} // namespace DB
