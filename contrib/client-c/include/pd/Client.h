#pragma once

#include <common/Log.h>
#include <kvproto/pdpb.grpc.pb.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include "IClient.h"

namespace pingcap
{
namespace pd
{

struct SecurityOption
{
    std::string CAPath;
    std::string CertPath;
    std::string KeyPath;
};

class Client : public IClient
{
    const int max_init_cluster_retries;

    const std::chrono::seconds pd_timeout;

    const std::chrono::microseconds loop_interval;

    const std::chrono::seconds update_leader_interval;

public:
    Client(const std::vector<std::string> & addrs);

    ~Client() override;

    //uint64_t getClusterID() override;

    // only implement a weak get ts.
    uint64_t getTS() override;

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegion(std::string key) override;

    //std::pair<metapb::Region, metapb::Peer> getPrevRegion(std::string key) override;

    std::tuple<metapb::Region, metapb::Peer, std::vector<metapb::Peer>> getRegionByID(uint64_t region_id) override;

    metapb::Store getStore(uint64_t store_id) override;

    //std::vector<metapb::Store> getAllStores() override;

    uint64_t getGCSafePoint() override;

    bool isMock() override;

private:
    void initClusterID();

    void updateLeader();

    void updateURLs(const ::google::protobuf::RepeatedPtrField<::pdpb::Member> & members);

    void leaderLoop();

    void switchLeader(const ::google::protobuf::RepeatedPtrField<std::string> &);

    std::unique_ptr<pdpb::PD::Stub> leaderStub();

    pdpb::GetMembersResponse getMembers(std::string);

    pdpb::RequestHeader * requestHeader();

    std::shared_ptr<grpc::Channel> getOrCreateGRPCConn(const std::string &);

    std::unique_ptr<pdpb::PD::Stub> stub_ptr;

    std::shared_mutex leader_mutex;

    std::mutex channel_map_mutex;

    std::mutex update_leader_mutex;

    std::mutex gc_safepoint_mutex;

    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_map;

    std::vector<std::string> urls;

    uint64_t cluster_id;

    std::string leader;

    std::atomic<bool> work_threads_stop;

    std::thread work_thread;

    std::condition_variable update_leader_cv;

    std::atomic<bool> check_leader;

    Logger * log;
};


} // namespace pd
} // namespace pingcap
