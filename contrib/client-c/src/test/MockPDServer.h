#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include <kvproto/pdpb.grpc.pb.h>

#include "MockTiKV.h"

namespace pingcap{
namespace test {

class PDService final : public ::pdpb::PD::Service {
public:
    PDService(std::vector<std::string> addrs_){
        addrs = addrsToUrls(addrs_);
        leader = addrs[0];
    }

    ::grpc::Status GetMembers(::grpc::ServerContext* context, const ::pdpb::GetMembersRequest* request, ::pdpb::GetMembersResponse* response) override 
    {
        pdpb::Member * leader_pb = new pdpb::Member();
        setMember(leader, leader_pb);
        response->set_allocated_leader(leader_pb);
        pdpb::Member * etcd_leader_pb = new pdpb::Member();
        setMember(leader, etcd_leader_pb);
        response->set_allocated_etcd_leader(etcd_leader_pb);
        for (size_t i = 0; i < addrs.size(); i++) {
            pdpb::Member * member = response -> add_members();
            setMember(addrs[i], member);
        }
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        response -> set_allocated_header(header);
        return ::grpc::Status::OK;
    }

    ::grpc::Status GetGCSafePoint(::grpc::ServerContext* context, const ::pdpb::GetGCSafePointRequest* request, ::pdpb::GetGCSafePointResponse* response) override
    {
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        response -> set_allocated_header(header);
        response -> set_safe_point(gc_point);
        return ::grpc::Status::OK;
    }

    ::grpc::Status GetStore(::grpc::ServerContext* context, const ::pdpb::GetStoreRequest* request, ::pdpb::GetStoreResponse* response) override
    {
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        auto it = stores.find(request->store_id());
        response -> set_allocated_header(header);
        if (it != stores.end()) {
            auto store = new ::metapb::Store();
            store -> set_address(it -> second -> getStoreUrl());
            store -> set_id(it -> second -> store_id);
            response -> set_allocated_store(store);
        }
        if (statuses.empty())
            return ::grpc::Status::OK;
        auto ret = statuses.front();
        statuses.pop();
        return ret;
    }

    void registerStoreAddr(uint64_t store_id, std::string addr)
    {
        stores[store_id] -> registerAddr(addr);
    }

    ::grpc::Status GetRegionByID(::grpc::ServerContext* context, const ::pdpb::GetRegionByIDRequest* request, ::pdpb::GetRegionResponse* response) override
    {
        pdpb::ResponseHeader * header = new pdpb::ResponseHeader();
        setHeader(header);
        auto it = regions.find(request->region_id());
        response -> set_allocated_header(header);
        if (it != regions.end()) {
            auto region = new ::metapb::Region (it -> second->meta);
            response -> set_allocated_region(region);
            auto leader = new ::metapb::Peer (it -> second->peer);
            response -> set_allocated_leader(leader);
            auto slave = response -> add_slaves();
            *slave = it->second->learner;
        }
        if (statuses.empty())
            return ::grpc::Status::OK;
        auto ret = statuses.front();
        statuses.pop();
        return ret;
    }

    void setGCPoint(uint64_t gc_point_) {
        gc_point = gc_point_;
    }

    void addStore() {
        std::string addr = ("127.0.0.1:" + std::to_string(6000+cur_store_id));
        Store* store = new Store(addr, cur_store_id);
        stores[cur_store_id] = store;
        store -> aynsc_run();
        cur_store_id ++;
    }

    void addRegion(::metapb::Region region, uint64_t leader_id, uint64_t learner_id) {
        ::metapb::Peer *learner, * leader;
        int i = 0;
        for (auto it = stores.begin(); it != stores.end(); it++, i ++) {
            Store * store = it -> second;
            ::metapb::Peer* peer = region.add_peers();
            peer -> set_id(i);
            peer -> set_store_id(store->store_id);
            if (store -> store_id == learner_id) {
                learner = peer;
            }
            if (store -> store_id == leader_id) {
                leader = peer;
            }
        }
        kv::RegionPtr region_ptr = std::make_shared<kv::Region>(region, *leader, *learner);
        for (auto it: stores) {
            it.second -> addRegion(region_ptr);
        }
        regions[region_ptr->meta.id()] = (region_ptr);
    }
    std::map<uint64_t, Store*> stores;

    void registerGRPCStatus(::grpc::Status status_) {
        statuses.push(status_);
    }

private:
    std::vector<std::string> addrsToUrls(std::vector<std::string> addrs) {
        std::vector<std::string> urls;
        for (const std::string & addr: addrs) {
            if (addr.find("://") == std::string::npos) {
                urls.push_back("http://" + addr);
            } else {
                urls.push_back(addr);
            }
        }
        return urls;
    }

    std::queue<::grpc::Status> statuses;

    std::string leader;
    std::vector<std::string> addrs;
    std::map<uint64_t, kv::RegionPtr> regions;
    uint64_t gc_point;

    void setMember(const std::string & addr, pdpb::Member* member) {
        member->set_name(addr);
        member->add_peer_urls(addr);
        member->add_client_urls(addr);
        member->set_leader_priority(1);
    }

    void setHeader(pdpb::ResponseHeader * header) {
        header->set_cluster_id(0);
    }

    int cur_store_id = 0;
};

struct PDServerHandler {

    std::vector<std::string> addrs;
    PDService * service;

    PDServerHandler(std::vector<std::string> addrs_) : addrs(addrs_) {}

    void startServer() {
        grpc::ServerBuilder builder;
        for (auto addr : addrs) {
            builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
        }
        builder.RegisterService(service);
        auto server = builder.BuildAndStart();
        server->Wait();
    }

    PDService * RunPDServer()
    {
        service = new PDService(addrs);
        std::thread pd_server_thread(&PDServerHandler::startServer, this);
        pd_server_thread.detach();
        return service;
    }

};

}
}
