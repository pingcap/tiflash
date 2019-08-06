#include <queue>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <tikv/Region.h>

namespace pingcap
{
namespace test
{

class Store final : public ::tikvpb::Tikv::Service
{
public:
    Store(std::string addr_, int store_id_) : store_addr(addr_), store_id(store_id_) {}

    void addRegion(kv::RegionPtr region) { regions[region->verID().id] = region; }

    ::grpc::Status ReadIndex(
        ::grpc::ServerContext * context, const ::kvrpcpb::ReadIndexRequest * request, ::kvrpcpb::ReadIndexResponse * response) override
    {
        ::errorpb::Error * error_pb = checkContext(request->context());
        if (error_pb != NULL)
        {
            response->set_allocated_region_error(error_pb);
        }
        else
        {
            response->set_read_index(read_idx);
        }
        return ::grpc::Status::OK;
    }

    void setReadIndex(uint64_t idx_) { read_idx = idx_; }

    void aynsc_run()
    {
        std::thread server_thread(&Store::start_server, this);
        server_thread.detach();
    }

    uint64_t store_id;

    uint64_t getStoreID() {}

    std::string getStoreUrl()
    {
        if (addrs.empty())
            return store_addr;
        std::string addr = addrs.front();
        addrs.pop();
        return addr;
    }

    void registerAddr(std::string addr) { addrs.push(addr); }

    void registerStoreId(uint64_t store_id_) { store_ids.push(store_id_); }

    uint64_t getStoreId()
    {
        if (store_ids.empty())
        {
            return store_id;
        }
        uint64_t store_id_ = store_ids.front();
        store_ids.pop();
        return store_id_;
    }

    bool inject_region_not_found;

private:
    std::string store_addr;

    std::queue<std::string> addrs;

    std::queue<uint64_t> store_ids;

    std::string addrToUrl(std::string addr)
    {
        if (addr.find("://") == std::string::npos)
        {
            return ("http://" + addr);
        }
        else
        {
            return addr;
        }
    }

    std::map<uint64_t, kv::RegionPtr> regions;

    int read_idx;

    void start_server()
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(store_addr, grpc::InsecureServerCredentials());
        builder.RegisterService(this);
        auto server = builder.BuildAndStart();
        server->Wait();
    }


    ::errorpb::Error * checkContext(const ::kvrpcpb::Context & ctx)
    {
        uint64_t store_id_ = ctx.peer().id();
        ::errorpb::Error * err = new ::errorpb::Error();
        if (store_id_ != getStoreId())
        {
            ::errorpb::StoreNotMatch * store_not_match = new ::errorpb::StoreNotMatch();
            err->set_allocated_store_not_match(store_not_match);
            return err;
        }

        uint64_t region_id = ctx.region_id();
        auto it = regions.find(region_id);
        if (it == regions.end() || inject_region_not_found)
        {
            inject_region_not_found = false;
            ::errorpb::RegionNotFound * region_not_found = new ::errorpb::RegionNotFound();
            region_not_found->set_region_id(region_id);
            err->set_allocated_region_not_found(region_not_found);
            return err;
        }
        //
        //bool found = false
        //for (auto addr : it->addrs) {
        //    if (addr == store_addr)
        //        found = true;
        //}
        //if (!found) {
        //    RegionNotFound * region_not_found = new RegionNotFound();
        //    region_not_found -> set_region_id(region_id);
        //    err -> set_allocated_region_not_found(store_not_match);
        //    return err;
        //}

        if (it->second->verID().confVer != ctx.region_epoch().conf_ver() || it->second->verID().ver != ctx.region_epoch().version())
        {
            ::errorpb::EpochNotMatch * epoch_not_match = new ::errorpb::EpochNotMatch();
            err->set_allocated_epoch_not_match(epoch_not_match);
            return err;
        }

        return nullptr;
    }
};

inline ::metapb::Region generateRegion(const kv::RegionVerID & ver_id, std::string start, std::string end)
{
    ::metapb::Region region;
    region.set_id(ver_id.id);
    region.set_start_key(start);
    region.set_end_key(end);
    ::metapb::RegionEpoch * region_epoch = new ::metapb::RegionEpoch();
    region_epoch->set_conf_ver(ver_id.confVer);
    region_epoch->set_version(ver_id.ver);
    region.set_allocated_region_epoch(region_epoch);
    return region;
}

} // namespace test
} // namespace pingcap
