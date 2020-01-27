#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

#include <iostream>


namespace DB
{

void GcBuff(BaseBuff * buff)
{
    delete buff->inner;
    buff->inner = nullptr;
}

static_assert(sizeof(TiFlashServerHelper) == 9 * sizeof(void *));
static_assert(alignof(TiFlashServerHelper) == alignof(void *));

TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, RaftCmdHeader header)
{
    try
    {
        raft_cmdpb::RaftCmdRequest request;
        request.ParseFromArray(req_buff.data, (int)req_buff.len);
        return server->tmt.getKVStore()->handleWriteRaftCmd(std::move(request), header.region_id, header.index, header.term, server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

BaseBuff HelloWorld()
{
    auto s = new std::string("Hello World");
    return BaseBuff{s, s->data(), s->size()};
}

TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header)
{
    try
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.ParseFromArray(req_buff.data, (int)req_buff.len);
        response.ParseFromArray(resp_buff.data, (int)resp_buff.len);

        auto & kvstore = server->tmt.getKVStore();
        return kvstore->handleAdminRaftCmd(
            std::move(request), std::move(response), header.region_id, header.index, header.term, server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleApplySnapshot(const TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotDataView lock_cf_view,
    SnapshotDataView write_cf_view, SnapshotDataView default_cf_view, uint64_t index, uint64_t term)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & kvstore = server->tmt.getKVStore();
        kvstore->handleApplySnapshot(std::move(region), peer_id, lock_cf_view, write_cf_view, default_cf_view, index, term, server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void AtomicUpdateProxy(DB::TiFlashServer * server, DB::TiFlashRaftProxy * proxy)
{
    try
    {
        server->proxy = proxy;
        if (server->proxy.load()->check_sum != 666)
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": check_sum of proxy is wrong");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleDestroy(TiFlashServer * server, RegionId region_id)
{
    try
    {
        auto & kvstore = server->tmt.getKVStore();
        kvstore->handleDestroy(region_id, server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

} // namespace DB
