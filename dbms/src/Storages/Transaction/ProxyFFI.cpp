#include <Interpreters/Context.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <sys/statvfs.h>

namespace DB
{

const std::string ColumnFamilyName::Lock = "lock";
const std::string ColumnFamilyName::Default = "default";
const std::string ColumnFamilyName::Write = "write";

ColumnFamilyType NameToCF(const std::string & cf)
{
    if (cf.empty() || cf == ColumnFamilyName::Default)
        return ColumnFamilyType::Default;
    if (cf == ColumnFamilyName::Lock)
        return ColumnFamilyType::Lock;
    if (cf == ColumnFamilyName::Write)
        return ColumnFamilyType::Write;
    throw Exception("Unsupported cf name " + cf, ErrorCodes::LOGICAL_ERROR);
}

const std::string & CFToName(const ColumnFamilyType type)
{
    switch (type)
    {
        case ColumnFamilyType::Default:
            return ColumnFamilyName::Default;
        case ColumnFamilyType::Write:
            return ColumnFamilyName::Write;
        case ColumnFamilyType::Lock:
            return ColumnFamilyName::Lock;
        default:
            throw Exception("Can not tell cf type " + std::to_string(static_cast<uint8_t>(type)), ErrorCodes::LOGICAL_ERROR);
    }
}

TiFlashRawString GenCppRawString(BaseBuffView view) { return view.len ? new std::string(view.data, view.len) : nullptr; }

static_assert(alignof(TiFlashServerHelper) == alignof(void *));

TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView cmds, RaftCmdHeader header)
{
    try
    {
        return server->tmt->getKVStore()->handleWriteRaftCmd(cmds, header.region_id, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header)
{
    try
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.ParseFromArray(req_buff.data, (int)req_buff.len);
        response.ParseFromArray(resp_buff.data, (int)resp_buff.len);

        auto & kvstore = server->tmt->getKVStore();
        return kvstore->handleAdminRaftCmd(
            std::move(request), std::move(response), header.region_id, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleApplySnapshot(
    const TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleApplySnapshot(std::move(region), peer_id, snaps, index, term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void AtomicUpdateProxy(DB::TiFlashServer * server, DB::TiFlashRaftProxyHelper * proxy) { server->proxy_helper = proxy; }

void HandleDestroy(TiFlashServer * server, RegionId region_id)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleDestroy(region_id, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

TiFlashApplyRes HandleIngestSST(TiFlashServer * server, SnapshotViewArray snaps, RaftCmdHeader header)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->handleIngestSST(header.region_id, snaps, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t HandleCheckTerminated(TiFlashServer * server) { return server->tmt->getTerminated().load(std::memory_order_relaxed) ? 1 : 0; }

FsStats HandleComputeFsStats(TiFlashServer * server)
{
    FsStats res; // res.ok = false by default
    try
    {
        auto global_capacity = server->tmt->getContext().getPathCapacity();
        res = global_capacity->getFsStats();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return res;
}

TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server) { return server->status.load(); }

bool TiFlashRaftProxyHelper::checkServiceStopped() const { return fn_handle_check_service_stopped(proxy_ptr); }
bool TiFlashRaftProxyHelper::checkEncryptionEnabled() const { return fn_is_encryption_enabled(proxy_ptr); }
EncryptionMethod TiFlashRaftProxyHelper::getEncryptionMethod() const { return fn_encryption_method(proxy_ptr); }
FileEncryptionInfo TiFlashRaftProxyHelper::getFile(std::string_view view) const { return fn_handle_get_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::newFile(std::string_view view) const { return fn_handle_new_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::deleteFile(std::string_view view) const { return fn_handle_delete_file(proxy_ptr, view); }
FileEncryptionInfo TiFlashRaftProxyHelper::linkFile(std::string_view src, std::string_view dst) const
{
    return fn_handle_link_file(proxy_ptr, src, dst);
}
FileEncryptionInfo TiFlashRaftProxyHelper::renameFile(std::string_view src, std::string_view dst) const
{
    return fn_handle_rename_file(proxy_ptr, src, dst);
}

struct PreHandleSnapshotRes
{
    RegionPtr region;
};

void * PreHandleSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & kvstore = server->tmt->getKVStore();
        auto new_region = kvstore->preHandleSnapshot(std::move(region), peer_id, snaps, index, term, *server->tmt);
        auto res = new PreHandleSnapshotRes{new_region};
        return res;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledSnapshot(TiFlashServer * server, void * res)
{
    PreHandleSnapshotRes * snap = reinterpret_cast<PreHandleSnapshotRes *>(res);
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleApplySnapshot(snap->region, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void GcPreHandledSnapshot(TiFlashServer *, void * res)
{
    PreHandleSnapshotRes * snap = reinterpret_cast<PreHandleSnapshotRes *>(res);
    delete snap;
}

void GcCppString(TiFlashServer *, TiFlashRawString s) { delete s; }

const char * IntoEncryptionMethodName(EncryptionMethod method)
{
    static const char * EncryptionMethodName[] = {
        "Unknown",
        "Plaintext",
        "Aes128Ctr",
        "Aes192Ctr",
        "Aes256Ctr",
    };
    return EncryptionMethodName[static_cast<uint8_t>(method)];
}

} // namespace DB
