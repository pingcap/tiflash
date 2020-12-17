#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <sys/statvfs.h>

namespace CurrentMetrics
{
extern const Metric RaftNumSnapshotsPendingApply;
}

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

RawCppPtr GenCppRawString(BaseBuffView view)
{
    return RawCppPtr(view.len ? new std::string(view.data, view.len) : nullptr, RawCppPtrType::String);
}

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

StoreStats HandleComputeStoreStats(TiFlashServer * server)
{
    StoreStats res; // res.fs_stats.ok = false by default
    try
    {
        auto global_capacity = server->tmt->getContext().getPathCapacity();
        res.fs_stats = global_capacity->getFsStats();
        // TODO: set engine read/write stats
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return res;
}

TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server) { return server->status.load(); }

RaftProxyStatus TiFlashRaftProxyHelper::getProxyStatus() const
{
    return static_cast<RaftProxyStatus>(fn_handle_get_proxy_status(proxy_ptr));
}
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

kvrpcpb::ReadIndexResponse TiFlashRaftProxyHelper::readIndex(const kvrpcpb::ReadIndexRequest & req) const
{
    auto res = batchReadIndex({req});
    return std::move(res->at(0).first);
}

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> & req) const
{
    std::vector<std::string> req_strs;
    req_strs.reserve(req.size());
    for (auto & r : req)
    {
        req_strs.emplace_back(r.SerializeAsString());
    }
    CppStrVec data(std::move(req_strs));
    assert(req_strs.empty());
    auto outer_view = data.intoOuterView();
    BatchReadIndexRes res(fn_handle_batch_read_index(proxy_ptr, outer_view));
    return res;
}

struct PreHandledSnapshot
{
    ~PreHandledSnapshot() { CurrentMetrics::sub(CurrentMetrics::RaftNumSnapshotsPendingApply); }
    PreHandledSnapshot(const RegionPtr & region_, RegionPtrWrap::CachePtr && cache_) : region(region_), cache(std::move(cache_))
    {
        CurrentMetrics::add(CurrentMetrics::RaftNumSnapshotsPendingApply);
    }
    RegionPtr region;
    RegionPtrWrap::CachePtr cache;
};

RawCppPtr PreHandleSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term)
{
    try
    {
        metapb::Region region;
        region.ParseFromArray(region_buff.data, (int)region_buff.len);
        auto & tmt = *server->tmt;
        auto & kvstore = tmt.getKVStore();
        auto new_region = kvstore->genRegionPtr(std::move(region), peer_id, index, term);
        auto new_region_block_cache = kvstore->preHandleSnapshot(new_region, snaps, tmt);
        auto res = new PreHandledSnapshot{new_region, std::move(new_region_block_cache)};
        return RawCppPtr{res, RawCppPtrType::PreHandledSnapshot};
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledSnapshot(TiFlashServer * server, PreHandledSnapshot * snap)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handleApplySnapshot(RegionPtrWrap{snap->region, std::move(snap->cache)}, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledSnapshot(TiFlashServer * server, void * res, RawCppPtrType type)
{
    switch (type)
    {
        case RawCppPtrType::PreHandledSnapshot:
        {
            PreHandledSnapshot * snap = reinterpret_cast<PreHandledSnapshot *>(res);
            ApplyPreHandledSnapshot(server, snap);
            break;
        }
        default:
            LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "unknown type " + std::to_string(uint32_t(type)));
            exit(-1);
    }
}

void GcRawCppPtr(TiFlashServer *, void * ptr, RawCppPtrType type)
{
    if (ptr)
    {
        switch (type)
        {
            case RawCppPtrType::String:
                delete reinterpret_cast<TiFlashRawString>(ptr);
                break;
            case RawCppPtrType::PreHandledSnapshot:
                delete reinterpret_cast<PreHandledSnapshot *>(ptr);
                break;
            default:
                LOG_ERROR(&Logger::get(__PRETTY_FUNCTION__), "unknown type " + std::to_string(uint32_t(type)));
                exit(-1);
        }
    }
}

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

BatchReadIndexRes::pointer GenBatchReadIndexRes(uint64_t cap)
{
    auto res = new BatchReadIndexRes::element_type();
    res->reserve(cap);
    return res;
}

void InsertBatchReadIndexResp(BatchReadIndexRes::pointer resp, BaseBuffView view, uint64_t region_id)
{
    kvrpcpb::ReadIndexResponse res;
    res.ParseFromArray(view.data, view.len);
    resp->emplace_back(std::move(res), region_id);
}
} // namespace DB
