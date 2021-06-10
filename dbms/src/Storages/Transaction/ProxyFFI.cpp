#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/FileEncryption.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>

namespace CurrentMetrics
{
extern const Metric RaftNumSnapshotsPendingApply;
}

namespace DB
{

const std::string ColumnFamilyName::Lock = "lock";
const std::string ColumnFamilyName::Default = "default";
const std::string ColumnFamilyName::Write = "write";

extern const uint64_t DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS;

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
    return GenRawCppPtr(view.len ? RawCppString::New(view.data, view.len) : nullptr, RawCppPtrTypeImpl::String);
}

static_assert(alignof(EngineStoreServerHelper) == alignof(RawVoidPtr));

static_assert(sizeof(RaftStoreProxyPtr) == sizeof(ConstRawVoidPtr));
static_assert(alignof(RaftStoreProxyPtr) == alignof(ConstRawVoidPtr));

EngineStoreApplyRes HandleWriteRaftCmd(const EngineStoreServerWrap * server, WriteCmdsView cmds, RaftCmdHeader header)
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

EngineStoreApplyRes HandleAdminRaftCmd(
    const EngineStoreServerWrap * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header)
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

static_assert(sizeof(RaftStoreProxyFFIHelper) == sizeof(TiFlashRaftProxyHelper));
static_assert(alignof(RaftStoreProxyFFIHelper) == alignof(TiFlashRaftProxyHelper));

void AtomicUpdateProxy(DB::EngineStoreServerWrap * server, RaftStoreProxyFFIHelper * proxy)
{
    server->proxy_helper = static_cast<TiFlashRaftProxyHelper *>(proxy);
}

void HandleDestroy(EngineStoreServerWrap * server, uint64_t region_id)
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

EngineStoreApplyRes HandleIngestSST(EngineStoreServerWrap * server, SSTViewVec snaps, RaftCmdHeader header)
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

uint8_t HandleCheckTerminated(EngineStoreServerWrap * server)
{
    return server->tmt->getTerminated().load(std::memory_order_relaxed) ? 1 : 0;
}

StoreStats HandleComputeStoreStats(EngineStoreServerWrap * server)
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

EngineStoreServerStatus HandleGetTiFlashStatus(EngineStoreServerWrap * server) { return server->status.load(); }

RaftProxyStatus TiFlashRaftProxyHelper::getProxyStatus() const { return fn_handle_get_proxy_status(proxy_ptr); }

BaseBuffView strIntoView(const std::string & view) { return BaseBuffView(view.data(), view.size()); }

bool TiFlashRaftProxyHelper::checkEncryptionEnabled() const { return fn_is_encryption_enabled(proxy_ptr); }
EncryptionMethod TiFlashRaftProxyHelper::getEncryptionMethod() const { return fn_encryption_method(proxy_ptr); }
FileEncryptionInfo TiFlashRaftProxyHelper::getFile(const std::string & view) const
{
    return fn_handle_get_file(proxy_ptr, strIntoView(view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::newFile(const std::string & view) const
{
    return fn_handle_new_file(proxy_ptr, strIntoView(view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::deleteFile(const std::string & view) const
{
    return fn_handle_delete_file(proxy_ptr, strIntoView(view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::linkFile(const std::string & src, const std::string & dst) const
{
    return fn_handle_link_file(proxy_ptr, strIntoView(src), strIntoView(dst));
}

struct CppStrVec
{
    std::vector<std::string> data;
    std::vector<BaseBuffView> view;
    CppStrVec(std::vector<std::string> && data_) : data(std::move(data_)) { updateView(); }
    CppStrVec(const CppStrVec &) = delete;
    void updateView();
    CppStrVecView intoOuterView() const { return {view.data(), view.size()}; }
};

void CppStrVec::updateView()
{
    view.clear();
    view.reserve(data.size());
    for (const auto & e : data)
    {
        view.emplace_back(strIntoView(e));
    }
}

kvrpcpb::ReadIndexResponse TiFlashRaftProxyHelper::readIndex(const kvrpcpb::ReadIndexRequest & req) const
{
    auto res = batchReadIndex({req}, DEFAULT_BATCH_READ_INDEX_TIMEOUT_MS);
    return std::move(res->at(0).first);
}

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> & req, uint64_t timeout_ms) const
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
    BatchReadIndexRes res(reinterpret_cast<BatchReadIndexRes::pointer>(fn_handle_batch_read_index(proxy_ptr, outer_view, timeout_ms)));
    return res;
}

struct PreHandledSnapshot
{
    ~PreHandledSnapshot() { CurrentMetrics::sub(CurrentMetrics::RaftNumSnapshotsPendingApply); }
    PreHandledSnapshot(const RegionPtr & region_, RegionPtrWithBlock::CachePtr && cache_) : region(region_), cache(std::move(cache_))
    {
        CurrentMetrics::add(CurrentMetrics::RaftNumSnapshotsPendingApply);
    }
    RegionPtr region;
    RegionPtrWithBlock::CachePtr cache;
};

RawCppPtr PreHandleSnapshot(
    EngineStoreServerWrap * server, BaseBuffView region_buff, uint64_t peer_id, SSTViewVec snaps, uint64_t index, uint64_t term)
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
        return GenRawCppPtr(res, RawCppPtrTypeImpl::PreHandledSnapshot);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledSnapshot(EngineStoreServerWrap * server, PreHandledSnapshot * snap)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        kvstore->handlePreApplySnapshot(RegionPtrWithBlock{snap->region, std::move(snap->cache)}, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ApplyPreHandledSnapshot(EngineStoreServerWrap * server, RawVoidPtr res, RawCppPtrType type)
{
    switch (static_cast<RawCppPtrTypeImpl>(type))
    {
        case RawCppPtrTypeImpl::PreHandledSnapshot:
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

void GcRawCppPtr(EngineStoreServerWrap *, RawVoidPtr ptr, RawCppPtrType type)
{
    if (ptr)
    {
        switch (static_cast<RawCppPtrTypeImpl>(type))
        {
            case RawCppPtrTypeImpl::String:
                delete reinterpret_cast<RawCppStringPtr>(ptr);
                break;
            case RawCppPtrTypeImpl::PreHandledSnapshot:
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

RawVoidPtr GenBatchReadIndexRes(uint64_t cap)
{
    auto res = new BatchReadIndexRes::element_type();
    res->reserve(cap);
    return res;
}

void InsertBatchReadIndexResp(RawVoidPtr resp, BaseBuffView view, uint64_t region_id)
{
    kvrpcpb::ReadIndexResponse res;
    res.ParseFromArray(view.data, view.len);
    reinterpret_cast<BatchReadIndexRes::pointer>(resp)->emplace_back(std::move(res), region_id);
}

RawCppPtr GenRawCppPtr(RawVoidPtr ptr_, RawCppPtrTypeImpl type_) { return RawCppPtr{ptr_, static_cast<RawCppPtrType>(type_)}; }

} // namespace DB
