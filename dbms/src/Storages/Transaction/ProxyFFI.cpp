// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CurrentMetrics.h>
#include <Common/nocopyable.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/FileEncryption.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ReadIndexWorker.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <kvproto/diagnosticspb.pb.h>

#include <ext/scope_guard.h>

#define CHECK_PARSE_PB_BUFF_IMPL(n, a, b, c)                                              \
    do                                                                                    \
    {                                                                                     \
        [[maybe_unused]] bool parse_res_##n = (a).ParseFromArray(b, static_cast<int>(c)); \
        assert(parse_res_##n);                                                            \
    } while (false)
#define CHECK_PARSE_PB_BUFF_FWD(n, ...) CHECK_PARSE_PB_BUFF_IMPL(n, __VA_ARGS__)
#define CHECK_PARSE_PB_BUFF(...) CHECK_PARSE_PB_BUFF_FWD(__LINE__, __VA_ARGS__)

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

EngineStoreApplyRes HandleWriteRaftCmd(
    const EngineStoreServerWrap * server,
    WriteCmdsView cmds,
    RaftCmdHeader header)
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
    const EngineStoreServerWrap * server,
    BaseBuffView req_buff,
    BaseBuffView resp_buff,
    RaftCmdHeader header)
{
    try
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        CHECK_PARSE_PB_BUFF(request, req_buff.data, req_buff.len);
        CHECK_PARSE_PB_BUFF(response, resp_buff.data, resp_buff.len);
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->handleAdminRaftCmd(
            std::move(request),
            std::move(response),
            header.region_id,
            header.index,
            header.term,
            *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t NeedFlushData(EngineStoreServerWrap * server, uint64_t region_id)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->needFlushRegionData(region_id, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t TryFlushData(EngineStoreServerWrap * server, uint64_t region_id, uint8_t flush_pattern, uint64_t index, uint64_t term)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->tryFlushRegionData(region_id, false, flush_pattern, *server->tmt, index, term);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

static_assert(sizeof(RaftStoreProxyFFIHelper) == sizeof(TiFlashRaftProxyHelper));
static_assert(alignof(RaftStoreProxyFFIHelper) == alignof(TiFlashRaftProxyHelper));

struct RustGcHelper : public ext::Singleton<RustGcHelper>
{
    void gcRustPtr(RawVoidPtr ptr, RawRustPtrType type) const
    {
        fn_gc_rust_ptr(ptr, type);
    }

    RustGcHelper() = default;

    void setRustPtrGcFn(void (*fn_gc_rust_ptr)(RawVoidPtr, RawRustPtrType))
    {
        this->fn_gc_rust_ptr = fn_gc_rust_ptr;
    }

private:
    void (*fn_gc_rust_ptr)(RawVoidPtr, RawRustPtrType);
};

void AtomicUpdateProxy(DB::EngineStoreServerWrap * server, RaftStoreProxyFFIHelper * proxy)
{
    // any usage towards proxy helper must happen after this function.
    {
        // init global rust gc function pointer here.
        RustGcHelper::instance().setRustPtrGcFn(proxy->fn_gc_rust_ptr);
    }
    server->proxy_helper = static_cast<TiFlashRaftProxyHelper *>(proxy);
    std::atomic_thread_fence(std::memory_order_seq_cst);
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


StoreStats HandleComputeStoreStats(EngineStoreServerWrap * server)
{
    StoreStats res{}; // res.fs_stats.ok = false by default
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

EngineStoreServerStatus HandleGetTiFlashStatus(EngineStoreServerWrap * server)
{
    return server->status.load();
}

RaftProxyStatus TiFlashRaftProxyHelper::getProxyStatus() const
{
    return fn_handle_get_proxy_status(proxy_ptr);
}

/// Use const pointer instead of const ref to avoid lifetime of `str` is shorter than view.
BaseBuffView strIntoView(const std::string * str_ptr)
{
    assert(str_ptr);
    return BaseBuffView{str_ptr->data(), str_ptr->size()};
}

bool TiFlashRaftProxyHelper::checkEncryptionEnabled() const
{
    return fn_is_encryption_enabled(proxy_ptr);
}
EncryptionMethod TiFlashRaftProxyHelper::getEncryptionMethod() const
{
    return fn_encryption_method(proxy_ptr);
}
FileEncryptionInfo TiFlashRaftProxyHelper::getFile(const std::string & view) const
{
    return fn_handle_get_file(proxy_ptr, strIntoView(&view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::newFile(const std::string & view) const
{
    return fn_handle_new_file(proxy_ptr, strIntoView(&view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::deleteFile(const std::string & view) const
{
    return fn_handle_delete_file(proxy_ptr, strIntoView(&view));
}
FileEncryptionInfo TiFlashRaftProxyHelper::linkFile(const std::string & src, const std::string & dst) const
{
    return fn_handle_link_file(proxy_ptr, strIntoView(&src), strIntoView(&dst));
}

struct CppStrVec
{
    std::vector<std::string> data;
    std::vector<BaseBuffView> view;
    explicit CppStrVec(std::vector<std::string> && data_)
        : data(std::move(data_))
    {
        updateView();
    }
    DISALLOW_COPY(CppStrVec);
    void updateView();
    CppStrVecView intoOuterView() const { return {view.data(), view.size()}; }
};

void CppStrVec::updateView()
{
    view.clear();
    view.reserve(data.size());
    for (const auto & e : data)
    {
        view.emplace_back(strIntoView(&e));
    }
}

void InsertBatchReadIndexResp(RawVoidPtr resp, BaseBuffView view, uint64_t region_id)
{
    kvrpcpb::ReadIndexResponse res;
    CHECK_PARSE_PB_BUFF(res, view.data, view.len);
    reinterpret_cast<BatchReadIndexRes *>(resp)->emplace_back(std::move(res), region_id);
}

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex_v1(const std::vector<kvrpcpb::ReadIndexRequest> & req, uint64_t timeout_ms) const
{
    std::vector<std::string> req_strs;
    req_strs.reserve(req.size());
    for (const auto & r : req)
    {
        req_strs.emplace_back(r.SerializeAsString());
    }
    CppStrVec data(std::move(req_strs));
    auto outer_view = data.intoOuterView();
    BatchReadIndexRes res;
    res.reserve(req.size());
    fn_handle_batch_read_index(proxy_ptr, outer_view, &res, timeout_ms, InsertBatchReadIndexResp);
    return res;
}

RawRustPtrWrap::RawRustPtrWrap(RawRustPtr inner)
    : RawRustPtr(inner)
{
}

RawRustPtrWrap::~RawRustPtrWrap()
{
    if (ptr == nullptr)
        return;
    RustGcHelper::instance().gcRustPtr(ptr, type);
}
RawRustPtrWrap::RawRustPtrWrap(RawRustPtrWrap && src)
    : RawRustPtr()
{
    RawRustPtr & tar = (*this);
    tar = src;
    src.ptr = nullptr;
}

struct PreHandledSnapshotWithFiles
{
    ~PreHandledSnapshotWithFiles() { CurrentMetrics::sub(CurrentMetrics::RaftNumSnapshotsPendingApply); }
    PreHandledSnapshotWithFiles(const RegionPtr & region_, std::vector<DM::ExternalDTFileInfo> && external_files_)
        : region(region_)
        , external_files(std::move(external_files_))
    {
        CurrentMetrics::add(CurrentMetrics::RaftNumSnapshotsPendingApply);
    }
    RegionPtr region;
    std::vector<DM::ExternalDTFileInfo> external_files; // The file_ids storing pre-handled files
};

RawCppPtr PreHandleSnapshot(
    EngineStoreServerWrap * server,
    BaseBuffView region_buff,
    uint64_t peer_id,
    SSTViewVec snaps,
    uint64_t index,
    uint64_t term)
{
    try
    {
        metapb::Region region;
        CHECK_PARSE_PB_BUFF(region, region_buff.data, region_buff.len);
        auto & tmt = *server->tmt;
        auto & kvstore = tmt.getKVStore();
        auto new_region = kvstore->genRegionPtr(std::move(region), peer_id, index, term);

#ifndef NDEBUG
        {
            auto & kvstore = server->tmt->getKVStore();
            auto state = kvstore->getProxyHelper()->getRegionLocalState(new_region->id());
            assert(state.state() == raft_serverpb::PeerState::Applying);
        }
#endif

        switch (kvstore->applyMethod())
        {
        case TiDB::SnapshotApplyMethod::DTFile_Directory:
        case TiDB::SnapshotApplyMethod::DTFile_Single:
        {
            // Pre-decode and save as DTFiles
            auto ingest_ids = kvstore->preHandleSnapshotToFiles(new_region, snaps, index, term, tmt);
            auto * res = new PreHandledSnapshotWithFiles{new_region, std::move(ingest_ids)};
            return GenRawCppPtr(res, RawCppPtrTypeImpl::PreHandledSnapshotWithFiles);
        }
        default:
            throw Exception("Unknow Region apply method: " + applyMethodToString(kvstore->applyMethod()));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

template <typename PreHandledSnapshot>
void ApplyPreHandledSnapshot(EngineStoreServerWrap * server, PreHandledSnapshot * snap)
{
    static_assert(std::is_same_v<PreHandledSnapshot, PreHandledSnapshotWithFiles>, "Unknown pre-handled snapshot type");

    try
    {
        auto & kvstore = server->tmt->getKVStore();
        if constexpr (std::is_same_v<PreHandledSnapshot, PreHandledSnapshotWithFiles>)
        {
            kvstore->applyPreHandledSnapshot(RegionPtrWithSnapshotFiles{snap->region, std::move(snap->external_files)}, *server->tmt);
        }
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
    case RawCppPtrTypeImpl::PreHandledSnapshotWithFiles:
    {
        auto * snap = reinterpret_cast<PreHandledSnapshotWithFiles *>(res);
        ApplyPreHandledSnapshot(server, snap);
        break;
    }
    default:
        LOG_ERROR(&Poco::Logger::get(__FUNCTION__), "unknown type {}", type);
        exit(-1);
    }
}

void GcRawCppPtr(RawVoidPtr ptr, RawCppPtrType type)
{
    if (ptr)
    {
        switch (static_cast<RawCppPtrTypeImpl>(type))
        {
        case RawCppPtrTypeImpl::String:
            delete reinterpret_cast<RawCppStringPtr>(ptr);
            break;
        case RawCppPtrTypeImpl::PreHandledSnapshotWithFiles:
            delete reinterpret_cast<PreHandledSnapshotWithFiles *>(ptr);
            break;
        case RawCppPtrTypeImpl::WakerNotifier:
            delete reinterpret_cast<AsyncNotifier *>(ptr);
            break;
        default:
            LOG_ERROR(&Poco::Logger::get(__FUNCTION__), "unknown type {}", type);
            exit(-1);
        }
    }
}

const char * IntoEncryptionMethodName(EncryptionMethod method)
{
    static const char * encryption_method_name[] = {
        "Unknown",
        "Plaintext",
        "Aes128Ctr",
        "Aes192Ctr",
        "Aes256Ctr",
        "SM4Ctr",
    };
    return encryption_method_name[static_cast<uint8_t>(method)];
}

RawCppPtr GenRawCppPtr(RawVoidPtr ptr_, RawCppPtrTypeImpl type_)
{
    return RawCppPtr{ptr_, static_cast<RawCppPtrType>(type_)};
}

CppStrWithView GetConfig(EngineStoreServerWrap * server, [[maybe_unused]] uint8_t full)
{
    std::string config_file_path;
    try
    {
        config_file_path = server->tmt->getContext().getConfigRef().getString("config-file");
        std::ifstream stream(config_file_path);
        if (!stream)
            return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}};
        auto * s = RawCppString::New((std::istreambuf_iterator<char>(stream)),
                                     std::istreambuf_iterator<char>());
        stream.close();
        /** the returned str must be formated as TOML, proxy will parse and show in form of JASON.
         *  curl `http://{status-addr}/config`, got:
         *  {"raftstore-proxy":xxxx,"engine-store":xxx}
         *
         *  if proxy can NOT parse it, return 500 Internal Server Error.
         * */
        return CppStrWithView{.inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String), .view = BaseBuffView{s->data(), s->size()}};
    }
    catch (...)
    {
        return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}};
    }
}

void SetStore(EngineStoreServerWrap * server, BaseBuffView buff)
{
    metapb::Store store{};
    CHECK_PARSE_PB_BUFF(store, buff.data, buff.len);
    assert(server);
    assert(server->tmt);
    assert(store.id() != 0);
    server->tmt->getKVStore()->setStore(std::move(store));
}

void MockSetFFI::MockSetRustGcHelper(void (*fn_gc_rust_ptr)(RawVoidPtr, RawRustPtrType))
{
    LOG_WARNING(&Poco::Logger::get(__FUNCTION__), "Set mock rust ptr gc function");
    RustGcHelper::instance().setRustPtrGcFn(fn_gc_rust_ptr);
}

void SetPBMsByBytes(MsgPBType type, RawVoidPtr ptr, BaseBuffView view)
{
    switch (type)
    {
    case MsgPBType::ReadIndexResponse:
        CHECK_PARSE_PB_BUFF(*reinterpret_cast<kvrpcpb::ReadIndexResponse *>(ptr), view.data, view.len);
        break;
    case MsgPBType::RegionLocalState:
        CHECK_PARSE_PB_BUFF(*reinterpret_cast<raft_serverpb::RegionLocalState *>(ptr), view.data, view.len);
        break;
    case MsgPBType::ServerInfoResponse:
        CHECK_PARSE_PB_BUFF(*reinterpret_cast<diagnosticspb::ServerInfoResponse *>(ptr), view.data, view.len);
        break;
    }
}

raft_serverpb::RegionLocalState TiFlashRaftProxyHelper::getRegionLocalState(uint64_t region_id) const
{
    assert(this->fn_get_region_local_state);

    raft_serverpb::RegionLocalState state;
    RawCppStringPtr error_msg_ptr{};
    SCOPE_EXIT({
        delete error_msg_ptr;
    });
    auto res = this->fn_get_region_local_state(this->proxy_ptr, region_id, &state, &error_msg_ptr);
    switch (res)
    {
    case KVGetStatus::Ok:
        break;
    case KVGetStatus::Error:
    {
        throw Exception(fmt::format("{} meet internal error: {}", __FUNCTION__, *error_msg_ptr), ErrorCodes::LOGICAL_ERROR);
    }
    case KVGetStatus::NotFound:
        // make not found as `Tombstone`
        state.set_state(raft_serverpb::PeerState::Tombstone);
        break;
    }
    return state;
}

void HandleSafeTSUpdate(EngineStoreServerWrap * server, uint64_t region_id, uint64_t self_safe_ts, uint64_t leader_safe_ts)
{
    RegionTable & region_table = server->tmt->getRegionTable();
    region_table.updateSafeTS(region_id, leader_safe_ts, self_safe_ts);
}


std::string_view buffToStrView(const BaseBuffView & buf)
{
    return std::string_view{buf.data, buf.len};
}

} // namespace DB
