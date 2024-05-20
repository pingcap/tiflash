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
#include <Common/TiFlashMetrics.h>
#include <Common/nocopyable.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/PathCapacityMetrics.h>
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
        throw Exception(
            "Can not tell cf type " + std::to_string(static_cast<uint8_t>(type)),
            ErrorCodes::LOGICAL_ERROR);
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
        return server->tmt->getKVStore()
            ->handleWriteRaftCmd(cmds, header.region_id, header.index, header.term, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t TryFlushData(
    EngineStoreServerWrap * server,
    uint64_t region_id,
    uint8_t flush_pattern,
    uint64_t index,
    uint64_t term,
    uint64_t truncated_index,
    uint64_t truncated_term)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->tryFlushRegionData(
            region_id,
            false,
            flush_pattern,
            *server->tmt,
            index,
            term,
            truncated_index,
            truncated_term);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

RawCppPtr CreateWriteBatch(const EngineStoreServerWrap * dummy)
{
    try
    {
        // Don't move the dummy argument, it is useful on proxy's side.
        // This function is not protected by try-catch, since it's rarely throw.
        UNUSED(dummy);
        return GenRawCppPtr(new UniversalWriteBatch(), RawCppPtrTypeImpl::WriteBatch);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void WriteBatchPutPage(RawVoidPtr ptr, BaseBuffView page_id, BaseBuffView value)
{
    try
    {
        LOG_TRACE(&Poco::Logger::get("ProxyFFI"), "FFI write page {}", UniversalPageId(page_id.data, page_id.len));
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        MemoryWriteBuffer buf(0, value.len);
        buf.write(value.data, value.len);
        auto data_size = buf.count();
        assert(data_size == value.len);
        wb->putPage(UniversalPageId(page_id.data, page_id.len), 0, buf.tryGetReadBuffer(), data_size);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void WriteBatchDelPage(RawVoidPtr ptr, BaseBuffView page_id)
{
    try
    {
        LOG_TRACE(&Poco::Logger::get("ProxyFFI"), "FFI delete page {}", UniversalPageId(page_id.data, page_id.len));
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        wb->delPage(UniversalPageId(page_id.data, page_id.len));
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint64_t GetWriteBatchSize(RawVoidPtr ptr)
{
    try
    {
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        return wb->getTotalDataSize();
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t IsWriteBatchEmpty(RawVoidPtr ptr)
{
    try
    {
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        return wb->empty();
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleMergeWriteBatch(RawVoidPtr lhs, RawVoidPtr rhs)
{
    try
    {
        auto * lwb = reinterpret_cast<UniversalWriteBatch *>(lhs);
        auto * rwb = reinterpret_cast<UniversalWriteBatch *>(rhs);
        lwb->merge(*rwb);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleClearWriteBatch(RawVoidPtr ptr)
{
    try
    {
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        wb->clear();
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandleConsumeWriteBatch(const EngineStoreServerWrap * server, RawVoidPtr ptr)
{
    try
    {
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        auto * wb = reinterpret_cast<UniversalWriteBatch *>(ptr);
        LOG_TRACE(&Poco::Logger::get("ProxyFFI"), "FFI consume write batch {}", wb->toString());
        uni_ps->write(std::move(*wb), DB::PS::V3::PageType::RaftData, nullptr);
        wb->clear();
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

CppStrWithView HandleReadPage(const EngineStoreServerWrap * server, BaseBuffView page_id)
{
    try
    {
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        RaftDataReader reader(*uni_ps);
        auto * page = new Page(reader.read(UniversalPageId(page_id.data, page_id.len)));
        if (page->isValid())
        {
            LOG_TRACE(
                &Poco::Logger::get("ProxyFFI"),
                "FFI read page {} success",
                UniversalPageId(page_id.data, page_id.len));
            return CppStrWithView{
                .inner = GenRawCppPtr(page, RawCppPtrTypeImpl::UniversalPage),
                .view = BaseBuffView{page->data.begin(), page->data.size()},
            };
        }
        else
        {
            LOG_TRACE(
                &Poco::Logger::get("ProxyFFI"),
                "FFI read page {} fail",
                UniversalPageId(page_id.data, page_id.len));
            return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}};
        }
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

RawCppPtrCarr HandleScanPage(const EngineStoreServerWrap * server, BaseBuffView start_page_id, BaseBuffView end_page_id)
{
    try
    {
        LOG_TRACE(
            &Poco::Logger::get("ProxyFFI"),
            "FFI scan page from {} to {}",
            UniversalPageId(start_page_id.data, start_page_id.len),
            UniversalPageId(end_page_id.data, end_page_id.len));
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        RaftDataReader reader(*uni_ps);
        std::vector<UniversalPageId> page_ids;
        std::vector<DB::Page *> pages;
        auto checker = [&](const UniversalPageId & page_id, DB::Page page) {
            page_ids.push_back(page_id);
            pages.push_back(new Page(std::move(page)));
        };
        reader.traverse(
            UniversalPageId(start_page_id.data, start_page_id.len),
            UniversalPageId(end_page_id.data, end_page_id.len),
            checker);
        auto * data = static_cast<PageAndCppStrWithView *>(
            malloc(pages.size() * sizeof(PageAndCppStrWithView))); // NOLINT(cppcoreguidelines-no-malloc)
        for (size_t i = 0; i < pages.size(); i++)
        {
            auto * target = data + i;
            auto * key_str = RawCppString::New(page_ids[i].data(), page_ids[i].size());
            new (target) PageAndCppStrWithView{
                .page = GenRawCppPtr(pages[i], RawCppPtrTypeImpl::UniversalPage),
                .key = GenRawCppPtr(key_str, RawCppPtrTypeImpl::String),
                .page_view = BaseBuffView{.data = pages[i]->data.begin(), .len = pages[i]->data.size()},
                .key_view = BaseBuffView{.data = key_str->data(), .len = key_str->size()}};
        }
        return RawCppPtrCarr{
            .inner = data,
            .len = pages.size(),
            .type = static_cast<RawCppPtrType>(RawCppPtrTypeImpl::PageAndCppStr),
        };
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

CppStrWithView HandleGetLowerBound(const EngineStoreServerWrap * server, BaseBuffView raw_page_id)
{
    try
    {
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        RaftDataReader reader(*uni_ps);
        auto page_id_opt = reader.getLowerBound(UniversalPageId(raw_page_id.data, raw_page_id.len));
        if (page_id_opt.has_value())
        {
            LOG_TRACE(
                &Poco::Logger::get("ProxyFFI"),
                "FFI get lower bound for page {} success",
                UniversalPageId(raw_page_id.data, raw_page_id.len));
            auto * s = RawCppString::New(page_id_opt->asStr());
            return CppStrWithView{
                .inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String),
                .view = BaseBuffView{s->data(), s->size()},
            };
        }
        else
        {
            LOG_TRACE(
                &Poco::Logger::get("ProxyFFI"),
                "FFI get lower bound for page {} fail",
                UniversalPageId(raw_page_id.data, raw_page_id.len));
            return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}};
        }
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

uint8_t IsPSEmpty(const EngineStoreServerWrap * server)
{
    try
    {
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        return uni_ps->isEmpty();
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void HandlePurgePageStorage(const EngineStoreServerWrap * server)
{
    try
    {
        auto uni_ps = server->tmt->getContext().getWriteNodePageStorage();
        uni_ps->gc({});
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

static_assert(sizeof(RaftStoreProxyFFIHelper) == sizeof(TiFlashRaftProxyHelper));
static_assert(alignof(RaftStoreProxyFFIHelper) == alignof(TiFlashRaftProxyHelper));

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
        Stopwatch watch;
        kvstore->handleDestroy(region_id, *server->tmt);
        GET_METRIC(tiflash_raft_command_duration_seconds, type_remove_peer).Observe(watch.elapsedSeconds());
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
    return FileEncryptionInfo(fn_handle_get_file(proxy_ptr, strIntoView(&view)));
}
FileEncryptionInfo TiFlashRaftProxyHelper::newFile(const std::string & view) const
{
    return FileEncryptionInfo(fn_handle_new_file(proxy_ptr, strIntoView(&view)));
}
FileEncryptionInfo TiFlashRaftProxyHelper::deleteFile(const std::string & view) const
{
    return FileEncryptionInfo(fn_handle_delete_file(proxy_ptr, strIntoView(&view)));
}
FileEncryptionInfo TiFlashRaftProxyHelper::linkFile(const std::string & src, const std::string & dst) const
{
    return FileEncryptionInfo(fn_handle_link_file(proxy_ptr, strIntoView(&src), strIntoView(&dst)));
}
String TiFlashRaftProxyHelper::getMasterKey() const
{
    return String(*cloud_storage_engine_interfaces.fn_get_master_key(proxy_ptr));
}
bool TiFlashRaftProxyHelper::getKeyspaceEncryption(uint32_t keyspace_id) const
{
    return cloud_storage_engine_interfaces.fn_get_keyspace_encryption(proxy_ptr, keyspace_id);
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

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex_v1(
    const std::vector<kvrpcpb::ReadIndexRequest> & req,
    uint64_t timeout_ms) const
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
{}

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
    ~PreHandledSnapshotWithFiles()
    {
        CurrentMetrics::sub(CurrentMetrics::RaftNumSnapshotsPendingApply);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_raft_snapshot)
            .Decrement(prehandle_result.stats.raft_snapshot_bytes);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_dt_on_disk)
            .Decrement(prehandle_result.stats.dt_disk_bytes);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_dt_total)
            .Decrement(prehandle_result.stats.dt_total_bytes);
    }
    PreHandledSnapshotWithFiles(const RegionPtr & region_, PrehandleResult && prehandle_result_)
        : region(region_)
        , prehandle_result(std::move(prehandle_result_))
    {
        CurrentMetrics::add(CurrentMetrics::RaftNumSnapshotsPendingApply);
        if (prehandle_result.stats.approx_raft_snapshot_size)
            GET_METRIC(tiflash_raft_snapshot_total_bytes, type_approx_raft_snapshot)
                .Observe(prehandle_result.stats.approx_raft_snapshot_size);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_raft_snapshot)
            .Increment(prehandle_result.stats.raft_snapshot_bytes);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_dt_on_disk)
            .Increment(prehandle_result.stats.dt_disk_bytes);
        GET_METRIC(tiflash_raft_ongoing_snapshot_total_bytes, type_dt_total)
            .Increment(prehandle_result.stats.dt_total_bytes);
    }
    RegionPtr region;
    PrehandleResult prehandle_result; // The file_ids storing pre-handled files
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

        // Pre-decode and save as DTFiles
        // TODO(raftstore-v2) Forward deadline_index when TiKV supports.
        auto prehandle_result = kvstore->preHandleSnapshotToFiles(new_region, snaps, index, term, std::nullopt, tmt);
        auto * res = new PreHandledSnapshotWithFiles{new_region, std::move(prehandle_result)};
        return GenRawCppPtr(res, RawCppPtrTypeImpl::PreHandledSnapshotWithFiles);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        try
        {
            auto & kvstore = server->tmt->getKVStore();
            kvstore->applyPreHandledSnapshot(
                RegionPtrWithSnapshotFiles{snap->region, std::move(snap->prehandle_result.ingest_ids)},
                *server->tmt);
            uint64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            double elapsed = (end_time - snap->prehandle_result.stats.start_time) / 1000.0;
            GET_METRIC(tiflash_raft_command_duration_seconds, type_apply_snapshot_total).Observe(elapsed);
        }
        catch (...)
        {
            tryLogCurrentFatalException(__PRETTY_FUNCTION__);
            exit(-1);
        }
        break;
    }
    default:
        LOG_FATAL(&Poco::Logger::get(__FUNCTION__), "unknown type {}", type);
        exit(-1);
    }
}

void AbortPreHandledSnapshot(EngineStoreServerWrap * server, uint64_t region_id, uint64_t peer_id)
{
    try
    {
        UNUSED(peer_id);
        auto & kvstore = server->tmt->getKVStore();
        kvstore->abortPreHandleSnapshot(region_id, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ReleasePreHandledSnapshot(EngineStoreServerWrap * server, RawVoidPtr res, RawCppPtrType type)
{
    if (static_cast<RawCppPtrTypeImpl>(type) != RawCppPtrTypeImpl::PreHandledSnapshotWithFiles)
    {
        LOG_FATAL(&Poco::Logger::get(__FUNCTION__), "unknown type {}", type);
        exit(-1);
    }

    auto * snap = reinterpret_cast<PreHandledSnapshotWithFiles *>(res);
    try
    {
        auto s = RegionPtrWithSnapshotFiles{snap->region, std::move(snap->prehandle_result.ingest_ids)};
        auto & kvstore = server->tmt->getKVStore();
        kvstore->releasePreHandledSnapshot(s, *server->tmt);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

bool KvstoreRegionExists(EngineStoreServerWrap * server, uint64_t region_id)
{
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        return kvstore->getRegion(region_id) != nullptr;
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
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
        case RawCppPtrTypeImpl::WriteBatch:
            delete reinterpret_cast<UniversalWriteBatch *>(ptr);
            break;
        case RawCppPtrTypeImpl::UniversalPage:
            delete reinterpret_cast<Page *>(ptr);
            break;
        default:
            LOG_FATAL(&Poco::Logger::get(__FUNCTION__), "unknown type {}", type);
            exit(-1);
        }
    }
}

void GcRawCppPtrCArr(RawVoidPtr ptr, RawCppPtrType type, uint64_t len)
{
    if (ptr)
    {
        switch (static_cast<RawCppPtrTypeImpl>(type))
        {
        case RawCppPtrTypeImpl::PageAndCppStr:
        {
            auto * inner = reinterpret_cast<PageAndCppStrWithView *>(ptr);
            for (size_t i = 0; i < len; i++)
            {
                GcRawCppPtr(inner[i].page.ptr, inner[i].page.type);
                GcRawCppPtr(inner[i].key.ptr, inner[i].key.type);
            }
            delete inner;
            break;
        }
        default:
            LOG_FATAL(&Poco::Logger::get(__FUNCTION__), "unknown type arr {}", type);
            exit(-1);
        }
    }
}

void GcSpecialRawCppPtr(void * ptr, uint64_t hint_size, SpecialCppPtrType type)
{
    UNUSED(hint_size);
    if (ptr)
    {
        switch (static_cast<SpecialCppPtrType>(type))
        {
        case SpecialCppPtrType::None:
            // Do nothing.
            break;
        case SpecialCppPtrType::TupleOfRawCppPtr:
        {
            auto * special_ptr = reinterpret_cast<RawCppPtrTuple *>(ptr);
            delete special_ptr->inner;
            delete special_ptr;
            break;
        }
        case SpecialCppPtrType::ArrayOfRawCppPtr:
        {
            auto * special_ptr = reinterpret_cast<RawCppPtrArr *>(ptr);
            delete special_ptr->inner;
            delete special_ptr;
            break;
        }
        default:
            LOG_FATAL(
                &Poco::Logger::get(__FUNCTION__),
                "unknown type {}",
                static_cast<std::underlying_type_t<SpecialCppPtrType>>(type));
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
            return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}};
        auto * s = RawCppString::New((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
        stream.close();
        /** the returned str must be formated as TOML, proxy will parse and show in form of JASON.
         *  curl `http://{status-addr}/config`, got:
         *  {"raftstore-proxy":xxxx,"engine-store":xxx}
         *
         *  if proxy can NOT parse it, return 500 Internal Server Error.
         * */
        return CppStrWithView{
            .inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{s->data(), s->size()}};
    }
    catch (...)
    {
        return CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}};
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
    SCOPE_EXIT({ delete error_msg_ptr; });
    auto res = this->fn_get_region_local_state(this->proxy_ptr, region_id, &state, &error_msg_ptr);
    switch (res)
    {
    case KVGetStatus::Ok:
        break;
    case KVGetStatus::Error:
    {
        throw Exception(
            fmt::format("{} meet internal error: {}", __FUNCTION__, *error_msg_ptr),
            ErrorCodes::LOGICAL_ERROR);
    }
    case KVGetStatus::NotFound:
        // make not found as `Tombstone`
        state.set_state(raft_serverpb::PeerState::Tombstone);
        break;
    }
    return state;
}

void TiFlashRaftProxyHelper::notifyCompactLog(
    uint64_t region_id,
    uint64_t compact_index,
    uint64_t compact_term,
    uint64_t applied_index) const
{
    this->fn_notify_compact_log(this->proxy_ptr, region_id, compact_index, compact_term, applied_index);
}

void HandleSafeTSUpdate(
    EngineStoreServerWrap * server,
    uint64_t region_id,
    uint64_t self_safe_ts,
    uint64_t leader_safe_ts)
{
    try
    {
        RegionTable & region_table = server->tmt->getRegionTable();
        region_table.updateSafeTS(region_id, leader_safe_ts, self_safe_ts);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

std::string_view buffToStrView(const BaseBuffView & buf)
{
    return std::string_view{buf.data, buf.len};
}

// `s` must outlive returned base buff view.
BaseBuffView cppStringAsBuff(const std::string & s)
{
    return BaseBuffView{.data = s.data(), .len = s.size()};
}

BaseBuffView GetLockByKey(const EngineStoreServerWrap * server, uint64_t region_id, BaseBuffView key)
{
    auto tikv_key = TiKVKey(key.data, key.len);
    try
    {
        auto & kvstore = server->tmt->getKVStore();
        auto region = kvstore->getRegion(region_id);
        auto value = region->getLockByKey(tikv_key);
        if (!value)
        {
            // key not exist
            LOG_WARNING(
                Logger::get(),
                "Failed to get lock by key {}, region_id={}",
                tikv_key.toDebugString(),
                region_id);
            return BaseBuffView{};
        }

        return BaseBuffView{value->data(), value->dataSize()};
    }
    catch (...)
    {
        LOG_WARNING( //
            Logger::get(),
            "Failed to get lock by key {}, region_id={}",
            tikv_key.toDebugString(),
            region_id);
        return BaseBuffView{};
    }
}

void ReportThreadAllocateInfo(
    EngineStoreServerWrap * server,
    uint64_t tid,
    BaseBuffView name,
    ReportThreadAllocateInfoType type,
    uint64_t value)
{
    try
    {
        UNUSED(tid);
        if (!server || !server->tmt || !server->tmt->getKVStore())
            return;
        server->tmt->getKVStore()->reportThreadAllocInfo(buffToStrView(name), type, value);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

void ReportThreadAllocateBatch(
    EngineStoreServerWrap * server,
    uint64_t tid,
    BaseBuffView name,
    ReportThreadAllocateInfoBatch data)
{
    try
    {
        UNUSED(tid);
        if (!server || !server->tmt || !server->tmt->getKVStore())
            return;
        server->tmt->getKVStore()->reportThreadAllocBatch(buffToStrView(name), data);
    }
    catch (...)
    {
        tryLogCurrentFatalException(__PRETTY_FUNCTION__);
        exit(-1);
    }
}

} // namespace DB