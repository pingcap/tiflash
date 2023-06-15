// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <Debug/MockRaftStoreProxy.h>
#include <Debug/MockSSTReader.h>
#include <Debug/MockTiDB.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/RowCodec.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <google/protobuf/text_format.h>

namespace DB
{
namespace RegionBench
{
extern void setupPutRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &, const TiKVValue &);
extern void setupDelRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &);
} // namespace RegionBench

kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts)
{
    kvrpcpb::ReadIndexRequest req;
    req.set_start_ts(start_ts);
    req.mutable_context()->set_region_id(region_id);
    return req;
}

MockRaftStoreProxy & as_ref(RaftStoreProxyPtr ptr)
{
    return *reinterpret_cast<MockRaftStoreProxy *>(reinterpret_cast<size_t>(ptr.inner));
}

extern void mock_set_rust_gc_helper(void (*)(RawVoidPtr, RawRustPtrType));

RawRustPtr fn_make_read_index_task(RaftStoreProxyPtr ptr, BaseBuffView view)
{
    auto & x = as_ref(ptr);
    kvrpcpb::ReadIndexRequest req;
    req.ParseFromArray(view.data, view.len);
    auto * task = x.makeReadIndexTask(req);
    if (task)
        GCMonitor::instance().add(RawObjType::MockReadIndexTask, 1);
    return RawRustPtr{task, static_cast<uint32_t>(RawObjType::MockReadIndexTask)};
}

RawRustPtr fn_make_async_waker(void (*wake_fn)(RawVoidPtr),
                               RawCppPtr data)
{
    auto * p = new MockAsyncWaker{std::make_shared<MockAsyncNotifier>()};
    p->data->data = data;
    p->data->wake_fn = wake_fn;
    GCMonitor::instance().add(RawObjType::MockAsyncWaker, 1);
    return RawRustPtr{p, static_cast<uint32_t>(RawObjType::MockAsyncWaker)};
}

uint8_t fn_poll_read_index_task(RaftStoreProxyPtr, RawVoidPtr task, RawVoidPtr resp, RawVoidPtr waker)
{
    auto & read_index_task = *reinterpret_cast<MockReadIndexTask *>(task);
    auto * async_waker = reinterpret_cast<MockAsyncWaker *>(waker);
    auto res = read_index_task.data->poll(async_waker ? async_waker->data : nullptr);
    if (res)
    {
        auto buff = res->SerializePartialAsString();
        SetPBMsByBytes(MsgPBType::ReadIndexResponse, resp, BaseBuffView{buff.data(), buff.size()});
        return 1;
    }
    else
    {
        return 0;
    }
}

void fn_gc_rust_ptr(RawVoidPtr ptr, RawRustPtrType type_)
{
    if (!ptr)
        return;
    auto type = static_cast<RawObjType>(type_);
    GCMonitor::instance().add(type, -1);
    switch (type)
    {
    case RawObjType::None:
        break;
    case RawObjType::MockReadIndexTask:
        delete reinterpret_cast<MockReadIndexTask *>(ptr);
        break;
    case RawObjType::MockAsyncWaker:
        delete reinterpret_cast<MockAsyncWaker *>(ptr);
        break;
    }
}

void fn_handle_batch_read_index(RaftStoreProxyPtr, CppStrVecView, RawVoidPtr, uint64_t, void (*)(RawVoidPtr, BaseBuffView, uint64_t))
{
    throw Exception("`fn_handle_batch_read_index` is deprecated");
}

KVGetStatus fn_get_region_local_state(RaftStoreProxyPtr ptr, uint64_t region_id, RawVoidPtr data, RawCppStringPtr * error_msg)
{
    if (!ptr.inner)
    {
        *error_msg = RawCppString::New("RaftStoreProxyPtr is none");
        return KVGetStatus::Error;
    }
    auto & x = as_ref(ptr);
    auto region = x.getRegion(region_id);
    if (region)
    {
        auto state = region->getState();
        auto buff = state.SerializePartialAsString();
        SetPBMsByBytes(MsgPBType::RegionLocalState, data, BaseBuffView{buff.data(), buff.size()});
        return KVGetStatus::Ok;
    }
    else
        return KVGetStatus::NotFound;
}

RaftstoreVer fn_get_cluster_raftstore_version(RaftStoreProxyPtr ptr,
                                              uint8_t,
                                              int64_t)
{
    auto & x = as_ref(ptr);
    return x.cluster_ver;
}

TiFlashRaftProxyHelper MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(RaftStoreProxyPtr proxy_ptr)
{
    TiFlashRaftProxyHelper res{};
    res.proxy_ptr = proxy_ptr;
    res.fn_make_read_index_task = fn_make_read_index_task;
    res.fn_poll_read_index_task = fn_poll_read_index_task;
    res.fn_make_async_waker = fn_make_async_waker;
    res.fn_handle_batch_read_index = fn_handle_batch_read_index;
    res.fn_get_region_local_state = fn_get_region_local_state;
    res.fn_get_cluster_raftstore_version = fn_get_cluster_raftstore_version;
    {
        // make sure such function pointer will be set at most once.
        static std::once_flag flag;
        std::call_once(flag, []() { MockSetFFI::MockSetRustGcHelper(fn_gc_rust_ptr); });
    }

    return res;
}

raft_serverpb::RegionLocalState MockProxyRegion::getState()
{
    auto _ = genLockGuard();
    return state;
}

raft_serverpb::RaftApplyState MockProxyRegion::getApply()
{
    auto _ = genLockGuard();
    return apply;
}

void MockProxyRegion::updateAppliedIndex(uint64_t index)
{
    auto _ = genLockGuard();
    this->apply.set_applied_index(index);
}

uint64_t MockProxyRegion::getLatestAppliedIndex()
{
    return this->getApply().applied_index();
}

uint64_t MockProxyRegion::getLatestCommitTerm()
{
    return this->getApply().commit_term();
}

uint64_t MockProxyRegion::getLatestCommitIndex()
{
    return this->getApply().commit_index();
}

void MockProxyRegion::updateCommitIndex(uint64_t index)
{
    auto _ = genLockGuard();
    this->apply.set_commit_index(index);
}

void MockProxyRegion::setSate(raft_serverpb::RegionLocalState s)
{
    auto _ = genLockGuard();
    this->state = s;
}

MockProxyRegion::MockProxyRegion(uint64_t id_)
    : id(id_)
{
    apply.set_commit_index(RAFT_INIT_LOG_INDEX);
    apply.set_commit_term(RAFT_INIT_LOG_TERM);
    apply.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply.mutable_truncated_state()->set_index(RAFT_INIT_LOG_INDEX);
    apply.mutable_truncated_state()->set_term(RAFT_INIT_LOG_TERM);
    state.mutable_region()->set_id(id);
}

UniversalWriteBatch MockProxyRegion::persistMeta()
{
    auto _ = genLockGuard();
    auto wb = UniversalWriteBatch();

    auto region_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(this->id);
    auto region_local_state = this->state.SerializeAsString();
    MemoryWriteBuffer buf(0, region_local_state.size());
    buf.write(region_local_state.data(), region_local_state.size());
    wb.putPage(UniversalPageId(region_key.data(), region_key.size()), 0, buf.tryGetReadBuffer(), region_local_state.size());

    auto apply_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(this->id);
    auto raft_apply_state = this->apply.SerializeAsString();
    MemoryWriteBuffer buf2(0, raft_apply_state.size());
    buf2.write(raft_apply_state.data(), raft_apply_state.size());
    wb.putPage(UniversalPageId(apply_key.data(), apply_key.size()), 0, buf2.tryGetReadBuffer(), raft_apply_state.size());

    raft_serverpb::RegionLocalState restored_region_state;
    raft_serverpb::RaftApplyState restored_apply_state;
    restored_region_state.ParseFromArray(region_local_state.data(), region_local_state.size());
    restored_apply_state.ParseFromArray(raft_apply_state.data(), raft_apply_state.size());
    return wb;
}

void MockProxyRegion::addPeer(uint64_t store_id, uint64_t peer_id, metapb::PeerRole role)
{
    auto _ = genLockGuard();
    auto & peer = *state.mutable_region()->mutable_peers()->Add();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(role);
}

std::optional<kvrpcpb::ReadIndexResponse> RawMockReadIndexTask::poll(std::shared_ptr<MockAsyncNotifier> waker)
{
    auto _ = genLockGuard();

    if (!finished)
    {
        if (waker != this->waker)
        {
            this->waker = waker;
        }
        return {};
    }
    if (has_lock)
    {
        resp.mutable_locked();
        return resp;
    }
    if (has_region_error)
    {
        resp.mutable_region_error()->mutable_data_is_not_ready();
        return resp;
    }
    resp.set_read_index(region->getLatestCommitIndex());
    return resp;
}

void RawMockReadIndexTask::update(bool lock, bool region_error)
{
    {
        auto _ = genLockGuard();
        if (finished)
            return;
        finished = true;
        has_lock = lock;
        has_region_error = region_error;
    }
    if (waker)
        waker->wake();
}

MockProxyRegionPtr MockRaftStoreProxy::getRegion(uint64_t id)
{
    auto _ = genLockGuard();
    return doGetRegion(id);
}

MockProxyRegionPtr MockRaftStoreProxy::doGetRegion(uint64_t id)
{
    if (auto it = regions.find(id); it != regions.end())
    {
        return it->second;
    }
    return nullptr;
}

MockReadIndexTask * MockRaftStoreProxy::makeReadIndexTask(kvrpcpb::ReadIndexRequest req)
{
    auto _ = genLockGuard();

    wakeNotifier();

    auto region = doGetRegion(req.context().region_id());
    if (region)
    {
        auto * r = new MockReadIndexTask{};
        r->data = std::make_shared<RawMockReadIndexTask>();
        r->data->req = std::move(req);
        r->data->region = region;
        read_index_tasks.push_back(r->data);
        return r;
    }
    return nullptr;
}

void MockRaftStoreProxy::init(size_t region_num)
{
    auto _ = genLockGuard();
    for (size_t i = 0; i < region_num; ++i)
    {
        regions.emplace(i, std::make_shared<MockProxyRegion>(i));
    }
}

std::unique_ptr<TiFlashRaftProxyHelper> MockRaftStoreProxy::generateProxyHelper()
{
    auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(
        RaftStoreProxyPtr{this}));
    // Bind ffi to MockSSTReader.
    proxy_helper->sst_reader_interfaces = make_mock_sst_reader_interface();
    return proxy_helper;
}

size_t MockRaftStoreProxy::size() const
{
    auto _ = genLockGuard();
    return regions.size();
}

void MockRaftStoreProxy::wakeNotifier()
{
    notifier.wake();
}

void MockRaftStoreProxy::testRunNormal(const std::atomic_bool & over)
{
    while (!over)
    {
        runOneRound();
        notifier.blockedWaitFor(std::chrono::seconds(1));
    }
}

void MockRaftStoreProxy::runOneRound()
{
    auto _ = genLockGuard();
    while (!read_index_tasks.empty())
    {
        auto & t = *read_index_tasks.front();
        auto region_id = t.req.context().region_id();
        if (!region_id_to_drop.contains(region_id))
        {
            if (region_id_to_error.contains(region_id))
                t.update(false, true);
            else
                t.update(false, false);
        }
        read_index_tasks.pop_front();
    }
}

void MockRaftStoreProxy::unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb)
{
    auto _ = genLockGuard();
    cb(*this);
}

void MockRaftStoreProxy::bootstrapWithRegion(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id,
    std::optional<std::pair<std::string, std::string>> maybe_range)
{
    {
        auto _ = genLockGuard();
        RUNTIME_CHECK_MSG(regions.empty(), "Mock Proxy regions are not cleared");
        auto task_lock = kvs.genTaskLock();
        auto lock = kvs.genRegionMgrWriteLock(task_lock);
        RUNTIME_CHECK_MSG(lock.regions.empty(), "KVStore regions are not cleared");
    }
    auto start = RecordKVFormat::genKey(table_id, 0);
    auto end = RecordKVFormat::genKey(table_id + 1, 0);
    debugAddRegions(kvs, tmt, {region_id}, {maybe_range.value_or(std::make_pair(start.toString(), end.toString()))});
}

void MockRaftStoreProxy::debugAddRegions(
    KVStore & kvs,
    TMTContext & tmt,
    std::vector<UInt64> region_ids,
    std::vector<std::pair<std::string, std::string>> && ranges)
{
    UNUSED(tmt);
    int n = ranges.size();
    auto _ = genLockGuard();
    auto task_lock = kvs.genTaskLock();
    auto lock = kvs.genRegionMgrWriteLock(task_lock);
    for (int i = 0; i < n; ++i)
    {
        regions.emplace(region_ids[i], std::make_shared<MockProxyRegion>(region_ids[i]));
        auto region = tests::makeRegion(region_ids[i], ranges[i].first, ranges[i].second, kvs.getProxyHelper());
        lock.regions.emplace(region_ids[i], region);
        lock.index.add(region);
    }
}

std::tuple<uint64_t, uint64_t> MockRaftStoreProxy::normalWrite(
    UInt64 region_id,
    std::vector<HandleID> && keys,
    std::vector<std::string> && vals,
    std::vector<WriteCmdType> && cmd_types,
    std::vector<ColumnFamilyType> && cmd_cf)
{
    uint64_t index = 0;
    uint64_t term = 0;
    {
        auto region = getRegion(region_id);
        assert(region != nullptr);
        // We have a new entry.
        index = region->getLatestCommitIndex() + 1;
        term = region->getLatestCommitTerm();
        // The new entry is committed on Proxy's side.
        region->updateCommitIndex(index);
        // We record them, as persisted raft log, for potential recovery.
        std::vector<std::string> new_keys;
        for (size_t i = 0; i < cmd_types.size(); i++)
        {
            if (cmd_types[i] == WriteCmdType::Put)
            {
                auto cf_name = CFToName(cmd_cf[i]);
                new_keys.emplace_back(RecordKVFormat::genKey(table_id, keys[i], 1));
            }
            else
            {
                auto cf_name = CFToName(cmd_cf[i]);
                new_keys.emplace_back(RecordKVFormat::genKey(table_id, keys[i], 1));
            }
        }
        region->commands[index] = {
            term,
            MockProxyRegion::RawWrite{
                new_keys,
                vals,
                cmd_types,
                cmd_cf,
            }};
    }
    return std::make_tuple(index, term);
}

std::tuple<uint64_t, uint64_t> MockRaftStoreProxy::rawWrite(
    UInt64 region_id,
    std::vector<std::string> && keys,
    std::vector<std::string> && vals,
    std::vector<WriteCmdType> && cmd_types,
    std::vector<ColumnFamilyType> && cmd_cf,
    std::optional<uint64_t> forced_index)
{
    uint64_t index = 0;
    uint64_t term = 0;
    {
        auto region = getRegion(region_id);
        assert(region != nullptr);
        // We have a new entry.
        index = forced_index.value_or(region->getLatestCommitIndex() + 1);
        RUNTIME_CHECK(index > region->getLatestCommitIndex());
        term = region->getLatestCommitTerm();
        // The new entry is committed on Proxy's side.
        region->updateCommitIndex(index);
        // We record them, as persisted raft log, for potential recovery.
        region->commands[index] = {
            term,
            MockProxyRegion::RawWrite{
                keys,
                vals,
                cmd_types,
                cmd_cf,
            }};
    }
    return std::make_tuple(index, term);
}


std::tuple<uint64_t, uint64_t> MockRaftStoreProxy::adminCommand(
    UInt64 region_id,
    raft_cmdpb::AdminRequest && request,
    raft_cmdpb::AdminResponse && response,
    std::optional<uint64_t> forced_index)
{
    uint64_t index = 0;
    uint64_t term = 0;
    {
        auto region = getRegion(region_id);
        assert(region != nullptr);
        // We have a new entry.
        index = forced_index.value_or(region->getLatestCommitIndex() + 1);
        RUNTIME_CHECK(index > region->getLatestCommitIndex());
        term = region->getLatestCommitTerm();
        // The new entry is committed on Proxy's side.
        region->updateCommitIndex(index);
        // We record them, as persisted raft log, for potential recovery.
        region->commands[index] = {
            term,
            MockProxyRegion::AdminCommand{
                request,
                response,
            }};
    }
    return std::make_tuple(index, term);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeCompactLog(MockProxyRegionPtr region, UInt64 compact_index)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::CompactLog);
    request.mutable_compact_log()->set_compact_index(compact_index);
    // Find compact term, otherwise log must have been compacted.
    if (region->commands.contains(compact_index))
    {
        request.mutable_compact_log()->set_compact_term(region->commands[compact_index].term);
    }
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeChangePeer(metapb::Region && meta, std::vector<UInt64> peer_ids, bool is_v2)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    if (is_v2)
    {
        request.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeerV2);
    }
    else
    {
        request.set_cmd_type(raft_cmdpb::AdminCmdType::ChangePeer);
    }
    meta.mutable_peers()->Clear();
    for (auto i : peer_ids)
    {
        meta.add_peers()->set_id(i);
    }
    *response.mutable_change_peer()->mutable_region() = meta;
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composePrepareMerge(metapb::Region && target, UInt64 min_index)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);
    auto * prepare_merge = request.mutable_prepare_merge();
    prepare_merge->set_min_index(min_index);
    *prepare_merge->mutable_target() = target;
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeCommitMerge(metapb::Region && source, UInt64 commit)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::CommitMerge);
    auto * commit_merge = request.mutable_commit_merge();
    commit_merge->set_commit(commit);
    *commit_merge->mutable_source() = source;
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeRollbackMerge(UInt64 commit)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::RollbackMerge);
    auto * rollback_merge = request.mutable_rollback_merge();
    rollback_merge->set_commit(commit);
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeBatchSplit(std::vector<UInt64> && region_ids, std::vector<std::pair<std::string, std::string>> && ranges, metapb::RegionEpoch old_epoch)
{
    RUNTIME_CHECK_MSG(region_ids.size() == ranges.size(), "error composeBatchSplit input");
    auto n = region_ids.size();
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::BatchSplit);
    metapb::RegionEpoch new_epoch;
    new_epoch.set_version(old_epoch.version() + 1);
    new_epoch.set_conf_ver(old_epoch.conf_ver());
    {
        raft_cmdpb::BatchSplitResponse * splits = response.mutable_splits();
        for (size_t i = 0; i < n; ++i)
        {
            auto * region = splits->add_regions();
            region->set_id(region_ids[i]);
            region->set_start_key(ranges[i].first);
            region->set_end_key(ranges[i].second);
            region->add_peers();
            *region->mutable_region_epoch() = new_epoch;
        }
    }
    return std::make_tuple(request, response);
}

void MockRaftStoreProxy::doApply(
    KVStore & kvs,
    TMTContext & tmt,
    const FailCond & cond,
    UInt64 region_id,
    uint64_t index)
{
    auto region = getRegion(region_id);
    assert(region != nullptr);
    // We apply this committed entry.
    raft_cmdpb::RaftCmdRequest request;
    auto & cmd = region->commands[index];
    auto term = cmd.term;
    if (cmd.has_raw_write_request())
    {
        auto & c = cmd.raw_write();
        auto & keys = c.keys;
        auto & vals = c.vals;
        auto & cmd_types = c.cmd_types;
        auto & cmd_cf = c.cmd_cf;
        size_t n = keys.size();

        assert(n == vals.size());
        assert(n == cmd_types.size());
        assert(n == cmd_cf.size());
        for (size_t i = 0; i < n; i++)
        {
            if (cmd_types[i] == WriteCmdType::Put)
            {
                auto cf_name = CFToName(cmd_cf[i]);
                auto key = TiKVKey(keys[i].data(), keys[i].size());
                TiKVValue value = std::move(vals[i]);
                RegionBench::setupPutRequest(request.add_requests(), cf_name, key, value);
            }
            else
            {
                auto cf_name = CFToName(cmd_cf[i]);
                auto key = TiKVKey(keys[i].data(), keys[i].size());
                RegionBench::setupDelRequest(request.add_requests(), cf_name, key);
            }
        }
    }
    else if (cmd.has_admin_request())
    {
    }

    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_WRITE)
        return;

    auto old_applied = kvs.getRegion(region_id)->appliedIndex();
    auto old_applied_term = kvs.getRegion(region_id)->appliedIndexTerm();
    if (cmd.has_raw_write_request())
    {
        // TiFlash write
        kvs.handleWriteRaftCmd(std::move(request), region_id, index, term, tmt);
    }
    if (cmd.has_admin_request())
    {
        kvs.handleAdminRaftCmd(std::move(cmd.admin().request), std::move(cmd.admin().response), region_id, index, term, tmt);
    }

    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_ADVANCE)
    {
        kvs.getRegion(region_id)->setApplied(old_applied, old_applied_term);
        return;
    }

    if (cmd.has_admin_request())
    {
        if (cmd.admin().cmd_type() == raft_cmdpb::AdminCmdType::CompactLog)
        {
            auto i = cmd.admin().request.compact_log().compact_index();
            // TODO We should remove (0, index] here, it is enough to remove exactly index now.
            region->commands.erase(i);
        }
    }

    // Proxy advance
    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_PROXY_ADVANCE)
        return;
    region->updateAppliedIndex(index);
}

void MockRaftStoreProxy::replay(
    KVStore & kvs,
    TMTContext & tmt,
    uint64_t region_id,
    uint64_t to)
{
    auto region = getRegion(region_id);
    assert(region != nullptr);
    FailCond cond;
    for (uint64_t i = region->apply.applied_index() + 1; i <= to; i++)
    {
        doApply(kvs, tmt, cond, region_id, i);
    }
}

void MockRaftStoreProxy::Cf::finish_file(SSTFormatKind kind)
{
    if (freezed)
        return;
    auto region_id_str = std::to_string(region_id) + "_multi_" + std::to_string(c);
    region_id_str = MockRaftStoreProxy::encodeSSTView(kind, region_id_str);
    c++;
    sst_files.push_back(region_id_str);
    MockSSTReader::Data kv_list;
    for (auto & kv : kvs)
    {
        kv_list.emplace_back(kv.first, kv.second);
    }
    auto & mmp = MockSSTReader::getMockSSTData();
    mmp[MockSSTReader::Key{region_id_str, type}] = std::move(kv_list);
    kvs.clear();
}

std::vector<SSTView> MockRaftStoreProxy::Cf::ssts() const
{
    assert(freezed);
    std::vector<SSTView> sst_views;
    for (const auto & sst_file : sst_files)
    {
        sst_views.push_back(SSTView{
            type,
            BaseBuffView{sst_file.c_str(), sst_file.size()},
        });
    }
    return sst_views;
}

MockRaftStoreProxy::Cf::Cf(UInt64 region_id_, TableID table_id_, ColumnFamilyType type_)
    : region_id(region_id_)
    , table_id(table_id_)
    , type(type_)
    , c(0)
    , freezed(false)
{
    auto & mmp = MockSSTReader::getMockSSTData();
    auto region_id_str = std::to_string(region_id) + "_multi_" + std::to_string(c);
    mmp[MockSSTReader::Key{region_id_str, type}].clear();
}

void MockRaftStoreProxy::Cf::insert(HandleID key, std::string val)
{
    auto k = RecordKVFormat::genKey(table_id, key, 1);
    TiKVValue v = std::move(val);
    kvs.emplace_back(k, v);
}

void MockRaftStoreProxy::Cf::insert_raw(std::string key, std::string val)
{
    kvs.emplace_back(std::move(key), std::move(val));
}

RegionPtr MockRaftStoreProxy::snapshot(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id,
    std::vector<Cf> && cfs,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index)
{
    auto region = getRegion(region_id);
    auto old_kv_region = kvs.getRegion(region_id);
    // We have catch up to index by snapshot.
    // So we assume there are new data updated, so we inc index by 1.
    if (index == 0)
    {
        index = region->getLatestCommitIndex() + 1;
        term = region->getLatestCommitTerm();
    }

    auto new_kv_region = kvs.genRegionPtr(old_kv_region->cloneMetaRegion(), old_kv_region->mutMeta().peerId(), index, term);
    // The new entry is committed on Proxy's side.
    region->updateCommitIndex(index);

    std::vector<SSTView> ssts;
    for (auto & cf : cfs)
    {
        auto sst = cf.ssts();
        for (auto & it : sst)
        {
            ssts.push_back(it);
        }
    }
    SSTViewVec snaps{ssts.data(), ssts.size()};
    auto ingest_ids = kvs.preHandleSnapshotToFiles(
        new_kv_region,
        snaps,
        index,
        term,
        deadline_index,
        tmt);

    kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(RegionPtrWithSnapshotFiles{new_kv_region, std::move(ingest_ids)}, tmt);
    region->updateAppliedIndex(index);
    // PreHandledSnapshotWithFiles will do that, however preHandleSnapshotToFiles will not.
    new_kv_region->setApplied(index, term);

    // Region changes during applying snapshot, must re-get.
    return kvs.getRegion(region_id);
}

TableID MockRaftStoreProxy::bootstrapTable(
    Context & ctx,
    KVStore & kvs,
    TMTContext & tmt,
    bool drop_at_first)
{
    UNUSED(kvs);
    ColumnsDescription columns;
    auto & data_type_factory = DataTypeFactory::instance();
    columns.ordinary = NamesAndTypesList({NameAndTypePair{"a", data_type_factory.get("Int64")}});
    auto tso = tmt.getPDClient()->getTS();
    if (drop_at_first)
    {
        MockTiDB::instance().dropDB(ctx, "d", true);
    }
    MockTiDB::instance().newDataBase("d");
    // Make sure there is a table with smaller id.
    MockTiDB::instance().newTable("d", "prevt" + toString(random()), columns, tso, "", "dt");
    UInt64 table_id = MockTiDB::instance().newTable("d", "t" + toString(random()), columns, tso, "", "dt");

    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchemas(ctx, NullspaceID);
    this->table_id = table_id;
    return table_id;
}

std::pair<std::string, std::string> MockRaftStoreProxy::generateTiKVKeyValue(uint64_t tso, int64_t t) const
{
    WriteBufferFromOwnString buff;
    writeChar(RecordKVFormat::CFModifyFlag::PutFlag, buff);
    EncodeVarUInt(tso, buff);
    std::string value_write = buff.releaseStr();
    buff.restart();
    auto && table_info = MockTiDB::instance().getTableInfoByID(table_id);
    std::vector<Field> f{Field{std::move(t)}};
    encodeRowV1(*table_info, f, buff);
    std::string value_default = buff.releaseStr();
    return std::make_pair(value_write, value_default);
}

void GCMonitor::add(RawObjType type, int64_t diff)
{
    auto _ = genLockGuard();
    data[type] += diff;
}

bool GCMonitor::checkClean()
{
    auto _ = genLockGuard();
    for (auto && d : data)
    {
        if (d.second)
            return false;
    }
    return true;
}

bool GCMonitor::empty()
{
    auto _ = genLockGuard();
    return data.empty();
}

} // namespace DB
