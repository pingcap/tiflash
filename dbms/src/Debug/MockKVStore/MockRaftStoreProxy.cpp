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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <Debug/MockKVStore/MockFFIImpls.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockKVStore/MockSSTReader.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <google/protobuf/text_format.h>

namespace DB
{
namespace RegionBench
{
extern void setupPutRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &, const TiKVValue &);
extern void setupDelRequest(raft_cmdpb::Request *, const std::string &, const TiKVKey &);
} // namespace RegionBench

TiFlashRaftProxyHelper MockRaftStoreProxy::setRaftStoreProxyFFIHelper(RaftStoreProxyPtr proxy_ptr)
{
    TiFlashRaftProxyHelper res{};
    res.proxy_ptr = proxy_ptr;
    res.fn_make_read_index_task = MockFFIImpls::fn_make_read_index_task;
    res.fn_poll_read_index_task = MockFFIImpls::fn_poll_read_index_task;
    res.fn_make_async_waker = MockFFIImpls::fn_make_async_waker;
    res.fn_handle_batch_read_index = MockFFIImpls::fn_handle_batch_read_index;
    res.fn_get_region_local_state = MockFFIImpls::fn_get_region_local_state;
    res.fn_notify_compact_log = MockFFIImpls::fn_notify_compact_log;
    res.fn_get_cluster_raftstore_version = MockFFIImpls::fn_get_cluster_raftstore_version;
    res.fn_get_config_json = MockFFIImpls::fn_get_config_json;
    {
        // make sure such function pointer will be set at most once.
        static std::once_flag flag;
        std::call_once(flag, []() { MockSetFFI::MockSetRustGcHelper(MockFFIImpls::fn_gc_rust_ptr); });
    }

    return res;
}

MockProxyRegionPtr MockRaftStoreProxy::getRegion(uint64_t id) NO_THREAD_SAFETY_ANALYSIS
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

MockReadIndexTask * MockRaftStoreProxy::makeReadIndexTask(kvrpcpb::ReadIndexRequest req) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();

    mock_read_index.wakeNotifier();

    auto region = doGetRegion(req.context().region_id());
    if (region)
    {
        auto * r = new MockReadIndexTask{};
        r->data = std::make_shared<RawMockReadIndexTask>();
        r->data->req = std::move(req);
        r->data->region = region;
        mock_read_index.read_index_tasks.push_back(r->data);
        return r;
    }
    return nullptr;
}

void MockRaftStoreProxy::init(size_t region_num) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    for (size_t i = 0; i < region_num; ++i)
    {
        regions.emplace(i, std::make_shared<MockProxyRegion>(i));
    }
}

std::unique_ptr<TiFlashRaftProxyHelper> MockRaftStoreProxy::generateProxyHelper()
{
    auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(
        MockRaftStoreProxy::setRaftStoreProxyFFIHelper(RaftStoreProxyPtr{this}));
    // Bind ffi to MockSSTReader.
    proxy_helper->sst_reader_interfaces = make_mock_sst_reader_interface();
    return proxy_helper;
}

size_t MockRaftStoreProxy::size() const NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return regions.size();
}

void MockRaftStoreProxy::testRunReadIndex(const std::atomic_bool & over) NO_THREAD_SAFETY_ANALYSIS
{
    while (!over)
    {
        {
            auto _ = genLockGuard();
            mock_read_index.runOneRound();
        }
        mock_read_index.notifier.blockedWaitFor(std::chrono::seconds(1));
    }
}

void MockRaftStoreProxy::unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    cb(*this);
}

void MockRaftStoreProxy::bootstrapWithRegion(
    KVStore & kvs,
    TMTContext & tmt,
    RegionID region_id,
    std::optional<std::pair<std::string, std::string>> maybe_range) NO_THREAD_SAFETY_ANALYSIS
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
    std::vector<std::pair<std::string, std::string>> && ranges) NO_THREAD_SAFETY_ANALYSIS
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
        tmt.getRegionTable().updateRegion(*region);
    }
}

void MockRaftStoreProxy::loadRegionFromKVStore(KVStore & kvs, TMTContext & tmt, UInt64 region_id)
{
    UNUSED(tmt);
    auto kvr = kvs.getRegion(region_id);
    auto ori_r = getRegion(region_id);
    auto commit_index = RAFT_INIT_LOG_INDEX;
    auto commit_term = RAFT_INIT_LOG_TERM;
    if (!ori_r)
    {
        regions.emplace(region_id, std::make_shared<MockProxyRegion>(region_id));
    }
    else
    {
        commit_index = ori_r->getLatestCommitIndex();
        commit_term = ori_r->getLatestCommitTerm();
    }
    MockProxyRegionPtr r = getRegion(region_id);
    {
        r->state = kvr->getMeta().getRegionState().getBase();
        r->apply = kvr->getMeta().clonedApplyState();
        if (r->apply.commit_index() == 0)
        {
            r->apply.set_commit_index(commit_index);
            r->apply.set_commit_term(commit_term);
        }
    }
    LOG_INFO(
        log,
        "loadRegionFromKVStore [region_id={}] region_state {} apply_state {}",
        region_id,
        r->state.DebugString(),
        r->apply.DebugString());
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
        region->commands[index]
            = {term,
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
            },
        };
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
        region->commands[index]
            = {term,
               MockProxyRegion::AdminCommand{
                   request,
                   response,
               }};
    }
    return std::make_tuple(index, term);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeCompactLog(
    MockProxyRegionPtr region,
    UInt64 compact_index)
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

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeChangePeer(
    metapb::Region && meta,
    std::vector<UInt64> peer_ids,
    bool is_v2)
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

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composePrepareMerge(
    metapb::Region && target,
    UInt64 min_index)
{
    raft_cmdpb::AdminRequest request;
    raft_cmdpb::AdminResponse response;
    request.set_cmd_type(raft_cmdpb::AdminCmdType::PrepareMerge);
    auto * prepare_merge = request.mutable_prepare_merge();
    prepare_merge->set_min_index(min_index);
    *prepare_merge->mutable_target() = target;
    return std::make_tuple(request, response);
}

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeCommitMerge(
    metapb::Region && source,
    UInt64 commit)
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

std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> MockRaftStoreProxy::composeBatchSplit(
    std::vector<UInt64> && region_ids,
    std::vector<std::pair<std::string, std::string>> && ranges,
    metapb::RegionEpoch old_epoch)
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
    uint64_t index,
    std::optional<bool> check_proactive_flush)
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
    else if (cmd.has_admin_request()) {}

    region->updateAppliedIndex(index, false);

    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_WRITE)
        return;

    auto old_applied = kvs.getRegion(region_id)->appliedIndex();
    auto old_applied_term = kvs.getRegion(region_id)->appliedIndexTerm();
    if (cmd.has_raw_write_request())
    {
        // TiFlash write
        DB::DM::WriteResult write_task;
        RegionBench::applyWriteRaftCmd(kvs, std::move(request), region_id, index, term, tmt, &write_task);
        if (check_proactive_flush)
        {
            if (check_proactive_flush.value())
            {
                // fg flush
                ASSERT(write_task.has_value());
            }
            else
            {
                // bg flush
                ASSERT(!write_task.has_value());
            }
        }
    }
    if (cmd.has_admin_request())
    {
        if (cmd.admin().cmd_type() == raft_cmdpb::AdminCmdType::CompactLog)
        {
            auto res = kvs.tryFlushRegionData(
                region_id,
                false,
                true,
                tmt,
                index,
                term,
                region->getApply().truncated_state().index(),
                region->getApply().truncated_state().term());
            auto compact_index = cmd.admin().request.compact_log().compact_index();
            auto compact_term = cmd.admin().request.compact_log().compact_term();
            if (!res)
            {
                LOG_DEBUG(log, "mock pre exec reject");
            }
            else
            {
                region->tryUpdateTruncatedState(compact_index, compact_term);
                LOG_DEBUG(log, "mock pre exec success, update to {},{}", compact_index, compact_term);
            }
        }
        kvs.handleAdminRaftCmd(
            std::move(cmd.admin().request),
            std::move(cmd.admin().response),
            region_id,
            index,
            term,
            tmt);
    }

    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_KVSTORE_ADVANCE)
    {
        // We reset applied to old one.
        // TODO persistRegion to cowork with restore.
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
        else if (cmd.admin().cmd_type() == raft_cmdpb::AdminCmdType::BatchSplit)
        {
            for (auto && sp : cmd.admin().response.splits().regions())
            {
                auto r = sp.id();
                loadRegionFromKVStore(kvs, tmt, r);
            }
        }
    }

    // Proxy advance
    // In raftstore v1, applied_index in ApplyFsm is advanced before forward to TiFlash.
    // However, it is after TiFlash has persisted applied state that proxy's ApplyFsm will notify raft to advance.
    if (cond.type == MockRaftStoreProxy::FailCond::Type::BEFORE_PROXY_PERSIST_ADVANCE)
        return;

    region->persistAppliedIndex();
}

void MockRaftStoreProxy::reload()
{
    for (auto & iter : regions)
    {
        auto region = getRegion(iter.first);
        assert(region != nullptr);
        region->reload();
    }
}

void MockRaftStoreProxy::replay(KVStore & kvs, TMTContext & tmt, uint64_t region_id, uint64_t to)
{
    auto region = getRegion(region_id);
    assert(region != nullptr);
    FailCond cond;
    for (uint64_t i = region->apply.applied_index() + 1; i <= to; i++)
    {
        doApply(kvs, tmt, cond, region_id, i);
    }
}

std::tuple<RegionPtr, PrehandleResult> MockRaftStoreProxy::snapshot(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id,
    std::vector<MockSSTGenerator> && cfs,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index,
    bool cancel_after_prehandle)
{
    auto old_kv_region = kvs.getRegion(region_id);
    RUNTIME_CHECK(old_kv_region != nullptr);
    return snapshot(
        kvs,
        tmt,
        region_id,
        std::move(cfs),
        old_kv_region->cloneMetaRegion(),
        old_kv_region->getMeta().peerId(),
        index,
        term,
        deadline_index,
        cancel_after_prehandle);
}

std::tuple<RegionPtr, PrehandleResult> MockRaftStoreProxy::snapshot(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id,
    std::vector<MockSSTGenerator> && cfs,
    metapb::Region && region_meta,
    UInt64 peer_id,
    uint64_t index,
    uint64_t term,
    std::optional<uint64_t> deadline_index,
    bool cancel_after_prehandle)
{
    auto region = getRegion(region_id);
    RUNTIME_CHECK(region != nullptr);
    // We have catch up to index by snapshot.
    // So we assume there are new data updated, so we inc index by 1.
    if (index == 0)
    {
        index = region->getLatestCommitIndex() + 1;
        term = region->getLatestCommitTerm();
    }

    auto new_kv_region = kvs.genRegionPtr(std::move(region_meta), peer_id, index, term);
    // The new entry is committed on Proxy's side.
    region->updateCommitIndex(index);
    new_kv_region->setApplied(index, term);

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
    try
    {
        auto prehandle_result = kvs.preHandleSnapshotToFiles(new_kv_region, snaps, index, term, deadline_index, tmt);
        auto rg = RegionPtrWithSnapshotFiles{new_kv_region, std::vector(prehandle_result.ingest_ids)};
        if (cancel_after_prehandle)
        {
            kvs.releasePreHandledSnapshot(rg, tmt);
            return std::make_tuple(kvs.getRegion(region_id), prehandle_result);
        }
        kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(rg, tmt);
        // Though it is persisted earlier in real proxy, but the state is changed to Normal here.
        region->updateAppliedIndex(index, true);
        // Region changes during applying snapshot, must re-get.
        return std::make_tuple(kvs.getRegion(region_id), prehandle_result);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "mock apply snapshot error {}", e.message());
        e.rethrow();
    }
    LOG_FATAL(DB::Logger::get(), "Should not happen");
    exit(-1);
}

TableID MockRaftStoreProxy::bootstrapTable(Context & ctx, KVStore & kvs, TMTContext & tmt, bool drop_at_first)
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

    auto schema_syncer = tmt.getSchemaSyncerManager();
    schema_syncer->syncSchemas(ctx, NullspaceID);
    this->table_id = table_id;
    return table_id;
}

void MockRaftStoreProxy::clear() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    regions.clear();
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

void GCMonitor::add(RawObjType type, int64_t diff) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    data[type] += diff;
}

bool GCMonitor::checkClean() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    for (auto && d : data)
    {
        if (d.second)
        {
            LOG_INFO(&Poco::Logger::get("GCMonitor"), "checkClean {} has {}", magic_enum::enum_name(d.first), d.second);
            return false;
        }
    }
    return true;
}

bool GCMonitor::empty() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return data.empty();
}

} // namespace DB
