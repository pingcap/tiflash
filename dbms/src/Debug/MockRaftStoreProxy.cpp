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
#include <Debug/MockRaftStoreProxy.h>
#include <Debug/MockSSTReader.h>
#include <Debug/MockTiDB.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/tests/region_helper.h>
#include <TestUtils/TiFlashTestEnv.h>

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

TiFlashRaftProxyHelper MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(RaftStoreProxyPtr proxy_ptr)
{
    TiFlashRaftProxyHelper res{};
    res.proxy_ptr = proxy_ptr;
    res.fn_make_read_index_task = fn_make_read_index_task;
    res.fn_poll_read_index_task = fn_poll_read_index_task;
    res.fn_make_async_waker = fn_make_async_waker;
    res.fn_handle_batch_read_index = fn_handle_batch_read_index;
    res.fn_get_region_local_state = fn_get_region_local_state;
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

    wake();

    auto region = doGetRegion(req.context().region_id());
    if (region)
    {
        auto * r = new MockReadIndexTask{};
        r->data = std::make_shared<RawMockReadIndexTask>();
        r->data->req = std::move(req);
        r->data->region = region;
        tasks.push_back(r->data);
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

size_t MockRaftStoreProxy::size() const
{
    auto _ = genLockGuard();
    return regions.size();
}

void MockRaftStoreProxy::wake()
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
    while (!tasks.empty())
    {
        auto & t = *tasks.front();
        if (!region_id_to_drop.count(t.req.context().region_id()))
        {
            if (region_id_to_error.count(t.req.context().region_id()))
                t.update(false, true);
            else
                t.update(false, false);
        }
        tasks.pop_front();
    }
}

void MockRaftStoreProxy::unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb)
{
    auto _ = genLockGuard();
    cb(*this);
}

void MockRaftStoreProxy::bootstrap(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id)
{
    UNUSED(tmt);
    auto _ = genLockGuard();
    regions.emplace(region_id, std::make_shared<MockProxyRegion>(region_id));

    auto task_lock = kvs.genTaskLock();
    auto lock = kvs.genRegionWriteLock(task_lock);
    {
        auto region = tests::makeRegion(region_id, RecordKVFormat::genKey(table_id, 0), RecordKVFormat::genKey(table_id, 10));
        lock.regions.emplace(region_id, region);
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
        region->commands[index] = {
            term,
            MockProxyRegion::NormalWrite{
                keys,
                vals,
                cmd_types,
                cmd_cf,
            }};
    }
    return std::make_tuple(index, term);
}

std::tuple<uint64_t, uint64_t> MockRaftStoreProxy::compactLog(UInt64 region_id, UInt64 compact_index)
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
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        request.mutable_compact_log();
        request.set_cmd_type(raft_cmdpb::AdminCmdType::CompactLog);
        request.mutable_compact_log()->set_compact_index(compact_index);
        // Find compact term, otherwise log must have been compacted.
        if (region->commands.count(compact_index))
        {
            request.mutable_compact_log()->set_compact_term(region->commands[index].term);
        }
        region->commands[index] = {
            term,
            MockProxyRegion::AdminCommand{
                request,
                response,
            }};
    }
    return std::make_tuple(index, term);
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
    if (cmd.has_write_request())
    {
        auto & c = cmd.write();
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
                auto key = RecordKVFormat::genKey(table_id, keys[i], 1);
                TiKVValue value = std::move(vals[i]);
                RegionBench::setupPutRequest(request.add_requests(), cf_name, key, value);
            }
            else
            {
                auto cf_name = CFToName(cmd_cf[i]);
                auto key = RecordKVFormat::genKey(table_id, keys[i], 1);
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
    if (cmd.has_write_request())
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

void MockRaftStoreProxy::Cf::finish_file()
{
    if (freezed)
        return;
    auto region_id_str = std::to_string(region_id) + "_multi_" + std::to_string(c);
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

void MockRaftStoreProxy::snapshot(
    KVStore & kvs,
    TMTContext & tmt,
    UInt64 region_id,
    std::vector<Cf> && cfs,
    uint64_t index,
    uint64_t term)
{
    auto region = getRegion(region_id);
    auto kv_region = kvs.getRegion(region_id);
    // We have catch up to index by snapshot.
    index = region->getLatestCommitIndex() + 1;
    term = region->getLatestCommitTerm();
    // The new entry is committed on Proxy's side.
    region->updateCommitIndex(index);

    auto ori_snapshot_apply_method = kvs.snapshot_apply_method;
    kvs.snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Single;
    SCOPE_EXIT({
        kvs.snapshot_apply_method = ori_snapshot_apply_method;
    });
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
        kv_region,
        snaps,
        index,
        term,
        tmt);

    kvs.checkAndApplyPreHandledSnapshot<RegionPtrWithSnapshotFiles>(RegionPtrWithSnapshotFiles{kv_region, std::move(ingest_ids)}, tmt);
    region->updateAppliedIndex(index);
    // PreHandledSnapshotWithFiles will do that, however preHandleSnapshotToFiles will not.
    kv_region->setApplied(index, term);
}

TableID MockRaftStoreProxy::bootstrap_table(
    Context & ctx,
    KVStore & kvs,
    TMTContext & tmt)
{
    UNUSED(kvs);
    ColumnsDescription columns;
    auto & data_type_factory = DataTypeFactory::instance();
    columns.ordinary = NamesAndTypesList({NameAndTypePair{"a", data_type_factory.get("Int64")}});
    auto tso = tmt.getPDClient()->getTS();
    MockTiDB::instance().newDataBase("d");
    UInt64 table_id = MockTiDB::instance().newTable("d", "t", columns, tso, "", "dt");

    auto schema_syncer = tmt.getSchemaSyncer();
    schema_syncer->syncSchemas(ctx);
    this->table_id = table_id;
    return table_id;
}

void MockRaftStoreProxy::clear_tables(
    Context & ctx,
    KVStore & kvs,
    TMTContext & tmt)
{
    UNUSED(kvs);
    UNUSED(tmt);
    if (this->table_id != 1)
    {
        MockTiDB::instance().dropTable(ctx, "d", "t", false);
    }
    this->table_id = 1;
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
