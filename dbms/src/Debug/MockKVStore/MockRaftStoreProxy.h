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

#pragma once

#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <kvproto/raft_serverpb.pb.h>
#include <raft_cmdpb.pb.h>

#include <ext/singleton.h>

namespace DB
{
kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts);

struct MockProxyRegion : MutexLockWrap
{
    raft_serverpb::RegionLocalState getState();
    raft_serverpb::RegionLocalState & mutState();
    raft_serverpb::RaftApplyState getApply();
    void persistAppliedIndex();
    void persistAppliedIndex(const std::lock_guard<Mutex> & lock);
    void updateAppliedIndex(uint64_t index, bool persist_at_once);
    uint64_t getPersistedAppliedIndex();
    uint64_t getLatestAppliedIndex();
    uint64_t getLatestCommitTerm();
    uint64_t getLatestCommitIndex();
    void updateCommitIndex(uint64_t index);
    void tryUpdateTruncatedState(uint64_t index, uint64_t term);
    void setState(raft_serverpb::RegionLocalState);
    explicit MockProxyRegion(uint64_t id);
    UniversalWriteBatch persistMeta();
    void addPeer(uint64_t store_id, uint64_t peer_id, metapb::PeerRole role);
    void reload();

    struct RawWrite
    {
        std::vector<std::string> keys;
        std::vector<std::string> vals;
        std::vector<WriteCmdType> cmd_types;
        std::vector<ColumnFamilyType> cmd_cf;
    };
    struct AdminCommand
    {
        raft_cmdpb::AdminRequest request;
        raft_cmdpb::AdminResponse response;
        raft_cmdpb::AdminCmdType cmd_type() const { return request.cmd_type(); }
    };

    struct CachedCommand
    {
        uint64_t term;
        std::variant<AdminCommand, RawWrite> inner;

        bool has_admin_request() const { return std::holds_alternative<AdminCommand>(inner); }

        bool has_raw_write_request() const { return std::holds_alternative<RawWrite>(inner); }

        AdminCommand & admin() { return std::get<AdminCommand>(inner); }

        RawWrite & raw_write() { return std::get<RawWrite>(inner); }
    };

    const uint64_t id;
    raft_serverpb::RegionLocalState state;
    raft_serverpb::RaftApplyState apply;
    raft_serverpb::RaftApplyState persisted_apply;
    std::map<uint64_t, CachedCommand> commands;
};

using MockProxyRegionPtr = std::shared_ptr<MockProxyRegion>;

struct MockAsyncNotifier
{
    RawCppPtr data; // notifier
    void (*wake_fn)(RawVoidPtr);
    void wake() const { wake_fn(data.ptr); }
    ~MockAsyncNotifier() { GcRawCppPtr(data.ptr, data.type); }
};

struct MockAsyncWaker
{
    std::shared_ptr<MockAsyncNotifier> data;
};

struct RawMockReadIndexTask : MutexLockWrap
{
    std::optional<kvrpcpb::ReadIndexResponse> poll(std::shared_ptr<MockAsyncNotifier> waker);

    void update(bool lock = false, bool region_error = false);

    kvrpcpb::ReadIndexRequest req;
    kvrpcpb::ReadIndexResponse resp;

    MockProxyRegionPtr region;
    std::shared_ptr<MockAsyncNotifier> waker;
    bool has_lock{false};
    bool has_region_error{false};
    bool finished{false};
};

struct MockReadIndexTask
{
    std::shared_ptr<RawMockReadIndexTask> data;
};

struct MockReadIndex
{
    explicit MockReadIndex(LoggerPtr log_)
        : log(log_)
    {}
    void wakeNotifier();
    /// Handle one read index task.
    void runOneRound();

    LoggerPtr log;
    // Mock Proxy will drop read index requests to these regions
    std::unordered_set<uint64_t> region_id_to_drop;
    // Mock Proxy will return error read index response to these regions
    std::unordered_set<uint64_t> region_id_to_error;
    std::list<std::shared_ptr<RawMockReadIndexTask>> read_index_tasks;
    AsyncWaker::Notifier notifier;
};

struct MockRaftStoreProxy : MutexLockWrap
{
    /// Mutation funcs.
    struct FailCond
    {
        enum Type
        {
            NORMAL,
            BEFORE_KVSTORE_WRITE,
            BEFORE_KVSTORE_ADVANCE,
            BEFORE_PROXY_PERSIST_ADVANCE,
        };
        Type type = NORMAL;
    };

    std::unique_ptr<TiFlashRaftProxyHelper> generateProxyHelper();
    static TiFlashRaftProxyHelper setRaftStoreProxyFFIHelper(RaftStoreProxyPtr);

    MockProxyRegionPtr getRegion(uint64_t id);
    MockProxyRegionPtr doGetRegion(uint64_t id);

    void init(size_t region_num);
    size_t size() const;

    MockReadIndexTask * makeReadIndexTask(kvrpcpb::ReadIndexRequest req);
    void testRunReadIndex(const std::atomic_bool & over);

    void unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb);

    /// Boostrap with a given region.
    /// Similar to TiKV's `bootstrap_region`.
    void bootstrapWithRegion(
        KVStore & kvs,
        TMTContext & tmt,
        RegionID region_id,
        std::optional<std::pair<std::string, std::string>> maybe_range);

    /// Boostrap a table.
    /// Must be called if:
    /// 1. Applying snapshot which needs table schema
    /// 2. Doing row2col.
    TableID bootstrapTable(Context & ctx, KVStore & kvs, TMTContext & tmt, bool drop_at_first = true);

    /// Manually add a region.
    void debugAddRegions(
        KVStore & kvs,
        TMTContext & tmt,
        std::vector<UInt64> region_ids,
        std::vector<std::pair<std::string, std::string>> && ranges);

    /// Use region meta info in KVStore to replace what's in MockProxy.
    /// Do not use in normal logic.
    void loadRegionFromKVStore(KVStore & kvs, TMTContext & tmt, UInt64 region_id);

    /// We assume that we generate one command, and immediately commit.
    /// Normal write to a region.
    std::tuple<uint64_t, uint64_t> normalWrite(
        UInt64 region_id,
        std::vector<HandleID> && keys,
        std::vector<std::string> && vals,
        std::vector<WriteCmdType> && cmd_types,
        std::vector<ColumnFamilyType> && cmd_cf);

    std::tuple<uint64_t, uint64_t> rawWrite(
        UInt64 region_id,
        std::vector<std::string> && keys,
        std::vector<std::string> && vals,
        std::vector<WriteCmdType> && cmd_types,
        std::vector<ColumnFamilyType> && cmd_cf,
        std::optional<uint64_t> forced_index = std::nullopt);

    std::tuple<uint64_t, uint64_t> adminCommand(
        UInt64 region_id,
        raft_cmdpb::AdminRequest &&,
        raft_cmdpb::AdminResponse &&,
        std::optional<uint64_t> forced_index = std::nullopt);

    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composeCompactLog(
        MockProxyRegionPtr region,
        UInt64 compact_index);
    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composeChangePeer(
        metapb::Region && meta,
        std::vector<UInt64> peer_ids,
        bool is_v2 = true);
    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composePrepareMerge(
        metapb::Region && target,
        UInt64 min_index);
    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composeCommitMerge(
        metapb::Region && source,
        UInt64 commit);
    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composeRollbackMerge(UInt64 commit);
    static std::tuple<raft_cmdpb::AdminRequest, raft_cmdpb::AdminResponse> composeBatchSplit(
        std::vector<UInt64> && region_ids,
        std::vector<std::pair<std::string, std::string>> && ranges,
        metapb::RegionEpoch old_epoch);

    std::tuple<RegionPtr, PrehandleResult> snapshot(
        KVStore & kvs,
        TMTContext & tmt,
        UInt64 region_id,
        std::vector<MockSSTGenerator> && cfs,
        metapb::Region && region_meta,
        UInt64 peer_id,
        uint64_t index,
        uint64_t term,
        std::optional<uint64_t> deadline_index,
        bool cancel_after_prehandle);
    std::tuple<RegionPtr, PrehandleResult> snapshot(
        KVStore & kvs,
        TMTContext & tmt,
        UInt64 region_id,
        std::vector<MockSSTGenerator> && cfs,
        uint64_t index,
        uint64_t term,
        std::optional<uint64_t> deadline_index,
        bool cancel_after_prehandle = false);

    void doApply(
        KVStore & kvs,
        TMTContext & tmt,
        const FailCond & cond,
        UInt64 region_id,
        uint64_t index,
        std::optional<bool> check_proactive_flush = std::nullopt);

    void reload();
    void replay(KVStore & kvs, TMTContext & tmt, uint64_t region_id, uint64_t to);

    void clear();

    std::pair<std::string, std::string> generateTiKVKeyValue(uint64_t tso, int64_t t) const;

    MockRaftStoreProxy()
        : log(Logger::get("MockRaftStoreProxy"))
        , mock_read_index(MockReadIndex(log))
        , table_id(1)
        , cluster_ver(RaftstoreVer::V1)
    {
        proxy_config_string = R"({"raftstore":{"snap-handle-pool-size":4}})";
    }

    LoggerPtr log;
    MockReadIndex mock_read_index;
    TableID table_id;
    RaftstoreVer cluster_ver;
    std::string proxy_config_string;
    std::map<uint64_t, MockProxyRegionPtr> regions;
};

inline MockRaftStoreProxy & as_ref(RaftStoreProxyPtr ptr)
{
    return *reinterpret_cast<MockRaftStoreProxy *>(reinterpret_cast<size_t>(ptr.inner));
}

enum class RawObjType : uint32_t
{
    None,
    MockReadIndexTask,
    MockAsyncWaker,
    MockString,
    MockVecOfString
};

struct GCMonitor
    : MutexLockWrap
    , public ext::Singleton<GCMonitor>
{
    void add(RawObjType type, int64_t diff);

    bool checkClean();

    bool empty();

    std::unordered_map<RawObjType, int64_t> data;

    static GCMonitor global_gc_monitor;
};

template <typename... Types>
std::vector<std::pair<std::string, std::string>> regionRangeToEncodeKeys(Types &&... args)
{
    // RegionRangeKeys::RegionRange is not copy-constructible, however, initialize_list need copy construction.
    // So we have to so this way, rather than create a composeXXX that accepts a vector of RegionRangeKeys::RegionRange.
    std::vector<std::pair<std::string, std::string>> ranges_str;
    (
        [&] {
            auto & x = args;
            ranges_str.emplace_back(std::make_pair(x.first.toString(), x.second.toString()));
        }(),
        ...);
    return ranges_str;
}

struct RustStrWithViewVecInner
{
    std::vector<std::string> * vec;
    BaseBuffView * buffs;
    ~RustStrWithViewVecInner()
    {
        delete vec;
        delete[] buffs;
    }
};

inline BaseBuffView * createBaseBuffViewArray(size_t len)
{
    void * raw_memory = operator new[](len * sizeof(BaseBuffView));
    auto * ptr = static_cast<BaseBuffView *>(raw_memory);
    return ptr;
}
} // namespace DB
