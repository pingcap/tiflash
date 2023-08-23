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

#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/StorageEngineType.h>

#include <magic_enum.hpp>

namespace TiDB
{
struct TableInfo;
}
namespace DB
{
class Context;
namespace RegionBench
{
extern void concurrentBatchInsert(const TiDB::TableInfo &, Int64, Int64, Int64, UInt64, UInt64, Context &);
} // namespace RegionBench
namespace DM
{
enum class FileConvertJobType;
struct ExternalDTFileInfo;
} // namespace DM

namespace tests
{
class RegionKVStoreTest;
}

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class RegionTable;
class Region;
using RegionPtr = std::shared_ptr<Region>;
struct RaftCommandResult;
class KVStoreTaskLock;

struct MockRaftCommand;
struct MockTiDBTable;
struct TiKVRangeKey;

class TMTContext;

struct SSTViewVec;
struct WriteCmdsView;

enum class EngineStoreApplyRes : uint32_t;

struct TiFlashRaftProxyHelper;
struct RegionPreDecodeBlockData;
using RegionPreDecodeBlockDataPtr = std::unique_ptr<RegionPreDecodeBlockData>;
class ReadIndexWorkerManager;
using BatchReadIndexRes = std::vector<std::pair<kvrpcpb::ReadIndexResponse, uint64_t>>;
class ReadIndexStressTest;
struct FileUsageStatistics;
class PathPool;
class RegionPersister;
struct CheckpointInfo;
using CheckpointInfoPtr = std::shared_ptr<CheckpointInfo>;

enum class PersistRegionReason
{
    Debug,
    UselessAdminCommand, // Does not include passive CompactLog
    AdminCommand,
    Flush, // passive CompactLog
    ProactiveFlush,
    ApplySnapshotPrevRegion,
    ApplySnapshotCurRegion,
    IngestSst
};

constexpr const char * PersistRegionReasonMap[magic_enum::enum_count<PersistRegionReason>()]
    = {"debug",
       "admin cmd useless",
       "admin raft cmd",
       "tryFlushRegionData",
       "ProactiveFlush",
       "save previous region before apply",
       "save current region after apply",
       "ingestsst"};

static_assert(magic_enum::enum_count<PersistRegionReason>() == sizeof(PersistRegionReasonMap) / sizeof(const char *));

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    explicit KVStore(Context & context);
    void restore(PathPool & path_pool, const TiFlashRaftProxyHelper *);

    RegionPtr getRegion(RegionID region_id) const;

    using RegionRange = RegionRangeKeys::RegionRange;

    RegionMap getRegionsByRangeOverlap(const RegionRange & range) const;

    void traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const;

    void gcPersistedRegion(Seconds gc_persist_period = Seconds(60 * 5));

    static bool tryFlushRegionCacheInStorage(
        TMTContext & tmt,
        const Region & region,
        const LoggerPtr & log,
        bool try_until_succeed = true);

    size_t regionSize() const;
    EngineStoreApplyRes handleAdminRaftCmd(
        raft_cmdpb::AdminRequest && request,
        raft_cmdpb::AdminResponse && response,
        UInt64 region_id,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt);
    EngineStoreApplyRes handleWriteRaftCmd(
        const WriteCmdsView & cmds,
        UInt64 region_id,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt);
    EngineStoreApplyRes handleWriteRaftCmdInner(
        const WriteCmdsView & cmds,
        UInt64 region_id,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt,
        DM::WriteResult & write_result);

    bool needFlushRegionData(UInt64 region_id, TMTContext & tmt);
    bool tryFlushRegionData(
        UInt64 region_id,
        bool force_persist,
        bool try_until_succeed,
        TMTContext & tmt,
        UInt64 index,
        UInt64 term,
        uint64_t truncated_index,
        uint64_t truncated_term);

    void handleIngestCheckpoint(RegionPtr region, CheckpointInfoPtr checkpoint_info, TMTContext & tmt);

    // For Raftstore V2, there could be some orphan keys in the write column family being left to `new_region` after pre-handled.
    // All orphan write keys are asserted to be replayed before reaching `deadline_index`.
    std::vector<DM::ExternalDTFileInfo> preHandleSnapshotToFiles(
        RegionPtr new_region,
        SSTViewVec,
        uint64_t index,
        uint64_t term,
        std::optional<uint64_t> deadline_index,
        TMTContext & tmt);
    template <typename RegionPtrWrap>
    void applyPreHandledSnapshot(const RegionPtrWrap &, TMTContext & tmt);
    template <typename RegionPtrWrap>
    void releasePreHandledSnapshot(const RegionPtrWrap &, TMTContext & tmt);
    void abortPreHandleSnapshot(uint64_t region_id, TMTContext & tmt);

    void handleDestroy(UInt64 region_id, TMTContext & tmt);
    void setRegionCompactLogConfig(UInt64, UInt64, UInt64, UInt64);
    EngineStoreApplyRes handleIngestSST(UInt64 region_id, SSTViewVec, UInt64 index, UInt64 term, TMTContext & tmt);
    RegionPtr genRegionPtr(metapb::Region && region, UInt64 peer_id, UInt64 index, UInt64 term);
    const TiFlashRaftProxyHelper * getProxyHelper() const { return proxy_helper; }
    // Exported only for tests.
    TiFlashRaftProxyHelper * mutProxyHelperUnsafe() { return const_cast<TiFlashRaftProxyHelper *>(proxy_helper); }

    void addReadIndexEvent(Int64 f) { read_index_event_flag += f; }
    Int64 getReadIndexEvent() const { return read_index_event_flag; }

    void setStore(metapb::Store);

    // May return 0 if uninitialized
    StoreID getStoreID(std::memory_order = std::memory_order_relaxed) const;

    metapb::Store getStoreMeta() const;

    BatchReadIndexRes batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> & req, uint64_t timeout_ms) const;

    /// Initialize read-index worker context. It only can be invoked once.
    /// `worker_coefficient` means `worker_coefficient * runner_cnt` workers will be created.
    /// `runner_cnt` means number of runner which controls behavior of worker.
    void initReadIndexWorkers(
        std::function<std::chrono::milliseconds()> && fn_min_dur_handle_region,
        size_t runner_cnt,
        size_t worker_coefficient = 64);

    /// Create `runner_cnt` threads to run ReadIndexWorker asynchronously and automatically.
    /// If there is other runtime framework, DO NOT invoke it.
    void asyncRunReadIndexWorkers();

    /// Stop workers after there is no more read-index task.
    void stopReadIndexWorkers();

    /// TODO: if supported by runtime framework, run one round for specific runner by `id`.
    void runOneRoundOfReadIndexRunner(size_t runner_id);

    ~KVStore();

    FileUsageStatistics getFileUsageStatistics() const;

    // TODO(proactive flush)
    // void proactiveFlushCacheAndRegion(TMTContext & tmt, const DM::RowKeyRange & rowkey_range, KeyspaceID keyspace_id, TableID table_id, bool is_background);
    void notifyCompactLog(
        RegionID region_id,
        UInt64 compact_index,
        UInt64 compact_term,
        bool is_background,
        bool lock_held = true);
#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    friend struct MockRaftStoreProxy;
    friend class MockTiDB;
    friend struct MockTiDBTable;
    friend struct MockRaftCommand;
    friend class RegionMockTest;
    friend class NaturalDag;
    friend void RegionBench::concurrentBatchInsert(
        const TiDB::TableInfo &,
        Int64,
        Int64,
        Int64,
        UInt64,
        UInt64,
        Context &);
    using DBGInvokerPrinter = std::function<void(const std::string &)>;
    friend void dbgFuncRemoveRegion(Context &, const ASTs &, DBGInvokerPrinter);
    friend void dbgFuncPutRegion(Context &, const ASTs &, DBGInvokerPrinter);
    friend class tests::RegionKVStoreTest;
    friend class ReadIndexStressTest;
    struct StoreMeta
    {
        mutable std::mutex mu;

        using Base = metapb::Store;
        Base base;
        std::atomic_uint64_t store_id{0};
        void update(Base &&);
        Base getMeta() const;
        friend class KVStore;
    };
    StoreMeta & getStore();
    const StoreMeta & getStore() const;

    std::vector<DM::ExternalDTFileInfo> preHandleSSTsToDTFiles(
        RegionPtr new_region,
        const SSTViewVec,
        uint64_t index,
        uint64_t term,
        DM::FileConvertJobType,
        TMTContext & tmt);

    template <typename RegionPtrWrap>
    void checkAndApplyPreHandledSnapshot(const RegionPtrWrap &, TMTContext & tmt);
    template <typename RegionPtrWrap>
    void onSnapshot(const RegionPtrWrap &, RegionPtr old_region, UInt64 old_region_index, TMTContext & tmt);

    RegionPtr handleIngestSSTByDTFile(
        const RegionPtr & region,
        const SSTViewVec,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt);

    // Remove region from this TiFlash node.
    // If region is destroy or moved to another node(change peer),
    // set `remove_data` true to remove obsolete data from storage.
    void removeRegion(
        RegionID region_id,
        bool remove_data,
        RegionTable & region_table,
        const KVStoreTaskLock & task_lock,
        const RegionTaskLock & region_lock);
    void mockRemoveRegion(RegionID region_id, RegionTable & region_table);
    KVStoreTaskLock genTaskLock() const;

    RegionManager::RegionReadLock genRegionMgrReadLock() const;

    RegionManager::RegionWriteLock genRegionMgrWriteLock(const KVStoreTaskLock &);

    EngineStoreApplyRes handleUselessAdminRaftCmd(
        raft_cmdpb::AdminCmdType cmd_type,
        UInt64 curr_region_id,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt) const;

    /// Notice that if flush_if_possible is set to false, we only check if a flush is allowed by rowsize/size/interval.
    /// It will not check if a flush will eventually succeed.
    /// In other words, `canFlushRegionDataImpl(flush_if_possible=true)` can return false.
    bool canFlushRegionDataImpl(
        const RegionPtr & curr_region_ptr,
        UInt8 flush_if_possible,
        bool try_until_succeed,
        TMTContext & tmt,
        const RegionTaskLock & region_task_lock,
        UInt64 index,
        UInt64 term,
        UInt64 truncated_index,
        UInt64 truncated_term);

    bool forceFlushRegionDataImpl(
        Region & curr_region,
        bool try_until_succeed,
        TMTContext & tmt,
        const RegionTaskLock & region_task_lock,
        UInt64 index,
        UInt64 term) const;

    void persistRegion(
        const Region & region,
        std::optional<const RegionTaskLock *> region_task_lock,
        PersistRegionReason reason,
        const char * extra_msg) const;

    void releaseReadIndexWorkers();
    void handleDestroy(UInt64 region_id, TMTContext & tmt, const KVStoreTaskLock &);

    struct PreHandlingTrace : MutexLockWrap
    {
        std::unordered_map<uint64_t, std::shared_ptr<std::atomic_bool>> tasks;

        std::shared_ptr<std::atomic_bool> registerTask(uint64_t region_id)
        {
            // Automaticlly override the old one.
            auto _ = genLockGuard();
            auto b = std::make_shared<std::atomic_bool>(false);
            tasks[region_id] = b;
            return b;
        }
        std::shared_ptr<std::atomic_bool> deregisterTask(uint64_t region_id)
        {
            auto _ = genLockGuard();
            auto it = tasks.find(region_id);
            if (it != tasks.end())
            {
                auto b = it->second;
                tasks.erase(it);
                return b;
            }
            else
            {
                return nullptr;
            }
        }
        bool hasTask(uint64_t region_id)
        {
            auto _ = genLockGuard();
            return tasks.find(region_id) != tasks.end();
        }
    };

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    RegionManager region_manager;

    std::unique_ptr<RegionPersister> region_persister;

    std::atomic<Timepoint> last_gc_time = Timepoint::min();

    mutable std::mutex task_mutex;

    // raft_cmd_res stores the result of applying raft cmd. It must be protected by task_mutex.
    std::unique_ptr<RaftCommandResult> raft_cmd_res;

    LoggerPtr log;

    std::atomic<UInt64> region_compact_log_period;
    std::atomic<UInt64> region_compact_log_min_rows;
    std::atomic<UInt64> region_compact_log_min_bytes;
    std::atomic<UInt64> region_compact_log_gap;

    mutable std::mutex bg_gc_region_data_mutex;
    std::list<RegionDataReadInfoList> bg_gc_region_data;

    const TiFlashRaftProxyHelper * proxy_helper{nullptr};

    // It should be initialized after `proxy_helper` is set.
    // It should be visited from outside after status of proxy is `Running`
    ReadIndexWorkerManager * read_index_worker_manager{nullptr};

    std::atomic_int64_t read_index_event_flag{0};

    PreHandlingTrace prehandling_trace;

    StoreMeta store;
};

/// Encapsulation of lock guard of task mutex in KVStore
class KVStoreTaskLock : private boost::noncopyable
{
    friend class KVStore;
    explicit KVStoreTaskLock(std::mutex & mutex_)
        : lock(mutex_)
    {}
    std::lock_guard<std::mutex> lock;
};

void WaitCheckRegionReady(const TMTContext &, KVStore & kvstore, const std::atomic_size_t & terminate_signals_counter);
void WaitCheckRegionReady(const TMTContext &, KVStore & kvstore, const std::atomic_size_t &, double, double, double);

} // namespace DB
