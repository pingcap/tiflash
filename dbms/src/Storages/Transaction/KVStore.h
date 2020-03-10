#pragma once

#include <Storages/Transaction/IndexReaderCreate.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/RegionsRangeIndex.h>

namespace DB
{

// TODO move to Settings.h
static const Seconds REGION_CACHE_GC_PERIOD(60 * 5);

class Context;
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

struct MockTiDBTable;
struct TiKVRangeKey;

class TMTContext;

struct SnapshotDataView;
struct WriteCmdsView;

enum TiFlashApplyRes : uint32_t;

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    KVStore(const std::string & data_dir);
    void restore(const IndexReaderCreateFunc & index_reader_create);
    RegionPtr getRegion(const RegionID region_id) const;

    using RegionsAppliedindexMap = std::unordered_map<RegionID, std::pair<RegionPtr, UInt64>>;
    using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;
    /// Get and callback all regions whose range overlapped with start/end key.
    void handleRegionsByRangeOverlap(const RegionRange & range, std::function<void(RegionMap, const KVStoreTaskLock &)> && callback) const;

    void traverseRegions(std::function<void(RegionID, const RegionPtr &)> && callback) const;

    bool onSnapshot(
        RegionPtr new_region, TMTContext & tmt, const RegionsAppliedindexMap & regions_to_check = {}, bool try_flush_region = false);

    void gcRegionCache(Seconds gc_persist_period = REGION_CACHE_GC_PERIOD);

    void tryPersist(const RegionID region_id);

    static void tryFlushRegionCacheInStorage(TMTContext & tmt, const Region & region, Poco::Logger * log);

    size_t regionSize() const;
    TiFlashApplyRes handleAdminRaftCmd(raft_cmdpb::AdminRequest && request,
        raft_cmdpb::AdminResponse && response,
        UInt64 region_id,
        UInt64 index,
        UInt64 term,
        TMTContext & tmt);
    TiFlashApplyRes handleWriteRaftCmd(
        raft_cmdpb::RaftCmdRequest && request, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt);
    TiFlashApplyRes handleWriteRaftCmd(const WriteCmdsView & cmds, UInt64 region_id, UInt64 index, UInt64 term, TMTContext & tmt);
    void handleApplySnapshot(metapb::Region && region, uint64_t peer_id, const SnapshotDataView & lock_buff,
        const SnapshotDataView & write_buff, const SnapshotDataView & default_buff, uint64_t index, uint64_t term, TMTContext & tmt);
    bool tryApplySnapshot(RegionPtr new_region, Context & context, bool try_flush_region);
    void handleDestroy(UInt64 region_id, TMTContext & tmt);
    void setRegionCompactLogPeriod(Seconds period);

private:
    friend class MockTiDB;
    friend struct MockTiDBTable;
    friend void dbgFuncRemoveRegion(Context &, const ASTs &, /*DBGInvoker::Printer*/ std::function<void(const std::string &)>);
    void removeRegion(
        const RegionID region_id, RegionTable & region_table, const KVStoreTaskLock & task_lock, const RegionTaskLock & region_lock);
    void mockRemoveRegion(const RegionID region_id, RegionTable & region_table);
    KVStoreTaskLock genTaskLock() const;

    using RegionManageLock = std::lock_guard<std::mutex>;
    RegionManageLock genRegionManageLock() const;

    RegionMap & regionsMut();
    const RegionMap & regions() const;

private:
    RegionManager region_manager;

    RegionPersister region_persister;

    std::atomic<Timepoint> last_gc_time = Timepoint::min();

    mutable std::mutex task_mutex;
    // region_range_index must be protected by task_mutex. It's used to search for region by range.
    // region merge/split/apply-snapshot/remove will change the range.
    RegionsRangeIndex region_range_index;

    // raft_cmd_res stores the result of applying raft cmd. It must be protected by task_mutex.
    std::unique_ptr<RaftCommandResult> raft_cmd_res;

    Logger * log;

    std::atomic<Seconds> REGION_COMPACT_LOG_PERIOD;
};

/// Encapsulation of lock guard of task mutex in KVStore
class KVStoreTaskLock : private boost::noncopyable
{
    friend class KVStore;
    KVStoreTaskLock(std::mutex & mutex_) : lock(mutex_) {}
    std::lock_guard<std::mutex> lock;
};

} // namespace DB
