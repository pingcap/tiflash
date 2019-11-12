#pragma once

#include <Storages/Transaction/IndexReaderCreate.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/RegionsRangeIndex.h>

namespace DB
{

// TODO move to Settings.h
static const Seconds REGION_PERSIST_PERIOD(300);      // 5 minutes
static const Seconds KVSTORE_TRY_PERSIST_PERIOD(180); // 3 minutes

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class RegionTable;
struct RaftContext;

class Region;
using RegionPtr = std::shared_ptr<Region>;
struct RaftCommandResult;
class KVStoreTaskLock;

struct MockTiDBTable;
struct TiKVRangeKey;

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

    bool onSnapshot(RegionPtr new_region, Context * context, const RegionsAppliedindexMap & regions_to_check = {}, bool try_flush_region = false);

    // TODO: remove RaftContext and use Context + CommandServerReaderWriter
    void onServiceCommand(enginepb::CommandRequestBatch && cmds, RaftContext & context);

    // Send all regions status to remote TiKV.
    void report(RaftContext & context);

    // Persist chosen regions.
    // Currently we also trigger region files GC in it.
    bool tryPersist(
        const Seconds kvstore_try_persist_period = KVSTORE_TRY_PERSIST_PERIOD, const Seconds region_persist_period = REGION_PERSIST_PERIOD);

    void tryPersist(const RegionID region_id);

    size_t regionSize() const;

private:
    friend class MockTiDB;
    friend struct MockTiDBTable;
    friend void dbgFuncRemoveRegion(Context &, const ASTs &, std::function<void(const std::string &)>);
    void removeRegion(const RegionID region_id, RegionTable * region_table, const KVStoreTaskLock & task_lock);
    KVStoreTaskLock genTaskLock() const;

    using RegionManageLock = std::lock_guard<std::mutex>;
    RegionManageLock genRegionManageLock() const;

    RegionMap & regionsMut();
    const RegionMap & regions() const;

private:
    RegionManager region_manager;

    RegionPersister region_persister;

    std::atomic<Timepoint> last_try_persist_time = Clock::now();

    // onServiceCommand and onSnapshot should not be called concurrently
    mutable std::mutex task_mutex;
    // region_range_index must be protected by task_mutex. It's used to search for region by range.
    // region merge/split/apply-snapshot/remove will change the range.
    RegionsRangeIndex region_range_index;

    // raft_cmd_res stores the result of applying raft cmd. It must be protected by task_mutex.
    std::unique_ptr<RaftCommandResult> raft_cmd_res;

    Logger * log;
};

/// Encapsulation of lock guard of task mutex in KVStore
class KVStoreTaskLock : private boost::noncopyable
{
    friend class KVStore;
    KVStoreTaskLock(std::mutex & mutex_) : lock(mutex_) {}
    std::lock_guard<std::mutex> lock;
};

} // namespace DB
