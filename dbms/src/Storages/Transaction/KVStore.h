#pragma once

#include <Storages/Transaction/RegionClientCreate.h>
#include <Storages/Transaction/RegionPersister.h>

namespace DB
{

// TODO move to Settings.h
static const Seconds REGION_PERSIST_PERIOD(300);      // 5 minutes
static const Seconds KVSTORE_TRY_PERSIST_PERIOD(180); // 3 minutes

class RegionTable;
struct RaftContext;

class Region;
using RegionPtr = std::shared_ptr<Region>;

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    KVStore(const std::string & data_dir);
    void restore(const RegionClientCreateFunc & region_client_create, std::vector<RegionID> * regions_to_remove = nullptr);

    RegionPtr getRegion(RegionID region_id);

    void traverseRegions(std::function<void(RegionID region_id, const RegionPtr & region)> && callback);

    bool onSnapshot(RegionPtr new_region, RegionTable * region_table, const std::optional<UInt64> expect_old_index = {});
    // TODO: remove RaftContext and use Context + CommandServerReaderWriter
    void onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & context);

    // Send all regions status to remote TiKV.
    void report(RaftContext & context);

    // Persist chosen regions.
    // Currently we also trigger region files GC in it.
    bool tryPersistAndReport(RaftContext & raft_ctx, const Seconds kvstore_try_persist_period = KVSTORE_TRY_PERSIST_PERIOD,
        const Seconds region_persist_period = REGION_PERSIST_PERIOD);

    size_t regionSize() const;

    void removeRegion(RegionID region_id, RegionTable * context);

    void updateRegionTableBySnapshot(RegionTable & region_table);

private:
    RegionPersister region_persister;
    RegionMap regions;

    mutable std::mutex mutex;

    std::atomic<Timepoint> last_try_persist_time = Clock::now();

    // onServiceCommand and onSnapshot should not be called concurrently
    mutable std::mutex task_mutex;

    Logger * log;
};

using KVStorePtr = std::shared_ptr<KVStore>;

} // namespace DB
