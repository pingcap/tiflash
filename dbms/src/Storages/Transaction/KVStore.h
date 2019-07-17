#pragma once

#include <Storages/Transaction/RegionClientCreate.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>

namespace DB
{

// TODO move to Settings.h
static const Seconds REGION_PERSIST_PERIOD(300);      // 5 minutes
static const Seconds KVSTORE_TRY_PERSIST_PERIOD(180); // 3 minutes

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class RegionTable;
struct RaftContext;

class Region;
using RegionPtr = std::shared_ptr<Region>;

struct MockTiDBTable;

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    KVStore(const std::string & data_dir);
    void restore(const RegionClientCreateFunc & region_client_create);

    RegionPtr getRegion(const RegionID region_id) const;

    void traverseRegions(std::function<void(RegionID region_id, const RegionPtr & region)> && callback) const;

    bool onSnapshot(RegionPtr new_region, RegionTable * region_table);
    // TODO: remove RaftContext and use Context + CommandServerReaderWriter
    void onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & context);

    // Send all regions status to remote TiKV.
    void report(RaftContext & context);

    // Persist chosen regions.
    // Currently we also trigger region files GC in it.
    bool tryPersist(
        const Seconds kvstore_try_persist_period = KVSTORE_TRY_PERSIST_PERIOD, const Seconds region_persist_period = REGION_PERSIST_PERIOD);

    void tryPersist(const RegionID region_id);

    size_t regionSize() const;

    void updateRegionTableBySnapshot(RegionTable & region_table);

private:
    friend struct MockTiDBTable;
    void removeRegion(const RegionID region_id, RegionTable * region_table);

    RegionMap & regions();
    const RegionMap & regions() const;
    std::mutex & mutex() const;

private:
    RegionManager region_manager;

    RegionPersister region_persister;

    std::atomic<Timepoint> last_try_persist_time = Clock::now();

    // onServiceCommand and onSnapshot should not be called concurrently
    mutable std::mutex task_mutex;

    Logger * log;
};

} // namespace DB
