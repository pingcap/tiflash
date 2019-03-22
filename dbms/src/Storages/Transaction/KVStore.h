#pragma once

#include <functional>
#include <unordered_map>
#include <vector>

#include <IO/WriteHelpers.h>
#include <Raft/RaftContext.h>

#include <Interpreters/Context.h>
#include <Storages/Transaction/Consistency.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionPersister.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/RegionTable.h>


namespace DB
{

// TODO move to Settings.h
static const Seconds REGION_PERSIST_PERIOD(120);      // 2 minutes
static const Seconds KVSTORE_TRY_PERSIST_PERIOD(20); // 20 seconds

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    KVStore(const std::string & data_dir);
    void restore(const Region::RegionClientCreateFunc & region_client_create, std::vector<RegionID> * regions_to_remove = nullptr);

    RegionPtr getRegion(RegionID region_id);
    void traverseRegions(std::function<void(const RegionID region_id, const RegionPtr & region)> callback);

    void onSnapshot(RegionPtr region, Context * context);
    // TODO: remove RaftContext and use Context + CommandServerReaderWriter
    void onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & context);

    // Send all regions status to remote TiKV.
    void report(RaftContext & context);

    // Persist and report those expired regions.
    // Currently we also trigger region files GC in it.
    bool tryPersistAndReport(RaftContext & context, const Seconds kvstore_try_persist_period=KVSTORE_TRY_PERSIST_PERIOD,
        const Seconds region_persist_period=REGION_PERSIST_PERIOD);

    const RegionMap & getRegions();

    void removeRegion(RegionID region_id, Context * context);

    void checkRegion(RegionTable & region_table);

private:
    RegionPersister region_persister;
    RegionMap regions;

    std::mutex mutex;

    Consistency consistency;
    std::atomic<Timepoint> last_try_persist_time = Clock::now();

    // onServiceCommand and onSnapshot should not be called concurrently
    mutable std::mutex task_mutex;

    Logger * log;
};

using KVStorePtr = std::shared_ptr<KVStore>;

} // namespace DB
