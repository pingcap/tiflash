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


namespace DB
{

/// TODO: brief design document.
class KVStore final : private boost::noncopyable
{
public:
    KVStore(const std::string & data_dir, Context * context = nullptr);
    RegionPtr getRegion(RegionID region_id);
    void traverseRegions(std::function<void(Region * region)> callback);

    void onSnapshot(const RegionPtr & region, Context * context);
    // TODO: remove RaftContext and use Context + CommandServerReaderWriter
    void onServiceCommand(const enginepb::CommandRequestBatch & cmds, RaftContext & context);

    // Send all regions status to remote TiKV.
    void report(RaftContext & context);

    // Persist and report those expired regions.
    // Currently we also trigger region files GC in it.
    bool tryPersistAndReport(RaftContext & context);

    // For test, please do NOT remove.
    RegionMap & _regions() { return regions; }

private:
    void removeRegion(RegionID region_id, Context * context);

private:
    RegionPersister region_persister;
    RegionMap regions;

    std::mutex mutex;

    Consistency consistency;
    Timepoint last_try_persist_time = Clock::now();

    Logger * log;
};

using KVStorePtr = std::shared_ptr<KVStore>;

} // namespace DB
