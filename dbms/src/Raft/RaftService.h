#pragma once

#include <memory>
#include <queue>

#include <Raft/RaftContext.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;

class RaftService final : public enginepb::Engine::Service, public std::enable_shared_from_this<RaftService>, private boost::noncopyable
{
public:
    RaftService(Context & db_context);

    ~RaftService() final;

    void dataMemReclaim(RegionDataReadInfoList &&);
    void addRegionToDecode(const RegionPtr & region);

private:
    grpc::Status ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream) override;

    grpc::Status ApplySnapshot(
        grpc::ServerContext * grpc_context, CommandServerReader * reader, enginepb::SnapshotDone * response) override;

private:
    Context & db_context;
    KVStorePtr kvstore;

    BackgroundProcessingPool & background_pool;

    Logger * log;

    std::mutex apply_raft_cmd_mutex;

    std::mutex region_mutex;
    RegionMap regions_to_decode;

    std::mutex reclaim_mutex;
    std::list<RegionDataReadInfoList> data_to_reclaim;

    BackgroundProcessingPool::TaskHandle single_thread_task_handle;
    BackgroundProcessingPool::TaskHandle table_flush_handle;

    // kvstore will try to flush data into ch when handling raft cmd CompactLog in order to reduce the size of region.
    // use this task to reclaim data in another thread.
    BackgroundProcessingPool::TaskHandle data_reclaim_handle;
    BackgroundProcessingPool::TaskHandle region_decode_handle;
};

} // namespace DB
