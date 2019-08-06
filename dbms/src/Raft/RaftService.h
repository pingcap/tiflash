#pragma once

#include <memory>
#include <queue>

#include <Raft/RaftContext.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
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

class RaftService final : public enginepb::Engine::Service, public std::enable_shared_from_this<RaftService>, private boost::noncopyable
{
public:
    RaftService(const std::string & address_, Context & db_context);

    ~RaftService() final;

    void addRegionToFlush(const Region & region);

private:
    grpc::Status ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream) override;

    grpc::Status ApplySnapshot(
        grpc::ServerContext * grpc_context, CommandServerReader * reader, enginepb::SnapshotDone * response) override;

private:
    std::string address;

    GRPCServerPtr grpc_server;

    Context & db_context;
    KVStorePtr kvstore;

    BackgroundProcessingPool & background_pool;

    Logger * log;

    std::mutex mutex;
    std::queue<RegionID> regions_to_flush;

    BackgroundProcessingPool::TaskHandle persist_handle;
    BackgroundProcessingPool::TaskHandle table_flush_handle;
    BackgroundProcessingPool::TaskHandle region_flush_handle;
};

} // namespace DB
