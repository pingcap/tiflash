#include <Interpreters/Context.h>
#include <Raft/RaftService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

#include <numeric>

namespace DB
{

RaftService::RaftService(DB::Context & db_context_)
    : db_context(db_context_),
      kvstore(db_context.getTMTContext().getKVStore()),
      background_pool(db_context.getBackgroundPool()),
      log(&Logger::get("RaftService"))
{
    if (!db_context.getTMTContext().isInitialized())
        throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

    single_thread_task_handle = background_pool.addTask(
        [this] {
            auto & tmt = db_context.getTMTContext();
            {
                RegionTable & region_table = tmt.getRegionTable();
                region_table.checkTableOptimize();
            }
            kvstore->tryPersist();
            return false;
        },
        false);

    table_flush_handle = background_pool.addTask([this] {
        auto & tmt = db_context.getTMTContext();
        RegionTable & region_table = tmt.getRegionTable();

        // if all regions of table is removed, try to optimize data.
        if (auto table_id = region_table.popOneTableToOptimize(); table_id != InvalidTableID)
        {
            LOG_INFO(log, "try to final optimize table " << table_id);
            tryOptimizeStorageFinal(db_context, table_id);
            LOG_INFO(log, "finish final optimize table " << table_id);
        }
        return region_table.tryFlushRegions();
    });

    data_reclaim_handle = background_pool.addTask([this] {
        std::list<RegionDataReadInfoList> tmp;
        {
            std::lock_guard<std::mutex> lock(reclaim_mutex);
            tmp = std::move(data_to_reclaim);
        }
        auto total = std::accumulate(tmp.begin(), tmp.end(), size_t(0), [](size_t sum, const auto & list) { return sum + list.size(); });
        if (total)
            LOG_INFO(log, "Reclaimed " << total << " rows");
        return false;
    });

    region_decode_handle = background_pool.addTask([this] {
        RegionPtr region;
        {
            std::lock_guard<std::mutex> lock(region_mutex);
            if (regions_to_decode.empty())
                return false;
            auto it = regions_to_decode.begin();
            region = it->second;
            regions_to_decode.erase(it);
        }
        region->tryPreDecodeTiKVValue();
        return true;
    });

    {
        std::vector<RegionPtr> regions;
        kvstore->traverseRegions([&regions](RegionID, const RegionPtr & region) {
            if (region->dataSize())
                regions.emplace_back(region);
        });

        for (const auto & region : regions)
            addRegionToDecode(region);
    }
}

void RaftService::dataMemReclaim(DB::RegionDataReadInfoList && data)
{
    std::lock_guard<std::mutex> lock(reclaim_mutex);
    data_to_reclaim.emplace_back(std::move(data));
}

void RaftService::addRegionToDecode(const RegionPtr & region)
{
    {
        std::lock_guard<std::mutex> lock(region_mutex);
        regions_to_decode.emplace(region->id(), region);
    }
    region_decode_handle->wake();
}

RaftService::~RaftService()
{
    if (single_thread_task_handle)
    {
        background_pool.removeTask(single_thread_task_handle);
        single_thread_task_handle = nullptr;
    }
    if (table_flush_handle)
    {
        background_pool.removeTask(table_flush_handle);
        table_flush_handle = nullptr;
    }

    if (data_reclaim_handle)
    {
        background_pool.removeTask(data_reclaim_handle);
        data_reclaim_handle = nullptr;
    }

    if (region_decode_handle)
    {
        background_pool.removeTask(region_decode_handle);
        region_decode_handle = nullptr;
    }
}

grpc::Status RaftService::ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream)
{
    RaftContext raft_contex(&db_context, grpc_context, stream);

    try
    {
        kvstore->report(raft_contex);

        enginepb::CommandRequestBatch cmds;
        while (stream->Read(&cmds))
        {
            kvstore->onServiceCommand(std::move(cmds), raft_contex);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "gRPC ApplyCommandBatch error");
    }

    return grpc::Status::CANCELLED;
}

grpc::Status RaftService::ApplySnapshot(grpc::ServerContext *, CommandServerReader * reader, enginepb::SnapshotDone * /*response*/)
{
    try
    {
        applySnapshot(kvstore, std::bind(&CommandServerReader::Read, reader, std::placeholders::_1), &db_context);
        return grpc::Status::OK;
    }
    catch (...)
    {
        tryLogCurrentException(log, "gRPC ApplyCommandBatch error");
        return grpc::Status(grpc::StatusCode::UNKNOWN, "Runtime error, check theflash log for detail.");
    }
}

} // namespace DB
