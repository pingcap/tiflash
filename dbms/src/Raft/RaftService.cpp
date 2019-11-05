#include <Interpreters/Context.h>
#include <Raft/RaftService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

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

    if (!db_context.getTMTContext().disableBgFlush())
    {
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
            }
            return region_table.tryFlushRegions();
        });

        region_flush_handle = background_pool.addTask([this] {
            RegionID region_id;
            {
                std::lock_guard<std::mutex> lock(region_mutex);
                if (regions_to_flush.empty())
                    return false;
                region_id = regions_to_flush.front();
                regions_to_flush.pop();
            }
            RegionTable & region_table = db_context.getTMTContext().getRegionTable();
            region_table.tryFlushRegion(region_id);
            return true;
        });
    }

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

void RaftService::addRegionToFlush(const Region & region)
{
    if (!db_context.getTMTContext().disableBgFlush())
    {
        {
            std::lock_guard<std::mutex> lock(region_mutex);
            regions_to_flush.push(region.id());
        }
        region_flush_handle->wake();
    }
    else
    {
        auto & region_table =  db_context.getTMTContext().getRegionTable();
        region_table.tryFlushRegion(region.id());
    }
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

    if (region_flush_handle)
    {
        background_pool.removeTask(region_flush_handle);
        region_flush_handle = nullptr;
    }

    if (region_decode_handle)
    {
        background_pool.removeTask(region_decode_handle);
        region_decode_handle = nullptr;
    }
}

grpc::Status RaftService::ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream)
{
    RaftContext raft_context(&db_context, grpc_context, stream);

    try
    {
        kvstore->report(raft_context);

        enginepb::CommandRequestBatch cmds;
        while (stream->Read(&cmds))
        {
            kvstore->onServiceCommand(std::move(cmds), raft_context);
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
