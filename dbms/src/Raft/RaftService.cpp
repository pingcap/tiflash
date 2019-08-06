#include <Interpreters/Context.h>
#include <Raft/RaftService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/applySnapshot.h>

namespace DB
{

RaftService::RaftService(const std::string & address_, DB::Context & db_context_)
    : address(address_),
      db_context(db_context_),
      kvstore(db_context.getTMTContext().getKVStore()),
      background_pool(db_context.getBackgroundPool()),
      log(&Logger::get("RaftService"))
{
    if (!db_context.getTMTContext().isInitialized())
        throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    // Prevent TiKV from throwing "Received message larger than max (4404462 vs. 4194304)" error.
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    grpc_server = builder.BuildAndStart();

    persist_handle = background_pool.addTask([this] { return kvstore->tryPersist(); }, false);

    table_flush_handle = background_pool.addTask([this] {
        RegionTable & region_table = db_context.getTMTContext().getRegionTable();
        return region_table.tryFlushRegions();
    });

    region_flush_handle = background_pool.addTask([this] {
        RegionID region_id;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (regions_to_flush.empty())
                return false;
            region_id = regions_to_flush.front();
            regions_to_flush.pop();
        }
        RegionTable & region_table = db_context.getTMTContext().getRegionTable();
        region_table.tryFlushRegion(region_id);
        return true;
    });


    LOG_INFO(log, "Raft service listening on [" << address << "]");
}

void RaftService::addRegionToFlush(const Region & region)
{
    {
        std::lock_guard<std::mutex> lock(mutex);
        regions_to_flush.push(region.id());
    }
    region_flush_handle->wake();
}

RaftService::~RaftService()
{
    if (persist_handle)
    {
        background_pool.removeTask(persist_handle);
        persist_handle = nullptr;
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

    // wait 5 seconds for pending rpcs to gracefully stop
    gpr_timespec deadline{5, 0, GPR_TIMESPAN};
    LOG_DEBUG(log, "Begin to shutting down grpc server");
    grpc_server->Shutdown(deadline);
    grpc_server->Wait();
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
        tryLogCurrentException(log, "gRPC ApplyCommandBatch on " + address + " error");
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
        tryLogCurrentException(log, "gRPC ApplyCommandBatch on " + address + " error");
        return grpc::Status(grpc::StatusCode::UNKNOWN, "Runtime error, check theflash log for detail.");
    }
}

} // namespace DB
