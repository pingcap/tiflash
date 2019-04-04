#include <Storages/Transaction/applySnapshot.h>
#include <Storages/Transaction/TMTContext.h>

#include <Raft/RaftService.h>

namespace DB
{

RaftService::RaftService(const std::string & address_, DB::Context & db_context_) : address(address_),
    db_context(db_context_), kvstore(db_context.getTMTContext().kvstore),
    background_pool(db_context.getBackgroundPool()), log(&Logger::get("RaftService"))
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);

    // Prevent TiKV from throwing "Received message larger than max (4404462 vs. 4194304)" error.
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    grpc_server = builder.BuildAndStart();

    LOG_INFO(log, "Raft service listening on [" << address << "]");
}

RaftService::~RaftService()
{
    // REVIEW: can we remove this mutex?
    std::lock_guard<std::mutex> lock{mutex};
    grpc_server->Shutdown();
    grpc_server->Wait();
}

grpc::Status RaftService::ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream)
{
    RaftContext rctx(&db_context, grpc_context, stream);
    BackgroundProcessingPool::TaskHandle persist_handle;
    BackgroundProcessingPool::TaskHandle flush_handle;

    RegionTable & region_table = db_context.getTMTContext().region_table;

    try
    {
        kvstore->report(rctx);

        // REVIEW: persisting interval is?
        persist_handle = background_pool.addTask([&, this] { return kvstore->tryPersistAndReport(rctx); });
        flush_handle = background_pool.addTask([&] { return region_table.tryFlushRegions(); });

        enginepb::CommandRequestBatch request;
        // REVIEW: should we use EOS flag?
        while (stream->Read(&request))
        {
            applyCommand(rctx, request);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "gRPC ApplyCommandBatch on " + address + " error");
    }

    // REVIEW: is this removing will cause persisting missing? For example, call write some data, and then this conn broke before flushing
    if (persist_handle)
        background_pool.removeTask(persist_handle);
    if (flush_handle)
        background_pool.removeTask(flush_handle);

    // REVIEW: should we use OK?
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

void RaftService::applyCommand(RaftContext & context, const enginepb::CommandRequestBatch & cmd)
{
    kvstore->onServiceCommand(cmd, context);
}

} // namespace DB
