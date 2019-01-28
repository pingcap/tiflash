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
    std::lock_guard<std::mutex> lock{mutex};
    grpc_server->Shutdown();
    grpc_server->Wait();
}

grpc::Status RaftService::ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream)
{
    RaftContext rctx(&db_context, grpc_context, stream);
    BackgroundProcessingPool::TaskHandle persist_handle;
    try
    {
        kvstore->report(rctx);

        persist_handle = background_pool.addTask([&, this] { return kvstore->tryPersistAndReport(rctx); });

        enginepb::CommandRequestBatch request;
        while (stream->Read(&request))
        {
            applyCommand(rctx, request);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "gRPC ApplyCommandBatch on " + address + " error");
    }

    if (persist_handle)
        background_pool.removeTask(persist_handle);

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
