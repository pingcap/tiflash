#pragma once

#include <grpc++/grpc++.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/enginepb.grpc.pb.h>
#include <kvproto/enginepb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class Context;
using CommandServerReaderWriter = grpc::ServerReaderWriter<enginepb::CommandResponseBatch, enginepb::CommandRequestBatch>;
using CommandServerReader = grpc::ServerReader<enginepb::SnapshotRequest>;
using GRPCServerPtr = std::unique_ptr<grpc::Server>;

struct RaftContext
{
    RaftContext() = default;

    RaftContext(Context * context_, grpc::ServerContext * grpc_context_, CommandServerReaderWriter * stream_)
        : context(context_), grpc_context(grpc_context_), stream(stream_)
    {}

    Context * context = nullptr;
    grpc::ServerContext * grpc_context = nullptr;

    void send(const enginepb::CommandResponseBatch & resp)
    {
        if (!stream)
            return;
        std::lock_guard<std::mutex> lock(mutex);
        stream->Write(resp);
    }

private:
    // We temporary use this mutex to protect stream, will later change to asynchronous message sender.
    std::mutex mutex;
    CommandServerReaderWriter * stream = nullptr;
};

} // namespace DB
