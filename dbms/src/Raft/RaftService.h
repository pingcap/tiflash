#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <boost/noncopyable.hpp>

#include <Poco/Util/LayeredConfiguration.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Raft/RaftContext.h>
#include <Storages/Transaction/KVStore.h>

#include <Storages/MergeTree/BackgroundProcessingPool.h>

namespace DB
{

class RaftService final : public enginepb::Engine::Service, public std::enable_shared_from_this<RaftService>, private boost::noncopyable
{
public:
    RaftService(const std::string & address_, DB::Context & db_context);

    ~RaftService() final;

private:
    grpc::Status ApplyCommandBatch(grpc::ServerContext * grpc_context, CommandServerReaderWriter * stream) override;

    grpc::Status ApplySnapshot(grpc::ServerContext * grpc_context, CommandServerReader * reader,
        enginepb::SnapshotDone * response) override;

    void applyCommand(RaftContext & context, const enginepb::CommandRequestBatch & cmd);

private:
    std::string address;

    GRPCServerPtr grpc_server;

    std::mutex mutex;

    Context &  db_context;
    KVStorePtr kvstore;

    BackgroundProcessingPool & background_pool;

    Logger * log;
};

} // namespace DB
