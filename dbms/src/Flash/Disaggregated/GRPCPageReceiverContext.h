// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/UnaryCallback.h>
#include <Common/grpcpp.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Mpp/MPPTaskManager.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/StorageDisaggregated.h>
#include <common/types.h>
#include <grpcpp/completion_queue.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Cluster.h>
#include <tipb/executor.pb.h>

#include <memory>

namespace DB
{
using PageDataPacket = mpp::PagesPacket;
// TODO make the memory tracked
using TrackedPageDataPacketPtr = std::shared_ptr<PageDataPacket>;
using TrackedPageDataPacketPtrs = std::vector<TrackedPageDataPacketPtr>;

class FetchPagesStreamReader
{
public:
    virtual ~FetchPagesStreamReader() = default;
    virtual bool read(TrackedPageDataPacketPtr & packet) = 0;
    virtual grpc::Status finish() = 0;
    virtual void cancel(const String & reason) = 0;
};
using FetchPagesStreamReaderPtr = std::shared_ptr<FetchPagesStreamReader>;

struct FetchPagesRequest
{
    DM::RemoteSegmentReadTaskPtr seg_task;
    std::shared_ptr<mpp::FetchDisaggregatedPagesRequest> req;

    explicit FetchPagesRequest(DM::RemoteSegmentReadTaskPtr seg_task_);

    bool isValid() const { return seg_task != nullptr; }

    String identifier() const
    {
        assert(isValid());
        return fmt::format("{}+{}+{}", seg_task->store_id, seg_task->table_id, seg_task->segment_id);
    }

    String debugString() const;
};

class GRPCPagesReceiverContext
{
public:
    using Status = grpc::Status;
    using Request = FetchPagesRequest;
    using Reader = FetchPagesStreamReader;

    GRPCPagesReceiverContext(
        const DM::RemoteReadTaskPtr & remote_read_tasks,
        pingcap::kv::Cluster * cluster_,
        bool enable_async_grpc_);

    Request popRequest() const;

    bool supportAsync(const Request & request) const;

    FetchPagesStreamReaderPtr makeReader(const Request & request) const;

    static Status getStatusOK()
    {
        return grpc::Status::OK;
    }

    // Normally cancel will be sent by TiDB to all MPPTasks, so ExchangeReceiver no need to cancel.
    // But in disaggregated mode, TableScan in tiflash_compute node will be converted to ExchangeReceiver(executed in tiflash_compute node),
    // and ExchangeSender+TableScan(executed in tiflash_storage node).
    // So when we cancel the former MPPTask, the latter MPPTask needs to be handled by the tiflash_compute node itself.
    void cancelMPPTaskOnTiFlashStorageNode(LoggerPtr log);

    void finishTaskEstablish(const Request & req, bool meet_error);

    void finishTaskReceive(const DM::RemoteSegmentReadTaskPtr & seg_task);

private:
    DM::RemoteReadTaskPtr remote_read_tasks;
    pingcap::kv::Cluster * cluster;
    bool enable_async_grpc;
};
} // namespace DB
