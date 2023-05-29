// Copyright 2023 PingCAP, Ltd.
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
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <common/types.h>
#include <kvproto/disaggregated.pb.h>
#include <pingcap/kv/Cluster.h>

#include <memory>

namespace DB
{
using PageDataPacket = disaggregated::PagesPacket;
// TODO make the memory tracked
using TrackedPageDataPacketPtr = std::shared_ptr<PageDataPacket>;
using TrackedPageDataPacketPtrs = std::vector<TrackedPageDataPacketPtr>;

class FetchPagesResponseReader
{
public:
    virtual ~FetchPagesResponseReader() = default;
    virtual bool read(TrackedPageDataPacketPtr & packet) = 0;
    virtual grpc::Status finish() = 0;
    virtual void cancel(const String & reason) = 0;
};
using FetchPagesResponseReaderPtr = std::shared_ptr<FetchPagesResponseReader>;

struct FetchPagesRequest
{
    DM::RNRemoteSegmentReadTaskPtr seg_task;
    std::shared_ptr<disaggregated::FetchDisaggPagesRequest> req;

    explicit FetchPagesRequest(DM::RNRemoteSegmentReadTaskPtr seg_task_);

    bool isValid() const { return seg_task != nullptr; }

    const String & address() const;

    String identifier() const
    {
        assert(isValid());
        return fmt::format("{}+{}+{}+{}", seg_task->store_id, seg_task->ks_table_id.first, seg_task->ks_table_id.second, seg_task->segment_id);
    }

    String debugString() const;
};

class GRPCPagesReceiverContext
{
public:
    using Status = grpc::Status;

    GRPCPagesReceiverContext(
        const DM::RNRemoteReadTaskPtr & remote_read_tasks,
        pingcap::kv::Cluster * cluster_);

    FetchPagesRequest nextFetchPagesRequest() const;

    FetchPagesResponseReaderPtr doRequest(const FetchPagesRequest & request) const;

    static Status getStatusOK()
    {
        return grpc::Status::OK;
    }

    // When error happens, try cancel disagg task on the storage node side.
    void cancelDisaggTaskOnTiFlashStorageNode(LoggerPtr log);

    void finishTaskEstablish(const FetchPagesRequest & req, bool meet_error);

    void finishTaskReceive(const DM::RNRemoteSegmentReadTaskPtr & seg_task);

private:
    // The remote segment task pool
    DM::RNRemoteReadTaskPtr remote_read_tasks;
    pingcap::kv::Cluster * cluster;
};
} // namespace DB
