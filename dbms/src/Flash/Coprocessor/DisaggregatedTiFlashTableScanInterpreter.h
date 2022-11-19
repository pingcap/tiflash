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

#include <Common/Logger.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Interpreters/Context.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

class DisaggregatedTiFlashTableScanInterpreter
{
public:
    DisaggregatedTiFlashTableScanInterpreter(
            Context & context_,
            const TiDBTableScan & table_scan_,
            const std::vector<RemoteRequest> & remote_requests_,
            LoggerPtr log_)
        : context(context_)
        , table_scan(table_scan_)
        , remote_requests(remote_requests_)
        , log(log_)
        , sender_target_task_start_ts(context_.getDAGContext()->getMPPTaskMeta().start_ts())
        , sender_target_task_task_id(context_.getDAGContext()->getMPPTaskMeta().task_id()) {}
    void execute(DAGPipeline & pipeline);

    // To help find exec summary of ExchangeSender in tiflash_storage and merge it into TableScan's exec summary.
    static const String ExecIDPrefixForTiFlashStorageSender;
private:
    std::vector<pingcap::coprocessor::BatchCopTask> buildBatchCopTasks();
    std::shared_ptr<mpp::DispatchTaskRequest> buildDispatchMPPTaskRequest(const pingcap::coprocessor::BatchCopTask & batch_cop_task);
    std::vector<std::shared_ptr<::mpp::DispatchTaskRequest>> buildAndDispatchMPPTaskRequests();

    void buildReceiverStreams(const std::vector<std::shared_ptr<::mpp::DispatchTaskRequest>> & dispatch_reqs, DAGPipeline & pipeline);

    Context & context;
    const TiDBTableScan & table_scan;
    const std::vector<RemoteRequest> & remote_requests;
    LoggerPtr log;
    uint64_t sender_target_task_start_ts;
    int64_t sender_target_task_task_id;
};
} // namespace DB
