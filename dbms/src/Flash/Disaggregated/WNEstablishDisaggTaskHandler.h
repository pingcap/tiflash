// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <kvproto/disaggregated.pb.h>
#include <tipb/select.pb.h>

#include <memory>

namespace DB
{
class WNEstablishDisaggTaskHandler;
class DAGContext;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

/**
 * Handling with the `EstablishDisaggTask` request on write node.
 */
class WNEstablishDisaggTaskHandler
{
public:
    void prepare(const disaggregated::EstablishDisaggTaskRequest * request);

    void execute(disaggregated::EstablishDisaggTaskResponse * response);

    WNEstablishDisaggTaskHandler(ContextPtr context_, const DM::DisaggTaskId & task_id);

private:
    ContextPtr context;
    tipb::DAGRequest dag_req;
    std::unique_ptr<DAGContext> dag_context;
    QueryExecutorHolder query_executor_holder;
    const LoggerPtr log;
    MemTrackerWrapper mem_tracker_wrapper;
};
} // namespace DB
