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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TaskStatus.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <kvproto/mpp.pb.h>

#include <atomic>
#include <boost/noncopyable.hpp>
#include <memory>
#include <unordered_map>

namespace DB
{
class MPPTaskManager;
class MPPTask : public std::enable_shared_from_this<MPPTask>
    , private boost::noncopyable
{
public:
    using Ptr = std::shared_ptr<MPPTask>;

    /// Ensure all MPPTasks are allocated as std::shared_ptr
    template <typename... Args>
    static Ptr newTask(Args &&... args)
    {
        return Ptr(new MPPTask(std::forward<Args>(args)...));
    }

    const MPPTaskId & getId() const { return id; }

    bool isRootMPPTask() const { return dag_context->isRootMPPTask(); }

    TaskStatus getStatus() const { return status.load(); }

    void cancel(const String & reason);

    void prepare(const mpp::DispatchTaskRequest & task_request);

    void run();

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel);

    int getNeededThreads();

    enum class ScheduleState
    {
        WAITING,
        SCHEDULED,
        FAILED,
        EXCEEDED,
        COMPLETED
    };

    bool scheduleThisTask(ScheduleState state);

    bool isScheduled();

    // tunnel and error_message
    std::pair<MPPTunnelPtr, String> getTunnel(const ::mpp::EstablishMPPConnectionRequest * request);

    ~MPPTask();

private:
    MPPTask(const mpp::TaskMeta & meta_, const ContextPtr & context_);

    void runImpl();

    void unregisterTask();

    void writeErrToAllTunnels(const String & e);

    /// Similar to `writeErrToAllTunnels`, but it just try to write the error message to tunnel
    /// without waiting the tunnel to be connected
    void closeAllTunnels(const String & reason);

    void finishWrite();

    bool switchStatus(TaskStatus from, TaskStatus to);

    void preprocess();

    void scheduleOrWait();

    int estimateCountOfNewThreads();

    void sendCancelToQuery(bool kill);

    BlockInputStreamPtr getDataStream();

    tipb::DAGRequest dag_req;

    ContextPtr context;
    // `dag_context` holds inputstreams which could hold ref to `context` so it should be destructed
    // before `context`.
    std::unique_ptr<DAGContext> dag_context;
    MemoryTracker * memory_tracker = nullptr;

    std::atomic<TaskStatus> status{INITIALIZING};

    mpp::TaskMeta meta;

    MPPTaskId id;

    MPPTunnelSetPtr tunnel_set;

    // which targeted task we should send data by which tunnel.
    std::unordered_map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    std::mutex stream_mu;
    // must destroy before DAGContext.
    BlockInputStreamPtr data_stream;

    MPPTaskManager * manager = nullptr;

    const LoggerPtr log;

    MPPTaskStatistics mpp_task_statistics;

    Exception err;

    friend class MPPTaskManager;

    int needed_threads;

    std::mutex schedule_mu;
    std::condition_variable schedule_cv;
    ScheduleState schedule_state;
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

using MPPTaskMap = std::unordered_map<MPPTaskId, MPPTaskPtr>;

} // namespace DB
