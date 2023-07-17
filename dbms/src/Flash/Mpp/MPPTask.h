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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Mpp/MPPReceiverSet.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/MPPTaskScheduleEntry.h>
#include <Flash/Mpp/MPPTaskStatistics.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TaskStatus.h>
#include <Interpreters/Context_fwd.h>
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
using MPPTaskManagerPtr = std::shared_ptr<MPPTaskManager>;
class DAGContext;
class ProcessListEntry;

enum class AbortType
{
    /// todo add ONKILL to distinguish between silent cancellation and kill
    ONCANCELLATION,
    ONERROR,
};

// This struct notify the MPPTaskManager that this MPPTask is completed destructed
class MPPTaskMonitorHelper
{
public:
    MPPTaskMonitorHelper() = default;

    ~MPPTaskMonitorHelper();

    void initAndAddself(MPPTaskManager * manager_, const String & task_unique_id_);

private:
    MPPTaskManager * manager = nullptr;
    String task_unique_id;
    bool initialized = false;
};

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

    bool isRootMPPTask() const;

    TaskStatus getStatus() const { return status.load(); }

    void handleError(const String & error_msg);

    void prepare(const mpp::DispatchTaskRequest & task_request);

    void run();

    bool scheduleThisTask(ScheduleState state);

    MPPTaskScheduleEntry & getScheduleEntry() { return schedule_entry; }

    // tunnel and error_message
    std::pair<MPPTunnelPtr, String> getTunnel(const ::mpp::EstablishMPPConnectionRequest * request);

    ~MPPTask();

private:
    MPPTask(const mpp::TaskMeta & meta_, const ContextPtr & context_);

    void runImpl();

    void unregisterTask();

    // abort the mpp task, note this function should be non-blocking, it just set some flags
    void abort(const String & message, AbortType abort_type);

    void abortTunnels(const String & message, bool wait_sender_finish);
    void abortReceivers();
    void abortQueryExecutor();

    void finishWrite();

    bool switchStatus(TaskStatus from, TaskStatus to);

    void preprocess();

    void scheduleOrWait();

    int estimateCountOfNewThreads();

    void registerTunnels(const mpp::DispatchTaskRequest & task_request);

    void initProcessListEntry(const std::shared_ptr<ProcessListEntry> & query_process_list_entry);

    void initExchangeReceivers();

    String getErrString() const;
    void setErrString(const String & message);

    MemoryTracker * getMemoryTracker() const;

    void reportStatus(const String & err_msg);

private:
    struct ProcessListEntryHolder
    {
        std::shared_ptr<ProcessListEntry> process_list_entry;
        ~ProcessListEntryHolder()
        {
            /// Because MemoryTracker is now saved in `MPPQuery` and shared by all the mpp tasks belongs to the same mpp query,
            /// it may not be destructed when MPPTask is destructed, so need to manually reset current_memory_tracker to nullptr at the
            /// end of the destructor of MPPTask, otherwise, current_memory_tracker may point to a invalid memory tracker
            current_memory_tracker = nullptr;
        }
    };
    // We must ensure this member variable is put at this place to be destructed at proper time
    MPPTaskMonitorHelper mpp_task_monitor_helper;

    // To make sure dag_req is not destroyed before the mpp task ends.
    tipb::DAGRequest dag_req;
    mpp::TaskMeta meta;
    MPPTaskId id;

    ContextPtr context;

    MPPTaskManager * manager;
    std::atomic<bool> is_public{false};

    MPPTaskScheduleEntry schedule_entry;

    ProcessListEntryHolder process_list_entry_holder;
    // `dag_context` holds inputstreams which could hold ref to `context` so it should be destructed
    // before `context`.
    std::unique_ptr<DAGContext> dag_context;

    QueryExecutorHolder query_executor_holder;

    std::atomic<TaskStatus> status{INITIALIZING};

    /// Used to protect concurrent access to `err_string`, `tunnel_set`, and `receiver_set`.
    mutable std::mutex mtx;

    String err_string;

    MPPTunnelSetPtr tunnel_set;

    MPPReceiverSetPtr receiver_set;

    int new_thread_count_of_mpp_receiver = 0;

    const LoggerPtr log;

    MPPTaskStatistics mpp_task_statistics;

    friend class MPPTaskManager;
    friend class MPPHandler;
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

using MPPTaskMap = std::unordered_map<MPPTaskId, MPPTaskPtr>;

} // namespace DB
