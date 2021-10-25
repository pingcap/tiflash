#pragma once

#include <Common/Exception.h>
#include <Common/LogWithPrefix.h>
#include <Common/MemoryTracker.h>
#include <DataStreams/BlockIO.h>
#include <Flash/Coprocessor/DAGContext.h>
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

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    uint64_t start_ts;
    int64_t task_id;

    bool operator<(const MPPTaskId & rhs) const { return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id); }

    String toString() const;
};

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

    std::vector<RegionInfo> prepare(const mpp::DispatchTaskRequest & task_request);

    void preprocess();

    void run();

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel);

    // tunnel and error_message
    std::pair<MPPTunnelPtr, String> getTunnel(const ::mpp::EstablishMPPConnectionRequest * request);

    ~MPPTask();

private:
    MPPTask(const mpp::TaskMeta & meta_, const Context & context_);

    void runImpl();

    void unregisterTask();

    void writeErrToAllTunnels(const String & e);

    /// Similar to `writeErrToAllTunnels`, but it just try to write the error message to tunnel
    /// without waiting the tunnel to be connected
    void closeAllTunnels(const String & reason);

    void finishWrite();

    bool switchStatus(TaskStatus from, TaskStatus to);

    Context context;

    RegionInfoMap local_regions;
    RegionInfoList remote_regions;

    std::unique_ptr<tipb::DAGRequest> dag_req;
    std::unique_ptr<DAGContext> dag_context;

    /// store io in MPPTask to keep the life cycle of memory_tracker for the current query
    /// BlockIO contains some information stored in Context and DAGContext, so need deconstruct it before Context and DAGContext
    BlockIO io;
    MemoryTracker * memory_tracker = nullptr;

    MPPTaskId id;

    std::atomic<TaskStatus> status{INITIALIZING};

    mpp::TaskMeta meta;
    MPPTunnelSetPtr tunnel_set;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    MPPTaskManager * manager = nullptr;

    const LogWithPrefixPtr log;

    Exception err;

    friend class MPPTaskManager;
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

using MPPTaskMap = std::map<MPPTaskId, MPPTaskPtr>;

} // namespace DB
