#pragma once

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    uint64_t start_ts;
    int64_t task_id;

    bool operator<(const MPPTaskId & rhs) const
    {
        return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id);
    }

    String toString() const
    {
        return "[" + std::to_string(start_ts) + "," + std::to_string(task_id) + "]";
    }
};

struct MPPTaskProgress
{
    std::atomic<UInt64> current_progress{0};
    UInt64 progress_on_last_check = 0;
    UInt64 epoch_when_found_no_progress = 0;
    bool found_no_progress = false;
    bool isTaskHanging(const Context & context);
};

class MPPTaskManager;
class MPPTask : public std::enable_shared_from_this<MPPTask>, private boost::noncopyable
{
    using Ptr = std::shared_ptr<MPPTask>;

    /// Ensure all MPPTasks are allocated as std::shared_ptr
    template <typename ... Args>
    static Ptr newTask(Args &&... args)
    {
        return Ptr(new MPPTask(std::forward<Args>(args)...));
    }

    const MPPTaskId & getId() const { return id; }

    bool isRootMPPTask() const { return dag_context->isRootMPPTask(); }

    TaskStatus getStatus() const { return static_cast<TaskStatus>(status.load()); }

    void unregisterTask();

    bool isTaskHanging();

    void cancel(const String & reason);

    /// Similar to `writeErrToAllTunnel`, but it just try to write the error message to tunnel
    /// without waiting the tunnel to be connected
    void closeAllTunnel(const String & reason)
    {
        for (auto & it : tunnel_map)
        {
            it.second->close(reason);
        }
    }

    void finishWrite()
    {
        for (auto it : tunnel_map)
        {
            it.second->writeDone();
        }
    }

    void writeErrToAllTunnel(const String & e);

    std::vector<RegionInfo> prepare(const mpp::DispatchTaskRequest & task_request);

    void updateProgress(const Progress &) { task_progress.current_progress++; }

    void run()
    {
        std::thread worker(&MPPTask::runImpl, this->shared_from_this());
        worker.detach();
    }

    void registerTunnel(const MPPTaskId & id, MPPTunnelPtr tunnel)
    {
        if (status == CANCELLED)
            throw Exception("the tunnel " + tunnel->id() + " can not been registered, because the task is cancelled");
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        if (tunnel_map.find(id) != tunnel_map.end())
        {
            throw Exception("the tunnel " + tunnel->id() + " has been registered");
        }
        tunnel_map[id] = tunnel;
        cv.notify_all();
    }

    MPPTunnelPtr getTunnelWithTimeout(const ::mpp::EstablishMPPConnectionRequest * request, std::chrono::seconds timeout, String & err_msg)
    {
        MPPTaskId id{request->receiver_meta().start_ts(), request->receiver_meta().task_id()};
        std::map<MPPTaskId, MPPTunnelPtr>::iterator it;
        bool cancelled = false;
        std::unique_lock<std::mutex> lk(tunnel_mutex);
        auto ret = cv.wait_for(lk, timeout, [&] {
            it = tunnel_map.find(id);
            if (status == CANCELLED)
            {
                cancelled = true;
                return true;
            }
            return it != tunnel_map.end();
        });
        if (cancelled)
            err_msg = "can't find tunnel ( " + toString(request->sender_meta().task_id()) + " + "
                + toString(request->receiver_meta().task_id()) + " because the task is cancelled";
        if (!ret)
            err_msg = "can't find tunnel ( " + toString(request->sender_meta().task_id()) + " + "
                + toString(request->receiver_meta().task_id()) + " ) within " + toString(timeout.count()) + " s";
        return (ret && !cancelled) ? it->second : nullptr;
    }
    ~MPPTask()
    {
        /// MPPTask maybe destructed by different thread, set the query memory_tracker
        /// to current_memory_tracker in the destructor
        current_memory_tracker = memory_tracker;
        closeAllTunnel("");
        LOG_DEBUG(log, "finish MPPTask: " << id.toString());
    }
private:
    MPPTask(const mpp::TaskMeta & meta_, const Context & context_)
        : context(context_), meta(meta_), log(&Logger::get("task " + std::to_string(meta_.task_id())))
    {
        id.start_ts = meta.start_ts();
        id.task_id = meta.task_id();
    }

    void runImpl();


    Context context;

    std::unique_ptr<tipb::DAGRequest> dag_req;
    std::unique_ptr<DAGContext> dag_context;

    /// store io in MPPTask to keep the life cycle of memory_tracker for the current query
    /// BlockIO contains some information stored in Context and DAGContext, so need deconstruct it before Context and DAGContext
    BlockIO io;
    MemoryTracker * memory_tracker = nullptr;

    MPPTaskId id;

    MPPTaskProgress task_progress;
    std::atomic<Int32> status{INITIALIZING};

    mpp::TaskMeta meta;

    // which targeted task we should send data by which tunnel.
    std::map<MPPTaskId, MPPTunnelPtr> tunnel_map;

    MPPTaskManager * manager = nullptr;

    Logger * log;

    Exception err;

    std::condition_variable cv;

    std::mutex tunnel_mutex;
};

using MPPTaskPtr = std::shared_ptr<MPPTask>;

using MPPTaskMap = std::map<MPPTaskId, MPPTaskPtr>;


} // namespace DB

