#pragma once

#include <mutex>
#include <string>

#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/RateLimiter.h>

namespace DB
{

namespace DM
{

class DeltaMergeTaskPool
{
public:
    enum TaskType
    {
        Split,
        Merge,
        MergeDelta,
        Compact,
        Flush,
        PlaceIndex,
    };

    static std::string TaskToString(TaskType type)
    {
        switch (type)
        {
        case Split:
            return "Split";
        case Merge:
            return "Merge";
        case MergeDelta:
            return "MergeDelta";
        case Compact:
            return "Compact";
        case Flush:
            return "Flush";
        case PlaceIndex:
            return "PlaceIndex";
        default:
            return "Unknown";
        }
    }

    struct BackgroundTask
    {
        BackgroundTask(TaskType                   type_,
                       DeltaMergeTaskPoolHandle   handle_,
                       const DMContextPtr &       dm_context_,
                       const DeltaMergeStorePtr & store_,
                       const SegmentPtr &         segment_,
                       const SegmentPtr &         next_segment_)
            : type{type_}, handle{handle_}, dm_context{dm_context_}, store{store_}, segment{segment_}, next_segment{next_segment_}
        {
        }

        BackgroundTask(const BackgroundTask & task) noexcept
            : type{task.type},
              handle{task.handle},
              dm_context{task.dm_context},
              store{task.store},
              segment{task.segment},
              next_segment{task.next_segment},
              snapshot{task.snapshot},
              next_snapshot{task.next_snapshot},
              task_size{task.task_size}
        {
        }

        TaskType type;

        DeltaMergeTaskPoolHandle handle;

        DMContextPtr       dm_context;
        DeltaMergeStorePtr store;
        SegmentPtr         segment;
        SegmentPtr         next_segment;
        SegmentSnapshotPtr snapshot;
        SegmentSnapshotPtr next_snapshot;

        Int64 task_size = -1;
        // guard `finished` field
        std::mutex task_mutex;
        bool       finished = false;

        explicit operator bool() { return (bool)segment; }
    };

    using BackgroundTaskHandle = std::shared_ptr<BackgroundTask>;

public:
    explicit DeltaMergeTaskPool(Context & db_context);

    ~DeltaMergeTaskPool();

    DeltaMergeTaskPoolHandle registerStore();

    using ThreadType = DeltaMergeStore::ThreadType;
    void addTask(const BackgroundTask & task, const ThreadType & whom);

    void removeAllTasksForStore(DeltaMergeTaskPoolHandle handle, const String & database_name, const String & table_name);

    bool handleBackgroundTask();

    void wake()
    {
        bool need_awake_background_pool = false;
        {
            std::scoped_lock lock{mutex};
            need_awake_background_pool = processing_tasks.empty();
        }
        if (need_awake_background_pool)
            background_task_handle->wake();
    }

    size_t getTaskNumForStore(DeltaMergeTaskPoolHandle handle);

    // a hint to suggest not add too many tasks to task pool
    size_t getAdviseMaxTaskNumForEveryStore() { return background_pool.getNumberOfThreads() * 3; }

private:
    bool handleTaskImpl(bool high_priority);

    BackgroundTaskHandle nextTask(bool high_priority);

    void addTaskToHighPriorityQueue(BackgroundTaskHandle & task, bool front);

    void removeTaskFromProcessingQueue(BackgroundTaskHandle & task);

private:
    Context & global_context;

    using TaskQueue = std::deque<BackgroundTaskHandle>;
    TaskQueue high_priority_tasks;
    TaskQueue low_priority_tasks;

    using TaskSet = std::unordered_set<BackgroundTaskHandle>;
    TaskSet processing_tasks;

    std::unordered_map<DeltaMergeTaskPoolHandle, size_t> task_counts;

    BackgroundProcessingPool &           background_pool;
    BackgroundProcessingPool::TaskHandle background_task_handle;

    RateLimiterPtr rate_limiter;

    std::atomic<UInt64> next_store_id;

    // we can just handling task from high priority queue one at a time to avoid create too many snapshot
    std::atomic<bool> handling_high_priority_task{false};

    Logger * log;

    std::mutex mutex;
};

} // namespace DM
} // namespace DB
