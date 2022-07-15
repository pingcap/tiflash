#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/ReadThread/CPU.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>

#include <ext/scope_guard.h>

namespace DB::DM
{
class SegmentReader
{
    inline static const std::string name{"SegmentReader"};

public:
    SegmentReader(WorkQueue<MergedTaskPtr> & task_queue_, const std::vector<int> & cpus_)
        : task_queue(task_queue_)
        , stop(false)
        , log(&Poco::Logger::get(name))
        , cpus(cpus_)
    {
        t = std::thread(&SegmentReader::run, this);
    }

    void setStop()
    {
        stop.store(true, std::memory_order_relaxed);
    }

    ~SegmentReader()
    {
        LOG_FMT_DEBUG(log, "SegmentReader stop begin");
        t.join();
        LOG_FMT_DEBUG(log, "SegmentReader stop end");
    }

private:
    void setCPUAffinity()
    {
#ifdef __linux__
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        for (int i : cpus)
        {
            CPU_SET(i, &cpu_set);
        }
        int ret = sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
        if (ret != 0)
        {
            LOG_FMT_ERROR(log, "sched_setaffinity fail: {}", std::strerror(errno));
            throw Exception(fmt::format("sched_setaffinity fail: {}", std::strerror(errno)));
        }
        LOG_FMT_DEBUG(log, "sched_setaffinity cpus {} succ", cpus);
#endif
    }

    bool isStop()
    {
        return stop.load(std::memory_order_relaxed);
    }

    void readSegments()
    {
        MergedTaskPtr merged_task;
        try
        {
            if (!task_queue.pop(merged_task))
            {
                LOG_FMT_INFO(log, "pop fail, stop {}", isStop());
                return;
            }

            SCOPE_EXIT({
                if (!merged_task->allStreamsFinished())
                {
                    SegmentReadTaskScheduler::instance().pushMergedTask(merged_task);
                }
            });

            int read_count = 0;
            while (!merged_task->allStreamsFinished() && !isStop())
            {
                auto c = merged_task->readBlock();
                read_count += c;
                if (c <= 0)
                {
                    break;
                }
            }
            if (read_count <= 0)
            {
                LOG_FMT_DEBUG(log, "pool {} seg_id {} read_count {}", merged_task->getPoolIds(), merged_task->getSegmentId(), read_count);
            }
        }
        catch (DB::Exception & e)
        {
            LOG_FMT_ERROR(log, "ErrMsg: {} StackTrace {}", e.message(), e.getStackTrace().toString());
            if (merged_task != nullptr)
            {
                merged_task->setException(e);
            }
        }
        catch (std::exception & e)
        {
            LOG_FMT_ERROR(log, "ErrMsg: {}", e.what());
            if (merged_task != nullptr)
            {
                merged_task->setException(DB::Exception(e.what()));
            }
        }
        catch (...)
        {
            tryLogCurrentException("exception thrown in SegmentReader");
            if (merged_task != nullptr)
            {
                merged_task->setException(DB::Exception("unknown exception thrown in SegmentReader"));
            }
        }
    }

    void run()
    {
        setCPUAffinity();
        setThreadName(name.c_str());
        while (!isStop())
        {
            readSegments();
        }
    }

    WorkQueue<MergedTaskPtr> & task_queue;
    std::atomic<bool> stop;
    Poco::Logger * log;
    std::thread t;
    std::vector<int> cpus;
};

void SegmentReaderPool::addTask(MergedTaskPtr && task)
{
    if (!task_queue.push(std::forward<MergedTaskPtr>(task), nullptr))
    {
        throw Exception("addTask fail");
    }
}

SegmentReaderPool::SegmentReaderPool(int thread_count, const std::vector<int> & cpus)
    : log(&Poco::Logger::get("SegmentReaderPool"))
{
    LOG_FMT_INFO(log, "Create SegmentReaderPool thread_count {} cpus {} start", thread_count, cpus);
    for (int i = 0; i < thread_count; i++)
    {
        readers.push_back(std::make_unique<SegmentReader>(task_queue, cpus));
    }
    LOG_FMT_INFO(log, "Create SegmentReaderPool thread_count {} cpus {} end", thread_count, cpus);
}

SegmentReaderPool::~SegmentReaderPool()
{
    for (auto & reader : readers)
    {
        reader->setStop();
    }
    task_queue.finish();
}

SegmentReaderPoolManager::SegmentReaderPoolManager()
    : log(&Poco::Logger::get("SegmentReaderPoolManager"))
{
    init();
}

SegmentReaderPoolManager::~SegmentReaderPoolManager() = default;

void SegmentReaderPoolManager::init()
{
    auto numa_nodes = getNumaNodes();
    int threads_per_node = numa_nodes.front().size();
    LOG_FMT_DEBUG(log, "SegmentReaderPoolManager::init threads_per_node {} numa_nodes {}", threads_per_node, numa_nodes.size());
    for (const auto & node : numa_nodes)
    {
        reader_pools.push_back(std::make_unique<SegmentReaderPool>(threads_per_node, node));
    }
}

void SegmentReaderPoolManager::addTask(MergedTaskPtr && task)
{
    static std::hash<uint64_t> hash_func;
    auto idx = hash_func(task->getSegmentId()) % reader_pools.size();
    reader_pools[idx]->addTask(std::move(task));
}

} // namespace DB::DM