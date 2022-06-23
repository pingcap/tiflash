#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include "Debug/DBGInvoker.h"
#include "common/logger_useful.h"
#include <Storages/DeltaMerge/ReadThread/CPU.h>
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
        if (!task_queue.pop(merged_task))
        {
            LOG_FMT_INFO(log, "pop fail, stop {}", isStop());
            return;
        }

        int64_t sleep_times = 0;
        int64_t read_block_count = 0;
        merged_task->init();
        while (!merged_task->allFinished() && !isStop())
        {
            auto [min_pending_block_count, max_pending_block_count] = merged_task->getMinMaxPendingBlockCount();
            constexpr int64_t pending_block_count_limit = 100;
            auto read_count = pending_block_count_limit - max_pending_block_count;  // TODO(jinhelin) max or min or ...
            if (read_count <= 0)
            {
                sleep_times++;
                ::usleep(1000);  // TODO(jinhelin): back to MergedTaskPool
                continue;
            }
            for (int c = 0; c < 2; c++)
            {
                read_block_count++;
                merged_task->readOneBlock();
            }
        }
        LOG_FMT_DEBUG(log, "seg_id {} sleep_times {} read_block_count {}", merged_task->seg_id, sleep_times, read_block_count);
    }

    void run()
    {
        setCPUAffinity();
        setThreadName(name.c_str());
        while (!isStop())
        {
            try
            {
                readSegments();  // TODO(jinhelin): how to send exception to upper threads?
            }
            catch (Exception & e)
            {
                LOG_FMT_ERROR(log, "ErrMsg: {}", e.message());
            }
            catch (std::exception & e)
            {
                LOG_FMT_ERROR(log, "ErrMsg: {}", e.what());
            }
            catch (...)
            {
                tryLogCurrentException("exception thrown in SegmentReader::readSegments");
            }
        }
    }

    WorkQueue<MergedTaskPtr> & task_queue;
    std::atomic<bool> stop;
    Poco::Logger * log;
    std::thread t;
    std::vector<int> cpus;
}; 

void SegmentReadThreadPool::init(int thread_count)
{
    LOG_FMT_INFO(log, "thread_count {} start", thread_count);
    auto numa_nodes = getNumaNodes();
    for (int i = 0; i < thread_count; i++)
    {
        readers.push_back(std::make_unique<SegmentReader>(task_queue, numa_nodes[i % numa_nodes.size()]));
    }
    LOG_FMT_INFO(log, "thread_count {} end", thread_count);
}

bool SegmentReadThreadPool::addTask(MergedTaskPtr && task)
{
    return task_queue.push(std::forward<MergedTaskPtr>(task));
}
    
SegmentReadThreadPool::SegmentReadThreadPool(int thread_count)
    : log(&Poco::Logger::get("SegmentReadThreadPool"))
{
    init(thread_count);
}

SegmentReadThreadPool::~SegmentReadThreadPool()
{
    for (auto & reader : readers)
    {
        reader->setStop();
    }
    task_queue.finish();
}

}