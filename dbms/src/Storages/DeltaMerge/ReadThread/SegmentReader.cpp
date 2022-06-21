#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>

namespace DB::DM
{
class SegmentReader
{
inline static const std::string name{"SegmentReader"};
public:
    SegmentReader(WorkQueue<MergedTaskPtr> & task_queue_)
        : task_queue(task_queue_)
        , stop(false)
        , log(&Poco::Logger::get(name))
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

        auto seg_id = merged_task->seg_id;
        auto & tasks = merged_task->tasks;
        std::vector<int> dones(tasks.size(), 0);
        size_t done_count = 0;
        while (done_count < tasks.size())
        {
            auto min_pending_block_count = std::numeric_limits<int64_t>::max();
            auto max_pending_block_count = std::numeric_limits<int64_t>::min();
            std::vector<std::pair<uint64_t, int64_t>> pendings;
            SegmentReadTaskPools pools(tasks.size(), nullptr);
            for (size_t i = 0; i < tasks.size(); i++)
            {
                pools[i] = tasks[i].second.lock();
                const auto &  pool = pools[i];
                if (pool == nullptr)
                {
                    done_count++;
                    dones[i] = 1;
                }
                else
                {
                    auto pending_count = pool->pendingBlockCount();
                    pendings.emplace_back(pool->getId(), pending_count);
                    min_pending_block_count = std::min(pending_count, min_pending_block_count);
                    max_pending_block_count = std::max(pending_count, max_pending_block_count);
                }
            }
            if (done_count >= tasks.size() || isStop())
            {
                break;
            }
            if (max_pending_block_count > 200)
            {
                LOG_FMT_DEBUG(log, "min_pending {} max_pending {} pendings {}", 
                    min_pending_block_count, max_pending_block_count, pendings);
            }
            constexpr int64_t pending_block_count_limit = 100;
            auto read_count = pending_block_count_limit - max_pending_block_count;  // max or min or ...
            if (read_count <= 0)
            {
                pools.clear();
                ::usleep(5000);  // TODO(jinhelin)
                continue;
            }
            //for (int64_t i = 0; i < read_count; i++)
            {
                for (size_t i = 0; i < tasks.size(); i++)
                {
                    if (dones[i])
                    {
                        continue;
                    }
                    auto & t = tasks[i];
                    auto block = t.first->read();
                    if (!block)
                    {
                        pools[i]->finishSegment(seg_id);
                        dones[i] = 1;
                        done_count++;
                    }
                    else
                    {
                        pools[i]->pushBlock(std::move(block));
                    }
                }
            }
        }
    }

    void run()
    {
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
}; 

bool SegmentReadThreadPool::addTask(MergedTaskPtr && task)
{
    return task_queue.push(std::forward<MergedTaskPtr>(task));
}
    
SegmentReadThreadPool::SegmentReadThreadPool(int thread_count)
    : log(&Poco::Logger::get("SegmentReadThreadPool"))
{
    LOG_FMT_INFO(log, "thread_count {} start", thread_count);
    for (int i = 0; i < thread_count; i++)
    {
        readers.push_back(std::make_unique<SegmentReader>(task_queue));
    }
    LOG_FMT_INFO(log, "thread_count {} end", thread_count);
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