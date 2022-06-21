#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>

namespace DB::DM
{
SegmentReadTaskScheduler::SegmentReadTaskScheduler() 
    : max_unexpired_pool_count(0)
    , stop(false)
    , log(&Poco::Logger::get("SegmentReadTaskScheduler")) 
{
    sche_thread = std::thread(&SegmentReadTaskScheduler::scheThread, this);
}

SegmentReadTaskScheduler::~SegmentReadTaskScheduler()
{
    setStop();
    sche_thread.join();
}

void SegmentReadTaskScheduler::add(SegmentReadTaskPoolPtr & pool)
{
    std::lock_guard lock(mtx);
    
    read_pools.add(std::weak_ptr<SegmentReadTaskPool>(pool));

    std::vector<uint64_t> seg_ids;
    for (const auto & task : pool->getTasks())
    {
        segments[task->segment->segmentId()].push_back(pool->getId());
        seg_ids.push_back(task->segment->segmentId());
    }

    auto [unexpired, expired] = read_pools.count();
    LOG_FMT_DEBUG(log, "add pool {} segment count {} segments {} unexpired pool {} expired pool {}",
        pool->getId(), seg_ids.size(), seg_ids, unexpired, expired);
    max_unexpired_pool_count = unexpired;
}

MergedTaskPtr SegmentReadTaskScheduler::getMergedTask()
{
    uint64_t seg_id = 0;
    SegmentReadTaskPools pools;
    {
        std::lock_guard lock(mtx);
        auto pool = unsafeScheduleSegmentReadTaskPool();
        if (pool == nullptr)
        {
            return {};
        }
        auto segment = unsafeScheduleSegment(pool);
        if (segment.first == 0)
        {
            return {};
        }
        seg_id = segment.first;
        pools = unsafeGetPools(segment.second);
    }
    
    std::vector<Task> tasks;
    tasks.reserve(pools.size());
    for (auto & pool : pools)
    {
        if (pool == nullptr)
        {
            continue;
        }
        tasks.push_back({pool->getInputStream(seg_id), std::weak_ptr<SegmentReadTaskPool>(pool)});
    }
    if (tasks.empty())
    {
        return {};
    }
    return std::make_shared<MergedTask>(seg_id, std::move(tasks));
}

SegmentReadTaskPools SegmentReadTaskScheduler::unsafeGetPools(const std::vector<uint64_t> & pool_ids)
{
    SegmentReadTaskPools pools;
    pools.reserve(pool_ids.size());
    for (uint64_t id : pool_ids)
    {
        auto sp = read_pools.get([id](const SegmentReadTaskPoolPtr & sp) { return sp->getId() == id; });
        pools.push_back(sp);
    }
    return pools;
}

SegmentReadTaskPoolPtr SegmentReadTaskScheduler::unsafeScheduleSegmentReadTaskPool()
{
    auto [unexpired, expired] = read_pools.count();
    LOG_FMT_DEBUG(log, "unsafeScheduleSegmentReadTaskPool unexpired pool {} expired pool {}", unexpired, expired);
    max_unexpired_pool_count = unexpired;
    for (int64_t i = 0; i < unexpired; i++)
    {
        auto pool = read_pools.next();
        if (pool == nullptr)
        {
            return nullptr;
        }
        if (pool->pendingBlockCount() < 20)
        {
            return pool;
        }
    }
    return nullptr;
}

std::pair<uint64_t, std::vector<uint64_t>> SegmentReadTaskScheduler::unsafeScheduleSegment(const SegmentReadTaskPoolPtr & pool)
{
    auto expected_merge_seg_count = std::min(max_unexpired_pool_count, 2);
    auto target = pool->scheduleSegment(segments, expected_merge_seg_count);
    if (target.first > 0)
    {
        segments.erase(target.first);
    }
    return target;
}

void SegmentReadTaskScheduler::setStop()
{
    stop.store(true, std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::isStop() const
{
    return stop.load(std::memory_order_relaxed);
}

bool SegmentReadTaskScheduler::schedule()
{
    auto merged_task = getMergedTask();
    if (merged_task == nullptr)
    {
        return false;
    }
    SegmentReadThreadPool::instance().addTask(merged_task);  // TODO(jinhelin): should not be fail.
    return true;
}

void SegmentReadTaskScheduler::scheThread()
{
    while (!isStop())
    {
        if (!schedule())
        {
            ::usleep(2000);
        }
    }
}

DMFileReaderPool & DMFileReaderPool::instance()
{
    static DMFileReaderPool reader_pool;
    return reader_pool;
}

void DMFileReaderPool::add(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    readers[reader.fileId()].insert(&reader);
}

void DMFileReaderPool::del(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(reader.fileId());
    if (itr == readers.end())
    {
        return;
    }
    itr->second.erase(&reader);
    if (itr->second.empty())
    {
        readers.erase(itr);
    }
}

void DMFileReaderPool::set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(from_reader.fileId());
    if (itr == readers.end())
    {
        return;
    }
    for (auto * r : itr->second)
    {
        if (&from_reader == r)
        {
            continue;
        }
        r->addCachedPacks(col_id, start, count, col);
    }
}
}