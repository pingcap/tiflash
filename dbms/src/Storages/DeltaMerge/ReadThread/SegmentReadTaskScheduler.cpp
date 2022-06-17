#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Segment.h>
#include <algorithm>
#include <limits>
namespace DB::DM
{
void SegmentReadTaskScheduler::add(SegmentReadTaskPoolPtr & pool)
{
    std::lock_guard lock(mtx);
    read_pools.push_back(pool);
    std::vector<UInt64> seg_ids;
    for (const auto & task : pool->getTasks())
    {
        segments[task->segment->segmentId()].push_back(pool->getId());
        seg_ids.push_back(task->segment->segmentId());
    }
    LOG_FMT_DEBUG(log, "add pool {} segments {}", pool->getId(), seg_ids.size(), seg_ids);
}

void SegmentReadTaskScheduler::del(UInt64 pool_id)
{
    std::lock_guard lock(mtx);
    auto itr = std::find_if(read_pools.begin(), read_pools.end(), [pool_id](const SegmentReadTaskPoolPtr & pool) { return pool->getId() == pool_id; });
    if (itr == read_pools.end())
    {
        throw Exception(fmt::format("del pool {} not found", pool_id));
    }
    read_pools.erase(itr);
    LOG_FMT_DEBUG(log, "del pool {}", pool_id);
}

std::pair<UInt64, std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>>> SegmentReadTaskScheduler::getInputStreams()
{
    auto [seg_id, segment_pools] = getSegment();
    std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>> streams;
    streams.reserve(segment_pools.size());
    for (auto & pool : segment_pools)
    {
        streams.push_back({pool->getInputStream(seg_id), pool});
    }
    return std::pair{seg_id, streams};
}

// TODO(jinhelin): use config or auto
constexpr int64_t MAX_PENDING_BLOCK_COUNT = 80;
constexpr int64_t MAX_ACTIVE_SEGMENT_COUNT = 40;

std::pair<uint64_t, SegmentReadTaskPools> SegmentReadTaskScheduler::getSegment()
{
    std::lock_guard lock(mtx);
    auto target = segments.end();
    SegmentReadTaskPools target_pools;
    for (auto itr = segments.begin(); itr != segments.end(); ++itr)
    {
        auto min_pending_block_count = std::numeric_limits<int64_t>::max();
        auto min_active_segment_count = std::numeric_limits<int64_t>::max();
        auto pools = unsafeGetPools(itr->second);
        for (const auto & pool : pools)
        {
            min_pending_block_count = std::min(pool->pendingBlockCount(), min_pending_block_count);
            min_active_segment_count = std::min(pool->activeSegmentCount(), min_active_segment_count);
        }
        if (min_active_segment_count >= MAX_ACTIVE_SEGMENT_COUNT
            || min_pending_block_count >= MAX_PENDING_BLOCK_COUNT)
        {
            LOG_FMT_DEBUG(log, "seg {} pool {} min_active_segment {} min_pending_block {} no need read",
                itr->first, itr->second, min_active_segment_count, min_pending_block_count);
            continue;
        }
        if (target == segments.end() || itr->second.size() > target->second.size())
        {
            target = itr;
            target_pools = pools;
        }
    }
    if (target == segments.end())
    {
        LOG_FMT_DEBUG(log, "no target segment, pending segments {} pending pools {}",
            segments.size(), read_pools.size());
        return {};
    }
    auto target_seg_id = target->first;
    segments.erase(target);
    LOG_FMT_DEBUG(log, "target segment {} pool {} pending segments {} pending pools {}",
        target_seg_id, target_pools.size(), segments.size(), read_pools.size());
    return {target_seg_id, target_pools};
}

SegmentReadTaskPools SegmentReadTaskScheduler::unsafeGetPools(const std::vector<uint64_t> & pool_ids)
{
    SegmentReadTaskPools result;
    result.reserve(pool_ids.size());
    for (uint64_t id : pool_ids)
    {
        auto itr = std::find_if(read_pools.begin(), read_pools.end(), [id](const SegmentReadTaskPoolPtr & pool) { return pool->getId() == id; });
        if (itr == read_pools.end())
        {
            throw Exception(fmt::format("unsafeGetPools pool {} not found", id));
        }
        result.push_back(*itr);
    }
    return result;
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