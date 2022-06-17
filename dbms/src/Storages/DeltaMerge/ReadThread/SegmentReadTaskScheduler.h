#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
namespace DB::DM
{

class SegmentReadTaskScheduler : private boost::noncopyable
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    void add(SegmentReadTaskPoolPtr & pool);
    void del(uint64_t pool_id);

    std::pair<uint64_t, std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>>> getInputStreams();
    
private:
    std::pair<uint64_t, SegmentReadTaskPools> getSegment();
    SegmentReadTaskPools unsafeGetPools(const std::vector<uint64_t> & pool_ids);
    std::mutex mtx;
    // pool_id -> pool
    // uint64_t next_pool_id;
    std::list<SegmentReadTaskPoolPtr> read_pools;
    // seg_id -> pool_ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> segments;

    Poco::Logger * log;

    SegmentReadTaskScheduler() : log(&Poco::Logger::get("SegmentReadTaskScheduler")) {}
};

class DMFileReader;
class DMFileReaderPool
{
public:
    static DMFileReaderPool & instance();

    void add(DMFileReader & reader);
    void del(DMFileReader & reader);
    void set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col);
private:
    std::mutex mtx;
    std::unordered_map<uint64_t, std::unordered_set<DMFileReader*>> readers;
};
}