#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
namespace DB::DM
{

template <typename T>
class WeakPtrList
{
public:
    using Element = std::weak_ptr<T>;
    using ElementPtr = std::shared_ptr<T>;
    using ElementList = std::list<Element>;
    using ElementIter = typename ElementList::iterator;

    WeakPtrList() : last_itr(read_pools.end()) {}

    void add(Element ptr)
    {
        read_pools.push_back(ptr);
    }

    ElementPtr next()
    {
        for (last_itr = nextItr(last_itr); !read_pools.empty(); last_itr = nextItr(last_itr))
        {
            auto pool = last_itr->lock();
            if (pool == nullptr)
            {
                last_itr = read_pools.erase(last_itr);
            }
            else  
            {
                return pool;
            }
        }
        return nullptr;
    }
private:
    
    ElementIter nextItr(ElementIter itr)
    {
        if (itr == read_pools.end() || std::next(itr) == read_pools.end())
        {
            return read_pools.begin();
        }
        else 
        {
            return std::next(itr);
        }
    }

    ElementList read_pools;
    ElementIter last_itr;
};

class SegmentReadTaskScheduler : private boost::noncopyable
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    void add(SegmentReadTaskPoolPtr & pool);

    std::pair<uint64_t, std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>>> getInputStreams();
    
private:
    std::pair<uint64_t, SegmentReadTaskPools> getSegment();
    SegmentReadTaskPools unsafeGetPools(const std::vector<uint64_t> & pool_ids);
    std::mutex mtx;
    
    WeakPtrList<SegmentReadTaskPool> read_pools;

    std::list<std::weak_ptr<SegmentReadTaskPool>>::iterator next_read_pool;
    std::list<std::weak_ptr<SegmentReadTaskPool>>::iterator initReadPool()
  

    // seg_id -> pool_ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> segments;

    Poco::Logger * log;

    SegmentReadTaskScheduler() : next_read_pool(0), log(&Poco::Logger::get("SegmentReadTaskScheduler")) {}
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