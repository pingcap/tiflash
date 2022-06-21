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
    
    // <unexpired_count, expired_count>
    std::pair<int64_t, int64_t> count() const
    {
        int64_t expired_count = 0;
        for (const auto & wp : read_pools)
        {
            expired_count += static_cast<int>(wp.expired());
        }
        return {read_pools.size() - expired_count, expired_count};
    }

    template <typename U>
    ElementPtr get(U pred)
    {
        for (const auto & wp : read_pools)
        {
            auto sp = wp.lock();
            if (sp != nullptr && pred(sp))
            {
                return sp;
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


    using Task = std::pair<BlockInputStreamPtr, std::weak_ptr<SegmentReadTaskPool>>;
    struct SharingTask
    {
        uint64_t seg_id;
        std::vector<Task> tasks;
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

    SharingTask getSharingTask();
    SegmentReadTaskPools getPools(const std::vector<uint64_t> & pool_ids);
private:
    std::pair<uint64_t, SegmentReadTaskPools> getSegment();
    SegmentReadTaskPools unsafeGetPools(const std::vector<uint64_t> & pool_ids);
    std::pair<uint64_t, std::vector<uint64_t>> unsafeScheduleSegment(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPoolPtr unsafeScheduleSegmentReadTaskPool();

    std::mutex mtx;
    WeakPtrList<SegmentReadTaskPool> read_pools;
    int64_t max_unexpired_pool_count;

    // seg_id -> pool_ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> segments;

    Poco::Logger * log;

    SegmentReadTaskScheduler() : max_unexpired_pool_count(0), log(&Poco::Logger::get("SegmentReadTaskScheduler")) {}
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