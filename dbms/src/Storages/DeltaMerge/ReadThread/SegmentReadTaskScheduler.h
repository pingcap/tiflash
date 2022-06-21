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

    WeakPtrList() : last_itr(l.end()) {}

    void add(Element ptr)
    {
        l.push_back(ptr);
    }

    ElementPtr next()
    {
        for (last_itr = nextItr(last_itr); !l.empty(); last_itr = nextItr(last_itr))
        {
            auto element = last_itr->lock();
            if (element == nullptr)
            {
                last_itr = l.erase(last_itr);
            }
            else  
            {
                return element;
            }
        }
        return nullptr;
    }
    
    // <unexpired_count, expired_count>
    std::pair<int64_t, int64_t> count() const
    {
        int64_t expired_count = 0;
        for (const auto & wp : l)
        {
            expired_count += static_cast<int>(wp.expired());
        }
        return {l.size() - expired_count, expired_count};
    }

    template <typename U>
    ElementPtr get(U pred)
    {
        for (const auto & wp : l)
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
        if (itr == l.end() || std::next(itr) == l.end())
        {
            return l.begin();
        }
        else 
        {
            return std::next(itr);
        }
    }

    ElementList l;
    ElementIter last_itr;
};

using Task = std::pair<BlockInputStreamPtr, std::weak_ptr<SegmentReadTaskPool>>;
struct MergedTask
{
    MergedTask(uint64_t seg_id_, std::vector<Task> && tasks_) 
        : seg_id(seg_id_)
        , tasks(std::forward<std::vector<Task>>(tasks_)) {}
    uint64_t seg_id;
    std::vector<Task> tasks;
};
using MergedTaskPtr = std::shared_ptr<MergedTask>;

class SegmentReadTaskScheduler : private boost::noncopyable
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    ~SegmentReadTaskScheduler();
    void add(SegmentReadTaskPoolPtr & pool);
    MergedTaskPtr getMergedTask();
private:
    SegmentReadTaskScheduler();
    void setStop();
    bool isStop() const;
    bool schedule();
    void scheThread();

    SegmentReadTaskPools unsafeGetPools(const std::vector<uint64_t> & pool_ids);
    std::pair<uint64_t, std::vector<uint64_t>> unsafeScheduleSegment(const SegmentReadTaskPoolPtr & pool);
    SegmentReadTaskPoolPtr unsafeScheduleSegmentReadTaskPool();

    std::mutex mtx;
    WeakPtrList<SegmentReadTaskPool> read_pools;
    int64_t max_unexpired_pool_count;
    // seg_id -> pool_ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> segments;

    std::atomic<bool> stop;
    std::thread sche_thread;

    Poco::Logger * log;
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