#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <limits>
#include <memory>
#include <utility>
#include <vector>
#include "Debug/DBGInvoker.h"

namespace DB::DM
{

class SegmentReadTaskPoolList
{
public:
    SegmentReadTaskPoolList() : last_itr(l.end()) {}

    void add(const SegmentReadTaskPoolPtr & ptr)
    {
        l.push_back(ptr);
    }

    SegmentReadTaskPoolPtr next()
    {
        for (last_itr = nextItr(last_itr); !l.empty(); last_itr = nextItr(last_itr))
        {
            auto ptr = *last_itr;
            if (ptr->expired())
            {
                last_itr = l.erase(last_itr);
            }
            else  
            {
                return ptr;
            }
        }
        return nullptr;
    }
    
    // <unexpired_count, expired_count>
    std::pair<int64_t, int64_t> count() const
    {
        int64_t expired_count = 0;
        for (const auto & p : l)
        {
            expired_count += static_cast<int>(p->expired());
        }
        return {l.size() - expired_count, expired_count};
    }

    SegmentReadTaskPoolPtr get(uint64_t pool_id) const
    {
        for (const auto & p : l)
        {
            if (p->getId() == pool_id)
            {
                return p;
            }
        }
        return nullptr;
    }
private:
    
    std::list<SegmentReadTaskPoolPtr>::iterator nextItr(std::list<SegmentReadTaskPoolPtr>::iterator itr)
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

    std::list<SegmentReadTaskPoolPtr> l;
    std::list<SegmentReadTaskPoolPtr>::iterator last_itr;
};

class MergedTask
{
public:
    MergedTask(uint64_t seg_id_, SegmentReadTaskPools && pools_) 
        : seg_id(seg_id_)
        , pools(std::forward<SegmentReadTaskPools>(pools_))
        , finished_count(0)
        , finished(pools.size(), 0) {}
 
    BlockInputStreams init()
    {
        if (!streams.empty())
        {
            return streams;
        }
        streams.resize(pools.size(), nullptr);
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (pools[i]->expired())
            {
                pools[i].reset();
                setFinished(i);
            }
            else 
            {
                streams[i] = pools[i]->getInputStream(seg_id);
            }
        }
        return streams;
    }

    void readOneBlock()
    {
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (isFinished(i))
            {
                continue;
            }

            auto & pool = pools[i];

            if (pool->expired())
            {
                pool.reset();
                setFinished(i);
                continue;
            }

            auto block = streams[i]->read();
            if (!block)
            {
                setFinished(i);
                pool->finishSegment(seg_id);
            }
            else
            {
                pool->pushBlock(std::move(block));
            }
        }
    }

    bool allFinished()
    {
        return finished_count >= finished.size();
    }

    std::pair<int64_t, int64_t> getMinMaxPendingBlockCount()
    {
        int64_t min = std::numeric_limits<int64_t>::max();
        int64_t max = std::numeric_limits<int64_t>::min();
        for (size_t i = 0; i < pools.size(); i++)
        {
            if (isFinished(i))
            {
                continue;
            }
            if (pools[i]->expired())
            {
                setFinished(i);
                continue;
            }
          
            auto pbc = pools[i]->pendingBlockCount();
            min = std::min(min, pbc);
            max = std::max(max, pbc);
        }
        return {min, max};
    }

    uint64_t seg_id;
    SegmentReadTaskPools pools;
    BlockInputStreams streams;

     bool isFinished(size_t i)
    {
        return finished[i];
    }
    void setFinished(size_t i)
    {
        if (!isFinished(i))
        {
            finished[i] = 1;
            finished_count++;
        }
    }
    size_t finished_count;
    std::vector<int8_t> finished;
};
using MergedTaskPtr = std::shared_ptr<MergedTask>;

class SegmentReadTaskScheduler
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    ~SegmentReadTaskScheduler();
    SegmentReadTaskScheduler(const SegmentReadTaskScheduler &) = delete;
    SegmentReadTaskScheduler & operator=(const SegmentReadTaskScheduler &) = delete;
    SegmentReadTaskScheduler(SegmentReadTaskScheduler &&) = delete;
    SegmentReadTaskScheduler & operator=(SegmentReadTaskScheduler &&) = delete;

    void add(const SegmentReadTaskPoolPtr & pool);
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
    SegmentReadTaskPoolList read_pools;
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