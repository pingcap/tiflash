// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>

#include <atomic>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "Common/Exception.h"
#include "Core/Block.h"
#include "Debug/DBGInvoker.h"
#include "Storages/DeltaMerge/DMContext.h"
#include "Storages/DeltaMerge/StableValueSpace.h"
#include "boost/core/noncopyable.hpp"
#include "common/logger_useful.h"
namespace DB
{
namespace DM
{
struct DMContext;
struct SegmentReadTask;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

using DMContextPtr = std::shared_ptr<DMContext>;
using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks = std::list<SegmentReadTaskPtr>;
using AfterSegmentRead = std::function<void(const DMContextPtr &, const SegmentPtr &)>;

template <typename T>
class WorkQueue
{
    // Protects all member variable access
    std::mutex mutex_;
    std::condition_variable readerCv_;
    std::condition_variable writerCv_;
    std::condition_variable finishCv_;
    std::queue<T> queue_;
    bool done_;
    std::size_t maxSize_;
    // Must have lock to call this function
    bool full() const
    {
        if (maxSize_ == 0)
        {
            return false;
        }
        return queue_.size() >= maxSize_;
    }

public:
    /**
   * Constructs an empty work queue with an optional max size.
   * If `maxSize == 0` the queue size is unbounded.
   *
   * @param maxSize The maximum allowed size of the work queue.
   */
    WorkQueue(std::size_t maxSize = 0)
        : done_(false)
        , maxSize_(maxSize)
    {}
    /**
   * Push an item onto the work queue.  Notify a single thread that work is
   * available.  If `finish()` has been called, do nothing and return false.
   * If `push()` returns false, then `item` has not been copied from.
   *
   * @param item  Item to push onto the queue.
   * @returns     True upon success, false if `finish()` has been called.  An
   *               item was pushed iff `push()` returns true.
   */
    template <typename U>
    bool push(U && item)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (full() && !done_)
            {
                writerCv_.wait(lock);
            }
            if (done_)
            {
                return false;
            }
            queue_.push(std::forward<U>(item));
        }
        readerCv_.notify_one();
        return true;
    }
    /**
   * Attempts to pop an item off the work queue.  It will block until data is
   * available or `finish()` has been called.
   *
   * @param[out] item  If `pop` returns `true`, it contains the popped item.
   *                    If `pop` returns `false`, it is unmodified.
   * @returns          True upon success.  False if the queue is empty and
   *                    `finish()` has been called.
   */
    bool pop(T & item)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            while (queue_.empty() && !done_)
            {
                readerCv_.wait(lock);
            }
            if (queue_.empty())
            {
                assert(done_);
                return false;
            }
            item = std::move(queue_.front());
            queue_.pop();
        }
        writerCv_.notify_one();
        return true;
    }
    /**
   * Sets the maximum queue size.  If `maxSize == 0` then it is unbounded.
   *
   * @param maxSize The new maximum queue size.
   */
    void setMaxSize(std::size_t maxSize)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            maxSize_ = maxSize;
        }
        writerCv_.notify_all();
    }
    /**
   * Promise that `push()` won't be called again, so once the queue is empty
   * there will never any more work.
   */
    void finish()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!done_);
            done_ = true;
        }
        readerCv_.notify_all();
        writerCv_.notify_all();
        finishCv_.notify_all();
    }
    /// Blocks until `finish()` has been called (but the queue may not be empty).
    void waitUntilFinished()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!done_)
        {
            finishCv_.wait(lock);
        }
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

struct SegmentReadTask
{
    SegmentPtr segment;
    SegmentSnapshotPtr read_snapshot;
    RowKeyRanges ranges;

    SegmentReadTask(const SegmentPtr & segment_, //
                    const SegmentSnapshotPtr & read_snapshot_,
                    const RowKeyRanges & ranges_);

    explicit SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshotPtr & read_snapshot_);

    ~SegmentReadTask();

    std::pair<size_t, size_t> getRowsAndBytes() const;

    void addRange(const RowKeyRange & range) { ranges.push_back(range); }

    void mergeRanges() { ranges = DM::tryMergeRanges(std::move(ranges), 1); }

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);
};

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    explicit SegmentReadTaskPool(const DMContextPtr & dm_context_, const ColumnDefines & columns_to_read_, const RSOperatorPtr & filter_, UInt64 max_version_, size_t expected_block_size_, bool is_raw_, bool do_range_filter_for_raw_, SegmentReadTasks && tasks_)
        : id(pool_id.fetch_add(1, std::memory_order_relaxed))
        , dm_context(dm_context_)
        , columns_to_read(columns_to_read_)
        , filter(filter_)
        , max_version(max_version_)
        , expected_block_size(expected_block_size_)
        , is_raw(is_raw_)
        , do_range_filter_for_raw(do_range_filter_for_raw_)
        , tasks(std::move(tasks_))
        , log(&Poco::Logger::get("SegmentReadTaskPool"))
    {}

    uint64_t getId() const { return id; }
    const SegmentReadTasks & getTasks() const { return tasks; }

    BlockInputStreamPtr getInputStream(UInt64 seg_id);
    void finishSegment(UInt64 seg_id);

    void pushBlock(Block && block) 
    { 
        pending_block_count.fetch_add(1, std::memory_order_relaxed);
        pending_block_size.fetch_add(block.bytes(), std::memory_order_relaxed);
        q.push(std::move(block)); 

        auto t = ::time(nullptr);
        auto l = last_print_time.load(std::memory_order_relaxed);
        if (t - l >= 1 && last_print_time.compare_exchange_strong(l, t))
        {
            LOG_FMT_DEBUG(log, "pending_block_count {} pending_block_size {} MB",
                pending_block_count.load(std::memory_order_relaxed), 
                pending_block_size.load(std::memory_order_relaxed) / 1000 / 1000);
        }
    }
    void popBlock(Block & block) 
    { 
        q.pop(block);
        if (block)
        {
            pending_block_count.fetch_sub(1, std::memory_order_relaxed);
            pending_block_size.fetch_sub(block.bytes(), std::memory_order_relaxed);
        }
    }

private:
    SegmentReadTaskPtr getTask(uint64_t seg_id);
    void activeSegment(UInt64 seg_id);

    const uint64_t id;
    DMContextPtr dm_context;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    const UInt64 max_version;
    const size_t expected_block_size;
    const bool is_raw;
    const bool do_range_filter_for_raw;

    std::mutex mutex;
    SegmentReadTasks tasks;
    std::unordered_set<UInt64> active_segment_ids;

    WorkQueue<Block> q;

    Poco::Logger * log;

    inline static std::atomic<uint64_t> pool_id{1};

    inline static std::atomic<int64_t> last_print_time{0};
    inline static std::atomic<int64_t> pending_block_count{0};
    inline static std::atomic<int64_t> pending_block_size{0};
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPools = std::vector<SegmentReadTaskPoolPtr>;

class SegmentReadTaskScheduler : private boost::noncopyable
{
public:
    static SegmentReadTaskScheduler & instance()
    {
        static SegmentReadTaskScheduler scheduler;
        return scheduler;
    }

    void add(SegmentReadTaskPoolPtr & pool);
    void del(UInt64 pool_id);

    std::pair< UInt64, std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>> > getInputStreams()
    {
        auto [seg_id, pool_ids] = getSegment();
        auto segment_pools = getPools(pool_ids);

        std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>> streams;
        streams.reserve(pool_ids.size());
        for (auto & pool : segment_pools)
        {
            streams.push_back({pool->getInputStream(seg_id), pool});
        }
        return std::pair{seg_id, streams};
    }

    std::pair<uint64_t, std::vector<uint64_t>> getSegment()
    {
        std::lock_guard lock(mtx);
        if (segments.empty())
        {
            return {};
        }
        auto target = segments.begin();
        for (auto itr = std::next(target); itr != segments.end(); ++itr)
        {
            if (itr->second.size() > target->second.size())
            {
                target = itr;
            }
        }
        auto result = std::pair<uint64_t, std::vector<uint64_t>>{target->first, target->second};
        segments.erase(target);
        LOG_FMT_DEBUG(log, "seg_id {} pool_id {}", result.first, result.second);
        return result;
    }

    SegmentReadTaskPools getPools(const std::vector<uint64_t> & pool_ids)
    {
        SegmentReadTaskPools result;
        result.reserve(pool_ids.size());
        std::lock_guard lock(mtx);
        for (uint64_t id : pool_ids)
        {
            auto itr = pools.find(id);
            if (itr != pools.end())
            {
                result.push_back(itr->second);
            }
            else
            {
                throw Exception(fmt::format("pool_id {} not found", id));
            }
        }
        return result;
    }

private:
    std::mutex mtx;
    // pool_id -> pool
    std::unordered_map<uint64_t, SegmentReadTaskPoolPtr> pools; // TODO: maybe weak_ptr is better.
    // seg_id -> pool_ids
    std::unordered_map<uint64_t, std::vector<uint64_t>> segments;

    Poco::Logger * log;

    SegmentReadTaskScheduler() : log(&Poco::Logger::get("SegmentReadTaskScheduler")) {}
};

class SegmentReader
{
public:
    SegmentReader()
        : stop(false)
        , log(&Poco::Logger::get("SegmentReader"))
    {
        t = std::thread(&SegmentReader::run, this);
    }

    ~SegmentReader()
    {
        t.join();
    }

    void setStop()
    {
        stop.store(true, std::memory_order_relaxed);
    }

private:
    std::atomic<bool> stop;
    Poco::Logger * log;
    std::thread t;

    void readSegments()
    {
        std::pair<UInt64, std::vector<std::pair<BlockInputStreamPtr, SegmentReadTaskPoolPtr>>> task;
        while (task.second.empty())
        {
            task = SegmentReadTaskScheduler::instance().getInputStreams();
            if (task.second.empty())
            {
                ::usleep(5000); // TODO: use notify
                if (stop.load(std::memory_order_relaxed))
                {
                    return;
                }
            }
        }
        UInt64 seg_id = task.first;
        auto & streams = task.second;
        std::vector<int> dones(streams.size(), 0);
        size_t done_count = 0;

        while (done_count < streams.size())
        {
            for (size_t i = 0; i < streams.size(); i++)
            {
                if (dones[i])
                {
                    continue;
                }
                auto & stream = streams[i];

                // TODO: Check next pack id?

                auto block = stream.first->read();
                if (!block)
                {
                    stream.second->finishSegment(seg_id);
                    dones[i] = 1;
                    done_count++;
                }
                else
                {
                    stream.second->pushBlock(std::move(block));
                }
            }
        }
    }

    void run()
    {
        setThreadName("SegmentReader");
        while (!stop.load(std::memory_order_relaxed))
        {
            try
            {
                readSegments();
            }
            catch (Exception & e)
            {
                LOG_FMT_ERROR(log, "ErrMsg: {}", e.message());
            }
            catch (...)
            {
                // TODO:
            }
        }
    }
};

class SegmentReadThreadPool
{
public:
    SegmentReadThreadPool(int thread_count)
        : log(&Poco::Logger::get("SegmentReadThreadPool"))
    {
        LOG_FMT_INFO(log, "thread_count {} start", thread_count);
        for (int i = 0; i < thread_count; i++)
        {
            readers.push_back(std::make_unique<SegmentReader>());
        }
        LOG_FMT_INFO(log, "thread_count {} end", thread_count);
    }

    ~SegmentReadThreadPool()
    {
        LOG_FMT_INFO(log, "Destroy thread pool start, thread_count {}", readers.size());
        for (auto & r : readers)
        {
            r->setStop();
        }
        LOG_FMT_INFO(log, "Destroy thread pool end, thread_count {}", readers.size());
    }

private:
    std::vector<std::unique_ptr<SegmentReader>> readers;
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

} // namespace DM
} // namespace DB
