#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <unistd.h>
#include <limits>
#include "Storages/DeltaMerge/ReadThread/WorkQueue.h"
#include "common/logger_useful.h"


namespace DB::DM
{
struct MergedTask;
using MergedTaskPtr = std::shared_ptr<MergedTask>;

class SegmentReader;
using SegmentReaderUPtr = std::unique_ptr<SegmentReader>;

class SegmentReadThreadPool
{
public:
    static SegmentReadThreadPool & instance()
    {
        static SegmentReadThreadPool thread_pool(40);
        return thread_pool;
    }

    bool addTask(const MergedTaskPtr & task);

private:
    
    SegmentReadThreadPool(int thread_count);
    ~SegmentReadThreadPool();
    SegmentReadThreadPool(const SegmentReadThreadPool&) = delete;
    SegmentReadThreadPool & operator=(const SegmentReadThreadPool&) = delete;
    SegmentReadThreadPool(SegmentReadThreadPool&&) = delete;
    SegmentReadThreadPool & operator=(SegmentReadThreadPool&&) = delete;

    WorkQueue<MergedTaskPtr> task_queue;
    std::vector<SegmentReaderUPtr> readers;
    Poco::Logger * log;
};

}