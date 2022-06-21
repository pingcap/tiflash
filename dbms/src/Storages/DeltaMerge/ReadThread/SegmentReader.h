#pragma once

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include "Storages/DeltaMerge/ReadThread/WorkQueue.h"
#include <common/logger_useful.h>

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
        static SegmentReadThreadPool thread_pool(40);  // TODO(jinhelin)
        return thread_pool;
    }

    ~SegmentReadThreadPool();
    SegmentReadThreadPool(const SegmentReadThreadPool&) = delete;
    SegmentReadThreadPool & operator=(const SegmentReadThreadPool&) = delete;
    SegmentReadThreadPool(SegmentReadThreadPool&&) = delete;
    SegmentReadThreadPool & operator=(SegmentReadThreadPool&&) = delete;

    bool addTask(MergedTaskPtr && task);

private:
    SegmentReadThreadPool(int thread_count);

    WorkQueue<MergedTaskPtr> task_queue;
    std::vector<SegmentReaderUPtr> readers;
    Poco::Logger * log;
};

}