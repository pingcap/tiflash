#pragma once

#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <common/logger_useful.h>

namespace DB::DM
{
class MergedTask;
using MergedTaskPtr = std::shared_ptr<MergedTask>;

class SegmentReader;
using SegmentReaderUPtr = std::unique_ptr<SegmentReader>;

class SegmentReaderPool
{
public:
    SegmentReaderPool(int thread_count, const std::vector<int> & cpus);
    ~SegmentReaderPool();
    SegmentReaderPool(const SegmentReaderPool &) = delete;
    SegmentReaderPool & operator=(const SegmentReaderPool &) = delete;
    SegmentReaderPool(SegmentReaderPool &&) = delete;
    SegmentReaderPool & operator=(SegmentReaderPool &&) = delete;

    void addTask(MergedTaskPtr && task);

private:
    void init(int thread_count, const std::vector<int> & cpus);

    WorkQueue<MergedTaskPtr> task_queue;
    std::vector<SegmentReaderUPtr> readers;
    Poco::Logger * log;
};

// SegmentReaderPoolManager is a NUMA-aware singleton that manages several SegmentReaderPool objects.
// The number of SegmentReadPool object is the same as the number of CPU NUMA node.
// Thread number of a SegmentReadPool object is the same as the number of CPU logical core of a CPU NUMA node.
// Function `addTask` dispatches MergedTask to SegmentReadPool by their segment id, so a segment read task
// wouldn't be processed across NUMA nodes.
class SegmentReaderPoolManager
{
public:
    static SegmentReaderPoolManager & instance()
    {
        static SegmentReaderPoolManager pool_manager;
        return pool_manager;
    }

    ~SegmentReaderPoolManager();
    SegmentReaderPoolManager(const SegmentReaderPoolManager &) = delete;
    SegmentReaderPoolManager & operator=(const SegmentReaderPoolManager &) = delete;
    SegmentReaderPoolManager(SegmentReaderPoolManager &&) = delete;
    SegmentReaderPoolManager & operator=(SegmentReaderPoolManager &&) = delete;

    void addTask(MergedTaskPtr && task);

private:
    SegmentReaderPoolManager();
    void init();

    std::vector<std::unique_ptr<SegmentReaderPool>> reader_pools;
    Poco::Logger * log;
};

} // namespace DB::DM