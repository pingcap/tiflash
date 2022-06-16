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

class SegmentReaderPoolManager
{
public:
    static SegmentReaderPoolManager & instance()
    {
        static SegmentReaderPoolManager pool_manager;
        return pool_manager;
    }

    static void init(Poco::Logger * log);

    void init(Poco::Logger * log, int threads_per_node, const std::vector<std::vector<int>> & numa_nodes);

    SegmentReaderPoolManager();
    ~SegmentReaderPoolManager();
    SegmentReaderPoolManager(const SegmentReaderPoolManager &) = delete;
    SegmentReaderPoolManager & operator=(const SegmentReaderPoolManager &) = delete;
    SegmentReaderPoolManager(SegmentReaderPoolManager &&) = delete;
    SegmentReaderPoolManager & operator=(SegmentReaderPoolManager &&) = delete;

    void addTask(MergedTaskPtr && task);

private:
    std::vector<std::unique_ptr<SegmentReaderPool>> reader_pools;
};

} // namespace DB::DM