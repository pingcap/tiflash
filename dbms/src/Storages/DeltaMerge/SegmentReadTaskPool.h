#pragma once

#include <Storages/DeltaMerge/Segment.h>

#include <queue>

namespace DB
{
namespace DM
{

struct SegmentReadTask
{
    SegmentPtr         segment;
    SegmentSnapshotPtr read_snapshot;
    PKRanges           ranges;

    explicit SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshotPtr & read_snapshot_)
        : segment(segment_), read_snapshot(read_snapshot_)
    {
    }

    SegmentReadTask(const SegmentPtr &         segment_, //
                    const SegmentSnapshotPtr & read_snapshot_,
                    const PKRanges &           ranges_)
        : segment(segment_), read_snapshot(read_snapshot_), ranges(ranges_)
    {
    }

    void addRange(const PKRange & range) { ranges.push_back(range); }
};

using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks   = std::queue<SegmentReadTaskPtr>;
using AfterSegmentRead   = std::function<void(const DMContextPtr &, const SegmentPtr &)>;

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    SegmentReadTaskPool(SegmentReadTasks && tasks_) : tasks(std::move(tasks_)) {}

    SegmentReadTaskPtr nextTask()
    {
        std::lock_guard lock(mutex);
        if (tasks.empty())
            return {};
        auto task = tasks.front();
        tasks.pop();
        return task;
    }

private:
    SegmentReadTasks tasks;

    std::mutex mutex;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DM
} // namespace DB
