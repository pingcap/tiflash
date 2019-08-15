#pragma once

#include <vector>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB
{
namespace DM
{

struct SegmentReadTask
{
    SegmentPtr      segment;
    SegmentSnapshot read_snapshot;
    HandleRanges    ranges;

    SegmentReadTask() = default;

    explicit SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshot & read_snapshot_)
        : segment(segment_), read_snapshot(read_snapshot_)
    {
    }

    SegmentReadTask(const SegmentPtr &      segment_, //
                    const SegmentSnapshot & read_snapshot_,
                    const HandleRanges &    ranges_)
        : segment(segment_), read_snapshot(read_snapshot_), ranges(ranges_)
    {
    }

    void addRange(const HandleRange & range) { ranges.push_back(range); }
};

class SegmentStreamCreator
{
public:
    virtual ~SegmentStreamCreator()                             = default;
    virtual BlockInputStreamPtr create(const SegmentReadTask &) = 0;
};

using SegmentStreamCreatorPtr = std::shared_ptr<SegmentStreamCreator>;
using SegmentReadTaskPtr      = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks        = std::vector<SegmentReadTaskPtr>;

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    SegmentReadTaskPool(SegmentReadTasks && tasks_) : tasks(std::move(tasks_)) {}

    SegmentReadTaskPtr nextTask()
    {
        std::lock_guard lock(mutex);
        return index == tasks.size() ? SegmentReadTaskPtr() : tasks[index++];
    }

private:
    SegmentReadTasks tasks;
    size_t           index = 0;

    std::mutex mutex;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DM
} // namespace DB