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

using SegmentReadTasks = std::vector<SegmentReadTask>;

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    using StreamCreator = std::function<BlockInputStreamPtr(const SegmentReadTask & task)>;
    SegmentReadTaskPool(SegmentReadTasks && tasks_, StreamCreator creator_) : tasks(std::move(tasks_)), creator(creator_) {}

    std::pair<UInt64, BlockInputStreamPtr> nextTask()
    {
        SegmentReadTask * task;
        {
            std::lock_guard<std::mutex> lock(mutex);

            if (index == tasks.size())
                return {0, {}};
            task = &(tasks[index++]);
        }
        return {task->segment->segmentId(), creator(*task)};
    }

private:
    SegmentReadTasks tasks;
    size_t           index = 0;
    StreamCreator    creator;

    std::mutex mutex;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DM
} // namespace DB