#pragma once

<<<<<<< HEAD
#include <Storages/DeltaMerge/RangeUtils.h>
#include <Storages/DeltaMerge/Segment.h>
=======
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
>>>>>>> 8f8b729e5... Add time and thread_id for snapshot to check stale snapshots (#2229)

#include <queue>

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

using DMContextPtr       = std::shared_ptr<DMContext>;
using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks   = std::list<SegmentReadTaskPtr>;
using AfterSegmentRead   = std::function<void(const DMContextPtr &, const SegmentPtr &)>;

struct SegmentReadTask
{
    SegmentPtr         segment;
    SegmentSnapshotPtr read_snapshot;
    HandleRanges       ranges;

    SegmentReadTask(const SegmentPtr &         segment_, //
                    const SegmentSnapshotPtr & read_snapshot_,
<<<<<<< HEAD
                    const HandleRanges &       ranges_)
        : segment(segment_), read_snapshot(read_snapshot_), ranges(ranges_)
    {
    }

    void addRange(const HandleRange & range) { ranges.push_back(range); }

    void mergeRanges() { ranges = DM::tryMergeRanges(std::move(ranges), 1); }

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size)
    {
        if (tasks.empty() || tasks.size() >= expected_size)
            return tasks;

        // Note that expected_size is normally small(less than 100), so the algorithm complexity here does not matter.

        // Construct a max heap, determined by ranges' count.
        auto cmp = [](const SegmentReadTaskPtr & a, const SegmentReadTaskPtr & b) { return a->ranges.size() < b->ranges.size(); };
        std::priority_queue<SegmentReadTaskPtr, std::vector<SegmentReadTaskPtr>, decltype(cmp)> largest_ranges_first(cmp);
        for (auto & task : tasks)
            largest_ranges_first.push(task);

        // Split the top task.
        while (largest_ranges_first.size() < expected_size && largest_ranges_first.top()->ranges.size() > 1)
        {
            auto top = largest_ranges_first.top();
            largest_ranges_first.pop();

            size_t split_count = top->ranges.size() / 2;

            auto left = std::make_shared<SegmentReadTask>(
                top->segment, top->read_snapshot->clone(), HandleRanges(top->ranges.begin(), top->ranges.begin() + split_count));
            auto right = std::make_shared<SegmentReadTask>(
                top->segment, top->read_snapshot->clone(), HandleRanges(top->ranges.begin() + split_count, top->ranges.end()));

            largest_ranges_first.push(left);
            largest_ranges_first.push(right);
        }

        SegmentReadTasks result_tasks;
        while (!largest_ranges_first.empty())
        {
            result_tasks.push_back(largest_ranges_first.top());
            largest_ranges_first.pop();
        }

        return result_tasks;
    }
=======
                    const RowKeyRanges &       ranges_);

    explicit SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshotPtr & read_snapshot_);

    ~SegmentReadTask();

    std::pair<size_t, size_t> getRowsAndBytes() const;

    void addRange(const RowKeyRange & range) { ranges.push_back(range); }

    void mergeRanges() { ranges = DM::tryMergeRanges(std::move(ranges), 1); }

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);
>>>>>>> 8f8b729e5... Add time and thread_id for snapshot to check stale snapshots (#2229)
};


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
        tasks.pop_front();
        return task;
    }

private:
    SegmentReadTasks tasks;

    std::mutex mutex;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DM
} // namespace DB
