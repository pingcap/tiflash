#pragma once

#include <Storages/DeltaMerge/RowKeyRangeUtils.h>

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
    RowKeyRanges       ranges;

    SegmentReadTask(const SegmentPtr &         segment_, //
                    const SegmentSnapshotPtr & read_snapshot_,
                    const RowKeyRanges &       ranges_);

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
