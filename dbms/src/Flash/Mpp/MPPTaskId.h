#pragma once

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    uint64_t start_ts;
    int64_t task_id;

    bool operator<(const MPPTaskId & rhs) const { return start_ts < rhs.start_ts || (start_ts == rhs.start_ts && task_id < rhs.task_id); }

    String toString() const
    {
        return fmt::format("[{},{}]", start_ts, task_id);
    }
};
}