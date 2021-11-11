#pragma once

#include <common/types.h>

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId()
        : start_ts(0)
        , task_id(unknown_task_id){};

    MPPTaskId(UInt64 start_ts_, Int64 task_id_)
        : start_ts(start_ts_)
        , task_id(task_id_){};

    const UInt64 start_ts;
    const Int64 task_id;

    bool operator<(const MPPTaskId & rhs) const;

    bool isUnknown() const { return task_id == unknown_task_id; }

    String toString() const;

    static const MPPTaskId unknown_mpp_task_id;

private:
    static constexpr Int64 unknown_task_id = -1;
};

} // namespace DB