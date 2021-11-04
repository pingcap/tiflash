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

    UInt64 start_ts;
    Int64 task_id;

    bool operator<(const MPPTaskId & rhs) const;

    String toString() const;

    static const MPPTaskId empty_mpp_task_id;

private:
    static constexpr Int64 unknown_task_id = -1;
};

} // namespace DB