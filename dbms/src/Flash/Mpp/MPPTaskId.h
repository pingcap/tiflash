#pragma once

#include <common/types.h>

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId(uint64_t start_ts_, int64_t task_id_): start_ts(start_ts_), task_id(task_id_) {};

    uint64_t start_ts;
    int64_t task_id;

    bool operator<(const MPPTaskId & rhs) const;

    String toString() const;
};
} // namespace DB