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

    bool isUnknown() const { return task_id == unknown_task_id; }

    String toString() const;

    static const MPPTaskId unknown_mpp_task_id;

private:
    static constexpr Int64 unknown_task_id = -1;
};

bool operator==(const MPPTaskId& lid, const MPPTaskId& rid)
{
    return lid.start_ts == rid.start_ts && lid.task_id == rid.task_id;
}
} // namespace DB

namespace std
{
template <>
class hash<DB::MPPTaskId>
{
public:
    size_t operator()(const DB::MPPTaskId & id) const
    {
        return hash<UInt64>()(id.start_ts) ^ hash<Int64>()(id.task_id);
    }
};
} // namespace std