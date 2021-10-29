#pragma once

#include <Common/Exception.h>
#include <common/StringRef.h>

#include <unordered_map>

namespace DB
{
enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    FINISHED,
    CANCELLED,
};

inline StringRef taskStatusToString(const TaskStatus & status)
{
    static std::unordered_map<TaskStatus, String> task_status_map{
        {INITIALIZING, "INITIALIZING"},
        {RUNNING, "RUNNING"},
        {FINISHED, "FINISHED"},
        {CANCELLED, "CANCELLED"}};

    auto it = task_status_map.find(status);
    if (it == task_status_map.end())
    {
        throw Exception("Unknown TaskStatus");
    }

    return it->second;
}
} // namespace DB
