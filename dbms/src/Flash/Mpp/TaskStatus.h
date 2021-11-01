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
    switch (status)
    {
    case INITIALIZING:
        return "INITIALIZING";
    case RUNNING:
        return "RUNNING";
    case FINISHED:
        return "FINISHED";
    case CANCELLED:
        return "CANCELLED";
    default:
        throw Exception("Unknown TaskStatus");
    }
}
} // namespace DB
