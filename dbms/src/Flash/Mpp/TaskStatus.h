#pragma once

#include <common/StringRef.h>

namespace DB
{
enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    FINISHED,
    CANCELLED,
};

inline StringRef taskStatusToString(const TaskStatus & status);
} // namespace DB
