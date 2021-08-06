#pragma once

namespace DB
{
enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    FINISHED,
    CANCELLED,
};
} // namespace DB

