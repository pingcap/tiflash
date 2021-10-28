#pragma once

#include <Common/Exception.h>

namespace DB
{
enum TaskStatus
{
    INITIALIZING,
    RUNNING,
    FINISHED,
    CANCELLED,
};

void stateToRunning(TaskStatus & status)
{
    if (status != INITIALIZING)
    {
        throw new TiFlashException();
    }
    status = RUNNING;
}

void stateToFinished(TaskStatus & status)
{
    status = FINISHED;
}

void stateToCancelled(TaskStatus & status)
{
    status = CANCELLED;
}
} // namespace DB

