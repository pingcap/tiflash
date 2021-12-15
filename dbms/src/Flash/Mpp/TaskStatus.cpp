#include <Common/Exception.h>
#include <Flash/Mpp/TaskStatus.h>

namespace DB
{
StringRef taskStatusToString(const TaskStatus & status)
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
