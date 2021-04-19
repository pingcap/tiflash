#pragma once

#include <Common/Stopwatch.h>
#include <Storages/IManageableStorage.h>

namespace DB
{
class GCManager
{
public:
    GCManager(Context & context) : global_context{context.getGlobalContext()}, log(&Logger::get("GCManager")) {};

    ~GCManager() = default;

    bool work();

private:
    Context & global_context;

    TableID next_table_id = InvalidTableID;

    AtomicStopwatch gc_check_stop_watch;

    Logger * log;
};
} // namespace DB
