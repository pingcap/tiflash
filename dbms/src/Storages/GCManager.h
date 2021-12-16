#pragma once

#include <Common/Stopwatch.h>
#include <Storages/Transaction/Types.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class Context;

class GCManager
{
public:
    explicit GCManager(Context & context);

    ~GCManager() = default;

    bool work();

private:
    Context & global_context;

    TableID next_table_id = InvalidTableID;

    Poco::Logger * log;
};
} // namespace DB
