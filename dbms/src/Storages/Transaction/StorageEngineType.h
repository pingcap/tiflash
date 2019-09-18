#pragma once

namespace TiDB
{

// Indicate that use 'TMT' or 'DM' as storage engine in AP. (TMT by default now)
enum class StorageEngine
{
    UNSPECIFIED = 0,
    TMT,
    DM,

    // indicate other engine type in ClickHouse
    UNSUPPORTED_ENGINES = 128,
    // Just for test, Buggy StorageMemory
    DEBUGGING_MEMORY = 129,
};

} // namespace TiDB
