#pragma once

namespace TiDB
{

// Indicate that use 'TMT' or 'DM' as storage engine in AP. (TMT by default now)
enum class StorageEngine
{
    UNSPECIFIED = 0,
    TMT,
    DT,

    // indicate other engine type in ClickHouse
    UNSUPPORTED_ENGINES = 128,
};

} // namespace TiDB
