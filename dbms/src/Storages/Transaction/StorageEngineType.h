#pragma once

namespace TiDB
{

// Indicate that use 'TMT' or 'DM' as storage engine in AP. (TMT by default now)
enum class StorageEngine
{
    TMT = 0,
    DM,
};

} // namespace TiDB
