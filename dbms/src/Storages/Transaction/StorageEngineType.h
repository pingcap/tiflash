#pragma once

#include <cstdint>
#include <string>

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

enum class SnapshotApplyMethod : std::int32_t
{
    Block = 1,
    // Invalid if the storage engine is not DeltaTree
    DTFile_Directory,
    DTFile_Single,
};

inline const std::string applyMethodToString(SnapshotApplyMethod method)
{
    switch (method)
    {
        case SnapshotApplyMethod::Block:
            return "block";
        case SnapshotApplyMethod::DTFile_Directory:
            return "file1";
        case SnapshotApplyMethod::DTFile_Single:
            return "file2";
        default:
            return "unknown(" + std::to_string(static_cast<std::int32_t>(method)) + ")";
    }
    return "unknown";
}

} // namespace TiDB
