// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
