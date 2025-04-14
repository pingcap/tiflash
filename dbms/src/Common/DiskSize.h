// Copyright 2025 PingCAP, Inc.
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

#include <fmt/format.h>
#include <sys/statvfs.h>

#include <string>

namespace DB
{
inline std::string getFsStatsOfPath(std::string_view file_path, struct statvfs & vfs)
{
    /// Get capacity, used, available size for one path.
    /// Similar to `handle_store_heartbeat` in TiKV release-4.0 branch
    /// https://github.com/tikv/tikv/blob/f14e8288f3/components/raftstore/src/store/worker/pd.rs#L593
    if (int code = statvfs(file_path.data(), &vfs); code != 0)
        return fmt::format("statvfs failed, path: {}, errno: {}", file_path, errno);
    return "";
}
} // namespace DB
