// Copyright 2023 PingCAP, Inc.
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

#include <common/defines.h>
#include <fmt/format.h>

#include <string>

namespace DB
{
enum MppVersion : int64_t
{
    MppVersionV0 = 0,
    MppVersionV1,
    MppVersionV2,
    MppVersionV3,
    //
    MppVersionMAX,
};
// Make MppVersion formatable by fmtlib
ALWAYS_INLINE inline auto format_as(MppVersion v)
{
    return fmt::underlying(v);
}

enum MPPDataPacketVersion : int64_t
{
    MPPDataPacketV0 = 0,
    MPPDataPacketV1,
    //
    MPPDataPacketMAX,
};
// Make MPPDataPacketVersion formatable by fmtlib
ALWAYS_INLINE inline auto format_as(MPPDataPacketVersion v)
{
    return fmt::underlying(v);
}

bool ReportStatusToCoordinator(int64_t mpp_version, const std::string & coordinator_address);
bool ReportExecutionSummaryToCoordinator(int64_t mpp_version, bool report_execution_summary);
bool CheckMppVersion(int64_t mpp_version);
std::string GenMppVersionErrorMessage(int64_t mpp_version);
int64_t GetMppVersion();
MppVersion getMaxMppVersionFromTiDB();

} // namespace DB
