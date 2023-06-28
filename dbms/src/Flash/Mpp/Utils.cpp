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

#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/Utils.h>
#include <Poco/String.h>
#include <common/defines.h>
#include <fiu.h>
#include <fmt/format.h>

#include <array>
#include <memory>

namespace DB
{
namespace FailPoints
{
extern const char invalid_mpp_version[];
} // namespace FailPoints

mpp::MPPDataPacket getPacketWithError(String reason)
{
    mpp::MPPDataPacket data;
    auto err = std::make_unique<mpp::Error>();
    err->set_mpp_version(DB::GetMppVersion());
    err->set_msg(std::move(reason));
    data.set_allocated_error(err.release());
    return data;
}

void trimStackTrace(String & message)
{
    auto stack_trace_pos = message.find("Stack trace");
    if (stack_trace_pos != String::npos)
    {
        message.resize(stack_trace_pos);
        Poco::trimRightInPlace(message);
    }
}

// Latest mpp-version supported by TiFlash
static MppVersion NewestMppVersion = MppVersion(MppVersion::MppVersionMAX - 1);
static MppVersion MinMppVersion = MppVersion::MppVersionV0;

// Use ReportStatus interface to report status
bool ReportStatusToCoordinator(int64_t mpp_version, const std::string & coordinator_address)
{
    return mpp_version >= MppVersion::MppVersionV2 && !coordinator_address.empty();
}

// Use ReportStatus interface to report execution summaries, instead of passing them within mpp data packet
bool ReportExecutionSummaryToCoordinator(int64_t mpp_version, bool report_execution_summary)
{
    return mpp_version >= MppVersion::MppVersionV2 && report_execution_summary;
}

// Check mpp-version is illegal
bool CheckMppVersion(int64_t mpp_version)
{
    fiu_do_on(FailPoints::invalid_mpp_version, {
        mpp_version = -1;
    });
    return mpp_version >= MinMppVersion && mpp_version <= NewestMppVersion;
}

std::string GenMppVersionErrorMessage(int64_t mpp_version)
{
    fiu_do_on(FailPoints::invalid_mpp_version, {
        mpp_version = -1;
    });
    auto err_msg = fmt::format("Invalid mpp version {}, TiFlash expects version: min {}, max {}, should upgrade {}",
                               mpp_version,
                               MinMppVersion,
                               NewestMppVersion,
                               (mpp_version < MinMppVersion) ? "TiDB/planner" : "TiFlash");
    return err_msg;
}

// Get latest mpp-version supported by TiFlash
int64_t GetMppVersion()
{
    return (NewestMppVersion);
}

} // namespace DB